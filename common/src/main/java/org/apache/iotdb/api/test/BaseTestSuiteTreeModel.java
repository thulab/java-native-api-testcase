package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.GenerateValues;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

import static org.apache.tsfile.encrypt.EncryptUtils.hexStringToByteArray;

// TODO：里面的 SQL 语句很多用的是老版本的，之后若数据库不支持了需要全面更新
public class BaseTestSuiteTreeModel {
    public Logger logger = Logger.getLogger(BaseTestSuiteTreeModel.class);
    public Session session = null;
    public SessionPool sessionPool = null;
    // 是对齐/非对齐序列。dynamic module. 动态模版相关
    protected boolean isAligned;
    // 是否打印查询结果
    protected boolean verbose;
    // 自动创建元数据开关。 dynamic module. 动态模版相关
    protected boolean auto_create_schema;
    protected long baseTime;
    /** PLM 就绪/清理等待上限（毫秒），可用 -Dplm.db-ready-timeout-ms 覆盖。 */
    private static final long PLM_DB_READY_TIMEOUT_MS = Long.getLong("plm.db-ready-timeout-ms", 120_000L);
    /** PLM 真实 run 标识；手工运行时为空，保持旧的“每个类全库清理”行为。 */
    private static final String PLM_RUN_ID = trimToNull(System.getenv("PLM_RUN_ID"));
    /** run 级清理标记目录；放在 ignores 的 reports/ 下，跨测试类进程共享。 */
    private static final Path RUN_STATE_ROOT =
            Paths.get(System.getProperty("user.dir"), "reports", "plm-run-state");

    @BeforeClass
    public void beforeSuite() throws IoTDBConnectionException, ParseException, IOException {
        if (ReadConfig.getInstance().getValue("is_sessionPool").equals("false")) {
            session = PrepareConnection.getSessionTreeModel();
        } else {
            session = PrepareConnection.getSessionTreeModel();
            sessionPool = PrepareConnection.getSessionPoolTreeModel();
        }
        verbose = Boolean.parseBoolean(ReadConfig.getInstance().getValue("verbose"));
        isAligned = Boolean.parseBoolean(ReadConfig.getInstance().getValue("isAligned"));
        auto_create_schema = Boolean.parseBoolean(ReadConfig.getInstance().getValue("auto_create_schema"));
        baseTime = parseDate();
        // PLM 2026-11-05：生产 run 只起一个 K8s 实例，且实例由 PLM 在 run 结束后销毁。
        // 首个类做一次全库/全模板清理并落 run 级标记；后续类只清自己的库/模板，
        // 不再每个类都 drop 全库。手工运行（无 PLM_RUN_ID）保持旧行为。
        if (isPlmRun()) {
            if (requiresGlobalClean()) {
                String message = "[plm] requiresGlobalClean 类触发全量清理: " + getClass().getSimpleName();
                logger.info(message);
                System.err.println(message);
                if (resetClusterState()) {
                    markRunClusterCleaned();
                }
            } else if (runAlreadyCleaned()) {
                cleanManagedResources();
            } else if (resetClusterState()) {
                markRunClusterCleaned();
            }
        } else {
            resetClusterState();
        }
    }

    /** 是否 PLM 真实 run（手工运行返回 false，保持旧的每类全量清理）。 */
    protected boolean isPlmRun() {
        return PLM_RUN_ID != null;
    }

    /** 需要“干净实例”的类（如 TestDataBase 会按 CSV 删任意库）覆盖为 true。 */
    protected boolean requiresGlobalClean() {
        return false;
    }

    /** 本类拥有的数据库（精确名称，不含通配符）；默认空，子类按需覆盖。 */
    protected Set<String> managedDatabases() {
        return Collections.emptySet();
    }

    /** 本类拥有的模板（精确名称，不含通配符）；默认空，子类按需覆盖。 */
    protected Set<String> managedTemplates() {
        return Collections.emptySet();
    }

    /** 本类拥有的数据库前缀（只匹配 prefix 本身、prefix_ / prefix. / prefix+数字）；默认空。 */
    protected Set<String> managedDatabasePrefixes() {
        return Collections.emptySet();
    }

    /** 本类拥有的模板前缀（匹配规则同上）；默认空。 */
    protected Set<String> managedTemplatePrefixes() {
        return Collections.emptySet();
    }

    /** 清理并探测集群；返回清理和探测是否都成功，只有成功才写 run 级标记。 */
    protected boolean resetClusterState() {
        boolean cleaned = false;
        try {
            // 两个清理都要尝试：用 &（非短路），单个失败不阻断另一个。
            cleaned = cleanDatabases(verbose) & cleanTemplates(verbose);
        } catch (Exception e) {
            logger.warn("[plm] 启动清理失败（继续执行，后续用例可能命中残留状态）：" + e.getMessage());
        }
        boolean ready = awaitClusterReady();
        return cleaned && ready;
    }

    /** 集群就绪探测：建库 + 建一条探测序列，成功后删除；906 期间等待重试。 */
    protected boolean awaitClusterReady() {
        long deadline = System.currentTimeMillis() + PLM_DB_READY_TIMEOUT_MS;
        long retrySleep = 500L;
        String probeDb = "root.plm_probe_" + Long.toHexString(System.nanoTime());
        while (true) {
            boolean created = false;
            try {
                session.createDatabase(probeDb);
                created = true;
                session.createTimeseries(probeDb + ".d.s", TSDataType.BOOLEAN,
                        TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
                return true;
            } catch (StatementExecutionException e) {
                if (!isRegionUnavailable(e)) {
                    logger.warn("[plm] 集群就绪探测非 906 失败：" + e.getMessage());
                    return false;
                }
            } catch (IoTDBConnectionException e) {
                logger.warn("[plm] 集群就绪探测连接失败（继续重试）：" + e.getMessage());
            } finally {
                if (created) {
                    try {
                        session.deleteDatabase(probeDb);
                    } catch (Exception ignored) {
                        // 探测库删除失败不阻塞测试；后续 cleanDatabases 会再次尝试。
                    }
                }
            }
            if (System.currentTimeMillis() >= deadline) {
                logger.warn("[plm] 集群就绪探测超时（" + PLM_DB_READY_TIMEOUT_MS
                        + "ms），让用例自身暴露失败");
                return false;
            }
            sleepQuietly(retrySleep);
            retrySleep = Math.min(retrySleep * 2, 5_000L);
        }
    }

    /**
     * 类级清理：只删本类声明的库/模板，默认空。首个类做过全局清理后，
     * 后续类靠它清掉自己的残留，不再 drop 整个实例。
     */
    protected void cleanManagedResources() {
        if (session == null) {
            return;
        }
        String scopeLog = "[plm] scoped cleanup: " + getClass().getSimpleName()
                + " db=" + managedDatabases() + " tpl=" + managedTemplates()
                + " dbPrefix=" + managedDatabasePrefixes() + " tplPrefix=" + managedTemplatePrefixes();
        logger.info(scopeLog);
        System.err.println(scopeLog);
        for (String database : managedDatabases()) {
            dropDatabaseIfExists(database);
        }
        for (String template : managedTemplates()) {
            dropTemplateIfExists(template);
        }
        for (String prefix : managedDatabasePrefixes()) {
            dropDatabasesMatching(prefix);
        }
        for (String prefix : managedTemplatePrefixes()) {
            dropTemplatesMatching(prefix);
        }
    }

    protected void dropDatabaseIfExists(String database) {
        if (database == null || database.trim().isEmpty() || database.contains("*")) {
            return;
        }
        try {
            if (!checkStroageGroupExists(database)) {
                return;
            }
        } catch (Exception e) {
            logger.warn("[plm] 检查库是否存在失败（跳过 scoped 清理）：" + database + " " + e.getMessage());
            return;
        }
        executeCleanupWithRetry("drop database " + database, "drop database " + database);
    }

    protected void dropTemplateIfExists(String template) {
        if (template == null || template.trim().isEmpty() || template.contains("*")) {
            return;
        }
        try {
            if (!checkTemplateExists(template)) {
                return;
            }
        } catch (Exception e) {
            logger.warn("[plm] 检查模板是否存在失败（跳过 scoped 清理）：" + template + " " + e.getMessage());
            return;
        }
        executeCleanupWithRetry("drop schema template `" + template + "`", "drop template " + template);
    }

    protected void dropDatabasesMatching(String prefix) {
        if (prefix == null || prefix.trim().isEmpty() || prefix.contains("*")) {
            return;
        }
        List<String> matches = new ArrayList<>();
        try (SessionDataSet dataSet = session.executeQueryStatement("show databases")) {
            while (dataSet.hasNext()) {
                String database = dataSet.next().getFields().get(0).toString();
                if (!database.equals("root.__audit") && matchesPrefix(database, prefix)) {
                    matches.add(database);
                }
            }
        } catch (Exception e) {
            logger.warn("[plm] 读取数据库列表失败（跳过前缀清理 " + prefix + "）：" + e.getMessage());
            return;
        }
        for (String database : matches) {
            executeCleanupWithRetry("drop database " + database, "drop database " + database);
        }
    }

    protected void dropTemplatesMatching(String prefix) {
        if (prefix == null || prefix.trim().isEmpty() || prefix.contains("*")) {
            return;
        }
        List<String> matches = new ArrayList<>();
        try (SessionDataSet dataSet = session.executeQueryStatement("show schema templates")) {
            while (dataSet.hasNext()) {
                String template = dataSet.next().getFields().get(0).toString();
                if (matchesPrefix(template, prefix)) {
                    matches.add(template);
                }
            }
        } catch (Exception e) {
            logger.warn("[plm] 读取模板列表失败（跳过前缀清理 " + prefix + "）：" + e.getMessage());
            return;
        }
        for (String template : matches) {
            executeCleanupWithRetry("drop schema template `" + template + "`", "drop template " + template);
        }
    }

    /** 只匹配 prefix 本身、prefix_ / prefix. / prefix+数字，避免误删同前缀的其它资源。 */
    private static boolean matchesPrefix(String name, String prefix) {
        if (name == null || prefix == null || !name.startsWith(prefix)) {
            return false;
        }
        if (name.length() == prefix.length()) {
            return true;
        }
        char next = name.charAt(prefix.length());
        return next == '_' || next == '.' || Character.isDigit(next);
    }

    private Path runCleanedMarker() {
        String safeRunId = PLM_RUN_ID.replaceAll("[^A-Za-z0-9._-]", "_");
        return RUN_STATE_ROOT.resolve(safeRunId).resolve("cluster-cleaned");
    }

    private boolean runAlreadyCleaned() {
        return isPlmRun() && Files.exists(runCleanedMarker());
    }

    private void markRunClusterCleaned() {
        if (!isPlmRun()) {
            return;
        }
        Path marker = runCleanedMarker();
        try {
            Files.createDirectories(marker.getParent());
            String content = "runId=" + PLM_RUN_ID
                    + "\nhost=" + ReadConfig.getInstance().getValue("host")
                    + "\nport=" + ReadConfig.getInstance().getValue("port")
                    + "\ncleanedAt=" + Instant.now() + "\n";
            Files.write(marker, content.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            String message = "[plm] run 级全局清理完成，后续测试类跳过全库 drop：" + marker;
            logger.info(message);
            System.err.println(message);
        } catch (FileAlreadyExistsException ignored) {
            // 另一个类已先完成清理（PLM 逐条派发时一般不会并发）。
        } catch (Exception e) {
            logger.warn("[plm] 写 run 级清理标记失败（后续类可能重复全局清理）：" + e.getMessage());
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /** 建库并等待该库的 schema region 可用（兼容 906/DataRegionGroups）。 */
    protected void createDatabaseAwaitReady(String database)
            throws IoTDBConnectionException, StatementExecutionException {
        try {
            session.createDatabase(database);
        } catch (StatementExecutionException e) {
            if (!isAlreadyCreated(e)) {
                throw e;
            }
        }
        awaitDatabaseReady(database);
    }

    /** 轮询探测序列，直到指定库可以正常写入（或超时后抛出最后一次 906）。 */
    protected void awaitDatabaseReady(String database)
            throws IoTDBConnectionException, StatementExecutionException {
        long deadline = System.currentTimeMillis() + PLM_DB_READY_TIMEOUT_MS;
        long retrySleep = 500L;
        StatementExecutionException last = null;
        while (true) {
            String probe = database + ".__plm_ready_" + Long.toHexString(System.nanoTime());
            try {
                session.createTimeseries(probe, TSDataType.BOOLEAN,
                        TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
                try {
                    session.deleteTimeseries(probe);
                } catch (Exception ignored) {
                    // 探测序列删除失败不影响就绪结论；@AfterClass 会清库。
                }
                return;
            } catch (StatementExecutionException e) {
                if (!isRegionUnavailable(e)) {
                    throw e;
                }
                last = e;
            }
            if (System.currentTimeMillis() >= deadline) {
                if (last != null) {
                    throw last;
                }
                return;
            }
            sleepQuietly(retrySleep);
            retrySleep = Math.min(retrySleep * 2, 5_000L);
        }
    }

    /** 906：schema/data region group 暂不可用（建库后 region 尚未就绪、集群抖动）。 */
    protected static boolean isRegionUnavailable(Throwable error) {
        String message = error == null ? null : error.getMessage();
        if (message == null) {
            return false;
        }
        return message.contains("906")
                || message.contains("SchemaRegionGroup")
                || message.contains("DataRegionGroup");
    }

    private static boolean isAlreadyCreated(Throwable error) {
        String message = error == null ? null : error.getMessage();
        return message != null && message.contains("already been created as database");
    }

    protected static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    protected interface SqlOperation {
        void run() throws IoTDBConnectionException, StatementExecutionException;
    }

    /**
     * 906 期间重试写入：自动建库（auto_create_schema）后 region 尚未就绪时，
     * 客户端可能自己重试到超时仍返回 906。这里按就绪上限做指数退避，
     * 避免单条用例被一次 region 排队直接判错。
     */
    protected void executeWithRegionRetry(String label, SqlOperation operation)
            throws IoTDBConnectionException, StatementExecutionException {
        long deadline = System.currentTimeMillis() + PLM_DB_READY_TIMEOUT_MS;
        long retrySleep = 500L;
        while (true) {
            try {
                operation.run();
                return;
            } catch (StatementExecutionException e) {
                if (!isRegionUnavailable(e) || System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                logger.warn("[plm] " + label + " 命中 906，等待 region 就绪后重试：" + e.getMessage());
                sleepQuietly(retrySleep);
                retrySleep = Math.min(retrySleep * 2, 5_000L);
            }
        }
    }

    @AfterClass
    public void afterSuie() throws IoTDBConnectionException, StatementExecutionException, IOException {
        try {
            if (isPlmRun()) {
                // run 实例由 PLM 在批次结束后销毁；这里只清本类自己的库/模板，
                // 不再每个类都 drop 全库。
                cleanManagedResources();
            } else {
                cleanDatabases(verbose);
                cleanTemplates(verbose);
            }
        } catch (Exception e) {
            logger.warn("[plm] @AfterClass 清理失败（继续关闭连接）：" + e.getMessage());
        }
        if (ReadConfig.getInstance().getValue("is_sessionPool").equals("false")) {
            if (session != null) {
                session.close();
            }
        } else {
            if (session != null) {
                session.close();
            }
            if (sessionPool != null) {
                sessionPool.close();
            }
        }
    }

    private long parseDate() throws IOException, ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.parse(ReadConfig.getInstance().getValue("time_base")).getTime();
    }

    public boolean checkStroageGroupExists(String storageGroupId) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet records = session.executeQueryStatement("show storage group " + storageGroupId)) {
            return records.hasNext();
        }
    }

    // 返回sql语句查询到的结果行数
    public int countLines(String sql, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet records = session.executeQueryStatement(sql)) {
            if (verbose) {
                logger.info(sql);
                logger.info("******** start ********");
            }
            int count = 0;
            while (records.hasNext()) {
                count++;
                if (verbose) {
                    logger.info(records.next());
                } else {
                    records.next();
                }
            }
            if (verbose) {
                logger.info("******** end ********" + count);
            }
            return count;
        }
    }

    public int getCount(String sql, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet records = session.executeQueryStatement(sql)) {
            SessionDataSet.DataIterator recordsIter = records.iterator();
            int count = 0;
            if (verbose) {
                logger.info(sql);
                logger.info("******** start ********");
            }
            while (recordsIter.next()) {
                count = recordsIter.getInt(1);
                if (verbose) {
                    logger.info(count);
                }
            }
            if (verbose) {
                logger.info("******** end ********");
            }
            return count;
        }
    }

    public int getStorageGroupCount(String storageGroupId) throws IoTDBConnectionException, StatementExecutionException {
        return getStorageGroupCount(storageGroupId, false);
    }

    public int getStorageGroupCount(String storageGroupId, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return getCount("count databases " + storageGroupId, verbose);
    }

    public void assertTSExists(String path, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet dataSet = session.executeQueryStatement("show timeseries " + path)) {
            SessionDataSet.DataIterator recordsIter = dataSet.iterator();
            boolean found = false;
            while (recordsIter.next()) {
                if (verbose) {
                    logger.info(recordsIter.getString(1));
                    logger.info(recordsIter.getString(4));
                    logger.info(recordsIter.getString(5));
                    logger.info(recordsIter.getString(6));
                }
                assert path.equals(recordsIter.getString(1)) : "Timeseries exists: " + path;
                found = true;
            }
            // 序列不存在时 show timeseries 返回空，原先 while 不执行、断言被跳过，方法名声称"断言存在"却什么都没校验。
            assert found : "Timeseries not exists: " + path;
        }
    }

    public int getTimeSeriesCount(String timeSeries, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return getCount("count timeseries " + timeSeries, verbose);
    }

    // 获取设备数量
    public int getDeviceCount(String device, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return getCount("count devices " + device, verbose);
    }

    public int getRecordCount(String device, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return getCount("select count(*) from " + device, verbose);
    }

    public void checkQueryResult(String sql, TSDataType tsDataType, Object expectValue) throws IoTDBConnectionException, StatementExecutionException {
        logger.debug("sql=" + sql);
        // 统计实际校验的行数：调用方都是刚写入数据后立即查询，预期至少返回一行。
        // 若查询返回 0 行，原先的 while 循环直接跳过、断言不执行，会造成"数据没写进去却测试通过"的静默漏判。
        int checked = 0;
        try (SessionDataSet dataSet = session.executeQueryStatement(sql)) {
            switch (tsDataType) {
                case INT32:
                case INT64:
                case FLOAT:
                case DOUBLE:
                case TIMESTAMP:
                    Double expect = Double.valueOf(expectValue.toString());
                    while (dataSet.hasNext()) {
                        RowRecord records = dataSet.next();
                        Double actualValue = Double.valueOf(records.getFields().get(0).toString());
                        assert actualValue.equals(expect) : "确认结果" + dataSet.getColumnTypes().get(1) + "值: expect " + expectValue + ", actual " + records.getFields().get(0).toString();
                        checked++;
                    }
                    break;
                case BOOLEAN:
                case TEXT:
                case STRING:
                    while (dataSet.hasNext()) {
                        RowRecord records = dataSet.next();
                        String actualValue = records.getFields().get(0).toString();
                        assert actualValue.equals(expectValue.toString()) : "确认结果" + dataSet.getColumnTypes().get(1) + "值: expect " + expectValue + ", actual " + actualValue;
                        checked++;
                    }
                    break;
                case BLOB:
                    while (dataSet.hasNext()) {
                        RowRecord records = dataSet.next();
                        String actualValue = records.getFields().get(0).toString();
                        actualValue = actualValue.substring(2);
                        byte[] bytes = hexStringToByteArray(actualValue);
                        actualValue = new String(bytes);
                        assert actualValue.equals(expectValue.toString()) : "确认结果" + dataSet.getColumnTypes().get(1) + "值: expect " + expectValue + ", actual " + actualValue;
                        checked++;
                    }
                    break;
                case DATE:
                    while (dataSet.hasNext()) {
                        RowRecord records = dataSet.next();
                        String actualValue = records.getFields().get(0).toString();
                        assert actualValue.equals(expectValue.toString().replace("-", "")) : "确认结果" + dataSet.getColumnTypes().get(1) + "值: expect " + expectValue + ", actual " + actualValue;
                        checked++;
                    }
                    break;
            }
        }
        assert checked > 0 : "查询无结果，期望值未被校验（疑似数据未写入）: sql=" + sql + ", expect=" + expectValue;
    }

    // 用于判断模板是否存在
    public boolean checkTemplateExists(String templateName) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet dataSet = session.executeQueryStatement("show schema templates ")) {
            SessionDataSet.DataIterator records = dataSet.iterator();
            while (records.next()) {
                if (templateName.equals(records.getString(1))) {
                    return true;
                }
            }
            return false;
        }
    }

    // 用于查看挂载了某个设备模板的路径
    public boolean checkTemplateContainPath(String templateName, String path) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet dataSet = session.executeQueryStatement("show paths set schema template " + templateName)) {
            while (dataSet.hasNext()) {
                String result = dataSet.next().getFields().get(0).toString();
                if (result.equals(path)) {
                    return true;
                }
            }
            return false;
        }
    }

    // 查看某个设备模板下的物理量
    public boolean checkUsingTemplate(String device, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        try (SessionDataSet dataSet = session.executeQueryStatement("show child nodes " + device)) {
            boolean result = dataSet.hasNext();
            if (verbose) {
                while (dataSet.hasNext()) {
                    RowRecord record = dataSet.next();
                    logger.info(record.toString());
                }
            }
            return !result;
        }
    }

    // 删除数据库：先收集快照再逐个删除，单个失败不阻断其余（否则残留会级联污染后续用例）。
    public boolean cleanDatabases(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        List<String> databases = new ArrayList<>();
        SessionDataSet dataSet = session.executeQueryStatement("show databases");
        while (dataSet.hasNext()) {
            RowRecord record = dataSet.next();
            String database = record.getFields().get(0).toString();
            if (!database.equals("root.__audit")) {
                databases.add(database);
            }
        }
        boolean ok = true;
        for (String database : databases) {
            if (verbose) {
                logger.info("drop databases: " + database);
            }
            ok &= executeCleanupWithRetry("drop database " + database, "drop database " + database);
        }
        return ok;
    }

    // 删除设备模板：同样先收集快照，避免边删边读导致漏删。
    public boolean cleanTemplates(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        List<String> templates = new ArrayList<>();
        SessionDataSet records = session.executeQueryStatement("show schema templates");
        while (records.hasNext()) {
            templates.add(records.next().getFields().get(0).toString());
        }
        boolean ok = true;
        for (String template : templates) {
            ok &= executeCleanupWithRetry("drop schema template `" + template + "`", "drop template " + template);
        }
        if (verbose) {
            logger.info("drop templates:" + templates.size());
        }
        return ok;
    }

    /** 清理语句最多重试 3 次；返回是否成功，供 run 级清理标记判断。 */
    protected boolean executeCleanupWithRetry(String sql, String label) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                session.executeNonQueryStatement(sql);
                return true;
            } catch (Exception e) {
                if (attempt >= 3 || !isRegionUnavailable(e)) {
                    logger.warn("[plm] 清理失败（" + label + "，已尝试 " + attempt + " 次）：" + e.getMessage());
                    return false;
                }
                sleepQuietly(300L * attempt);
            }
        }
        return false;
    }

    public void insertRecordSingle(String path, TSDataType tsDataType, boolean isAligned, String alias) throws IoTDBConnectionException, StatementExecutionException {
        List<TSDataType> tsDataTypeList = new ArrayList<>(1);
        List<String> tsNames = new ArrayList<>(1);

        int index;
        if (path.endsWith("`")) {
            int endIndex = path.length() - 2;
            while (true) {
                index = path.lastIndexOf("`", endIndex) - 1;
                if (path.charAt(index) == '`') {
                    index = index - 2;
                } else {
                    break;
                }
                endIndex = index - 1;
            }
        } else {
            index = path.lastIndexOf('.');
        }
        String device = path.substring(0, index);
        String tsName = path.substring(index + 1);
//        System.out.println("device: "+device);
//        System.out.println("tsName: "+tsName);

        tsDataTypeList.add(tsDataType);
        tsNames.add(tsName);
        if (alias == null) {
            insertRecordMulti(device, tsNames, tsDataTypeList, baseTime, isAligned, null);
        } else {
            List<String> aliasList = new ArrayList<>(1);
            aliasList.add(alias);
            insertRecordMulti(device, tsNames, tsDataTypeList, baseTime, isAligned, aliasList);
        }
    }

    public void insertRecordMulti(String device, List<String> tsNames, List<TSDataType> tsDataTypeList, long timestamp, boolean isAligned, List<String> aliasList) throws IoTDBConnectionException, StatementExecutionException {
        List<Object> values = new ArrayList<>(tsDataTypeList.size());
        for (TSDataType tsDataType : tsDataTypeList) {
            switch (tsDataType) {
                case BOOLEAN:
                    values.add(GenerateValues.getBoolean());
                    break;
                case INT32:
                    values.add(GenerateValues.getInt());
                    break;
                case INT64:
                    values.add(GenerateValues.getLong(8));
                    break;
                case FLOAT:
                    values.add(GenerateValues.getFloat(2, 100, 2000));
                    break;
                case DOUBLE:
                    values.add(GenerateValues.getDouble(2, 2000, 5000));
                    break;
                case TEXT:
                    values.add(GenerateValues.getCombinedCode());
                    break;
                case STRING:
                    values.add(GenerateValues.getStringValue());
                    break;
                case TIMESTAMP:
                    values.add(GenerateValues.getTimeStamp(8));
                    break;
                case BLOB:
                    values.add(GenerateValues.getBloB());
                    break;
                case DATE:
                    values.add(GenerateValues.getDateValue());
                    break;
            }
        }
//        System.out.println("-----------");
//        System.out.println(tsDataTypeList);
//        System.out.println(values);
//        System.out.println("-----------");
        executeWithRegionRetry("insertRecord " + device, () -> {
            if (isAligned) {
                session.insertAlignedRecord(device, timestamp, tsNames, tsDataTypeList, values);
            } else {
                session.insertRecord(device, timestamp, tsNames, tsDataTypeList, values);
            }
        });
        for (int i = 0; i < tsNames.size(); i++) {
            checkQueryResult("select " + tsNames.get(i) + " from "
                    + device + " where time=" + timestamp + ";", tsDataTypeList.get(i), values.get(i));
            if (aliasList != null) {
                checkQueryResult("select " + aliasList.get(i) + " from " + device + " where time=" + timestamp + ";", tsDataTypeList.get(i), values.get(i));
            }
        }
    }

    public void insertTabletSingle(String device, String tsName, TSDataType tsDataType, int insertCount, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        List<IMeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema(tsName, tsDataType));
        insertTabletMulti(device, schemaList, insertCount, isAligned);
    }

    public void insertTabletMulti(String device, List<IMeasurementSchema> schemaList, int insertCount, boolean isAligned) throws IoTDBConnectionException, StatementExecutionException {
        // 统一使用测试类共享的 session。原先 insertCount==0 时既新建并丢弃一个连接（泄漏），
        // 又在结尾 close 共享 session 导致同类后续用例失败，属逻辑错误，已移除该特殊处理。
        Session session = this.session;
        if (verbose) {
            logger.info("insertTabletMulti device=" + device + " schema=" + schemaList.size() + " insertCount=" + insertCount);
        }
        Tablet tablet = new Tablet(device, schemaList, insertCount);
        int rowIndex = 0;
        long timestamp = baseTime;
        for (int row = 0; row < insertCount; row++) {
            timestamp += 3600000; //+1小时
            tablet.addTimestamp(rowIndex, timestamp);
            for (IMeasurementSchema iMeasurementSchema : schemaList) {
                switch (iMeasurementSchema.getType()) {
                    case BOOLEAN:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, GenerateValues.getBoolean());
                        break;
                    case INT32:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, GenerateValues.getInt());
                        break;
                    case INT64:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, GenerateValues.getLong(10));
                        break;
                    case FLOAT:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, GenerateValues.getFloat(2, 100, 200));
                        break;
                    case DOUBLE:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, GenerateValues.getDouble(2, 500, 1000));
                        break;
                    case TEXT:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, GenerateValues.getChinese());
                        break;
                    case STRING:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, GenerateValues.getChinese());
                        break;
                    case DATE:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, LocalDate.of(1970, 1, 1));
                        break;
                    case TIMESTAMP:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, GenerateValues.getLong(10));
                        break;
                    case BLOB:
                        tablet.addValue(iMeasurementSchema.getMeasurementName(), rowIndex, new Binary(GenerateValues.getChinese(), Charset.defaultCharset()));
                        break;
                }
            }
            rowIndex++;
        }
        executeWithRegionRetry("insertTablet " + device, () -> {
            if (isAligned) {
                session.insertAlignedTablet(tablet);
            } else {
                session.insertTablet(tablet);
            }
        });
        checkQueryResult("select count(" + schemaList.get(0).getMeasurementName() + ") from "
                + device + ";", TSDataType.INT32, insertCount);
    }

    public void queryLastData(String tsPath, String expectValue, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        List<String> paths = new ArrayList<>(1);
        paths.add(tsPath);
        if (expectValue != null && !expectValue.isEmpty()) {
            List<String> expectValues = new ArrayList<>(1);
            expectValues.add(expectValue);
            queryLastData(paths, expectValues, verbose, null);
        } else {
            queryLastData(paths, null, verbose, null);
        }
    }

    public void queryLastData(List<String> paths, List<String> expectValues, boolean verbose, Long gtTime) throws IoTDBConnectionException, StatementExecutionException {
        // 临时 Session 与 SessionDataSet 都用 try-with-resources，确保查询或断言抛异常时连接仍被关闭，避免连接泄漏。
        try (Session session = PrepareConnection.getSessionTreeModel()) {
            try (SessionDataSet dataSet = (gtTime != null)
                    ? session.executeLastDataQuery(paths, gtTime, 1000L)
                    : session.executeLastDataQuery(paths)) {
                if (verbose) {
                    logger.info(paths + " expect=" + expectValues);
                    logger.info(dataSet.getColumnNames());
                }
                int i = 0;
                SessionDataSet.DataIterator records = dataSet.iterator();
                while (records.next()) {
                    if (verbose) {
                        for (int j = 1; j <= dataSet.getColumnNames().size(); j++) {
                            logger.info(records.getString(j) + ",");
                        }
                        logger.info("");
                    }
                    assert expectValues == null || expectValues.isEmpty() || expectValues.get(i).equals(records.getString(1)) : paths.get(i) + " :" + expectValues.get(i) + " == " + records.getString(1);
                    i++;
                }
                // 期望有值却查不到任何 last 数据时应判定失败，否则空结果会让断言被整段跳过，造成静默漏判。
                assert expectValues == null || expectValues.isEmpty() || i == expectValues.size()
                        : "查询 last 数据行数不足: paths=" + paths + " expect " + expectValues.size() + " 行, actual " + i + " 行";
            }
        }
    }

    public int getTemplateCount(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        return countLines("show schema templates", verbose);
    }

    public int getTSCountInTemplate(String templateName, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        String sql = "show nodes in schema template " + templateName;
        return countLines(sql, verbose);
    }

    public int getSetPathsCount(String templateName, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        String sql = "show paths set schema template " + templateName;
        return countLines(sql, verbose);
    }

    // 查看使用了某个设备模板的路径（即模板在该路径上已激活，序列已创建），并返回查询到的行数
    public int getActivePathsCount(String templateName, boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        String sql = "show paths using schema template " + templateName;
        return countLines(sql, verbose);
    }

    // 解除某个设备模板
    public void deactivateTemplate(String templateName, String path) throws IoTDBConnectionException, StatementExecutionException {
        String sql = "deactivate schema template " + templateName + " from " + path;
        logger.debug(sql);
        // 用 try-with-resources 确保 executeNonQueryStatement 抛异常时临时连接仍被关闭，避免连接泄漏。
        try (Session session = PrepareConnection.getSessionTreeModel()) {
            session.executeNonQueryStatement(sql);
        }
    }

    // 解除多个设备模板
    public void deactivateTemplate(String templateName, @NotNull List<String> paths) throws IoTDBConnectionException, StatementExecutionException {
        int count = getActivePathsCount(templateName, verbose);
        count -= paths.size();
        for (String path : paths) {
            checkUsingTemplate(path, true);
            String sql = "deactivate schema template " + templateName + " from " + path;
            logger.debug(sql);
            session.executeNonQueryStatement(sql);
        }
        assert count == getActivePathsCount(templateName, verbose) : "解除成功";
    }

    public void addTSIntoTemplate(String templateName, List<String> tsNameList, List<TSDataType> tsDataTypeList, List<TSEncoding> tsEncodingList, List<CompressionType> compressionTypeList) throws IoTDBConnectionException, StatementExecutionException {
        int beforeCount = getTSCountInTemplate(templateName, false);
        int expectCount = beforeCount + tsNameList.size();
        StringJoiner sb = new StringJoiner(" ");
        sb.add("alter schema template ");
        sb.add(templateName);
        sb.add(" add (");
        for (int i = 0; i < tsNameList.size(); i++) {
            if (i > 0) {
                sb.add(",");
            }
            sb.add(tsNameList.get(i));
            sb.add(tsDataTypeList.get(i).toString());
            sb.add("encoding=");
            sb.add(tsEncodingList.get(i).toString());
            sb.add("compression=");
            sb.add(compressionTypeList.get(i).toString());
        }
        sb.add(");");
//        System.out.println(sb.toString());
        session.executeNonQueryStatement(sb.toString());
        int actualCount = getTSCountInTemplate(templateName, false);
        logger.debug("beforeCount=" + beforeCount);
        assert expectCount == actualCount : "成功修改模版 expect=" + expectCount + " actual=" + actualCount;
    }

    public void addTSIntoTemplate(String templateName, String tsName, TSDataType tsDataType, TSEncoding tsEncoding, CompressionType compressionType, Session conn) throws IoTDBConnectionException, StatementExecutionException {
//        int count = getTSCountInTemplate(templateName, false);
//        count ++;
        StringJoiner sb = new StringJoiner(" ");
        sb.add("alter schema template ");
        sb.add(templateName);
        sb.add(" add (");
        sb.add(tsName);
        sb.add(tsDataType.toString());
        sb.add("encoding=");
        sb.add(tsEncoding.toString());
        sb.add("compression=");
        sb.add(compressionType.toString());
        sb.add(");");
        if (conn == null) {
            session.executeNonQueryStatement(sb.toString());
        } else {
            conn.executeNonQueryStatement(sb.toString());
            conn.close();
        }
//        assert count == getTSCountInTemplate(templateName, false) : "成功修改模版";
    }

    public void cleanTemplateNodes(String templateName, String prefix) throws IoTDBConnectionException, StatementExecutionException {
        // 两个查询结果集都用 try-with-resources 关闭，避免 SessionDataSet 泄漏。
        String usingSql = "show paths using schema template " + templateName;
        try (SessionDataSet records = session.executeQueryStatement(usingSql)) {
            SessionDataSet.DataIterator recordsIter = records.iterator();
            while (recordsIter.next()) {
                if (recordsIter.getString(1).startsWith(prefix)) {
                    deactivateTemplate(templateName, recordsIter.getString(1));
                }
            }
        }
        String setSql = "show paths set schema template " + templateName;
        try (SessionDataSet records = session.executeQueryStatement(setSql)) {
            SessionDataSet.DataIterator recordsIter = records.iterator();
            while (recordsIter.next()) {
                if (recordsIter.getString(1).startsWith(prefix)) {
                    session.unsetSchemaTemplate(recordsIter.getString(1), templateName);
                }
            }
        }
    }

}
