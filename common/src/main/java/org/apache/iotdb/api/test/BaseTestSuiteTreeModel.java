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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
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
    }

    @AfterClass
    public void afterSuie() throws IoTDBConnectionException, StatementExecutionException, IOException {
        cleanDatabases(verbose);
        cleanTemplates(verbose);
        if (ReadConfig.getInstance().getValue("is_sessionPool").equals("false")) {
            session.close();
        } else {
            session.close();
            sessionPool.close();
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

    // 删除数据库
    public void cleanDatabases(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet dataSet = session.executeQueryStatement("show databases");
        while (dataSet.hasNext()) {
            RowRecord record = dataSet.next();
            String database = record.getFields().get(0).toString();
            if (verbose) {
                logger.info("drop databases: " + database);
            }
            if (!database.equals("root.__audit")) {
                session.executeNonQueryStatement("drop database " + database);
            }
        }
    }

    // 删除设备模板
    public void cleanTemplates(boolean verbose) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement("show schema templates");
        int count = 0;
        while (records.hasNext()) {
            count++;
            session.dropSchemaTemplate("`" + records.next().getFields().get(0) + "`");
        }
        if (verbose) {
            logger.info("drop templates:" + count);
        }
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
        if (isAligned) {
            session.insertAlignedRecord(device, timestamp, tsNames, tsDataTypeList, values);
        } else {
            session.insertRecord(device, timestamp, tsNames, tsDataTypeList, values);
        }
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
        if (isAligned) {
            session.insertAlignedTablet(tablet);
        } else {
            session.insertTablet(tablet);
        }
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
