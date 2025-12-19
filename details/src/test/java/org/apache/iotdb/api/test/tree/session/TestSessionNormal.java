package org.apache.iotdb.api.test.tree.session;


import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;


/**
 * <p>Title：测试 Session 接口正常情况的会话构建功能<p/>
 * <p>Describe：测试树模型各种构建参数建立连接是否正常，测试构建连接后部分操作session配置的方法是否正常<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2025/11<p/>
 */
public class TestSessionNormal {
    // 读取配置文件
    private static ReadConfig config;

    // 在测试类之前读取配置文件
    @BeforeClass
    public void beforeClass() throws IOException {
        config = ReadConfig.getInstance();
    }

    /**
     * 测试Session会话是否构建正常
     */
    private void testValidity(Session session, String databaseNameSuffix) throws IoTDBConnectionException, StatementExecutionException {
        String databaseName = "root.testSessionNormal" + "." + databaseNameSuffix;
        String deviceId = databaseName + "." + "fdq";
        String measurementName = "s_boolean";
        String path = deviceId + "." + measurementName;
        try {
            session.createDatabase(databaseName);
            session.createTimeseries(path, TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.LZ4);
            session.insertRecord(deviceId, 1, Collections.singletonList("s_boolean"), Collections.singletonList(TSDataType.BOOLEAN), Collections.singletonList(true));
            SessionDataSet dataSet = session.executeQueryStatement("select s_boolean from " + deviceId, 60000);
            while (dataSet.hasNext()) {
                RowRecord record = dataSet.next();
                assert record.getTimestamp() == 1 : "期待值和实际不一致，期待：1，实际：" + record.getTimestamp();
                assert record.getField(0).getBoolV() : "期待值和实际不一致，期待：true，实际：" + record.getField(1).getBoolV();
            }
            session.deleteData(path, Long.MAX_VALUE);
            session.deleteTimeseries(path);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            session.deleteDatabase(databaseName);
        }
    }
    /**
     * 测试使用各种参数构建Session
     */
    @Test(priority = 10)
    public void testSessionGenerate() throws IoTDBConnectionException, StatementExecutionException {
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")))) {
            session.open(false);
            testValidity(session, "testSessionPartParam1");
        }
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"))) {
            session.open(false);
            testValidity(session, "testSessionPartParam2");
        }
        try (Session session = new Session(config.getValue("host"), config.getValue("port"), config.getValue("user"), config.getValue("password"))) {
            session.open(false);
            testValidity(session, "testSessionPartParam3");
        }
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")),
                config.getValue("user"), config.getValue("password"), 5000)) {
            session.open(false);
            testValidity(session, "testSessionPartParam4");
        }
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")),
                config.getValue("user"), config.getValue("password"), 5000, null, true)) {
            session.open();
            testValidity(session, "testSessionPartParam5");
        }
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")),
                config.getValue("user"), config.getValue("password"), 5000, null, 1024, 67108864, true, Version.V_1_0)) {
            session.open(false);
            testValidity(session, "testSessionPartParam6");
        }
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), null)) {
            session.open(false);
            testValidity(session, "testSessionPartParam7");
        }
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), true)) {
            session.open(false);
            testValidity(session, "testSessionPartParam8");
        }
        try (Session session = new Session(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"))) {
            session.open(false);
            testValidity(session, "testSessionPartParam9");
        }
        try (Session session = new Session(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"), 10000)) {
            session.open(false);
            testValidity(session, "testSessionPartParam10");
        }
        try (Session session = new Session(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"), null)) {
            session.open(false);
            testValidity(session, "testSessionPartParam11");
        }
        try (Session session = new Session(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"), 10000,
                null, 1024, 67108864, true, Version.V_1_0)) {
            session.open(false);
            testValidity(session, "testSessionPartParam12");
        }
    }

    /**
     * 测试Session会话其他方法
     */
    @Test(priority = 20)
    public void testSessionOtherMethod() throws IoTDBConnectionException, StatementExecutionException, TException {
        // 1、测试使用各种参数构建Session
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")))) {
            session.open();
            // 测试已开启的会话再次open，默认直接返回，不会有异常
            session.open(false);
            // 测试是否开启查询重定向
            assert !session.isEnableQueryRedirection() : "期待值和实际不一致，期待：false，实际：" + session.isEnableQueryRedirection();
            session.setEnableQueryRedirection(true);
            assert session.isEnableQueryRedirection() : "期待值和实际不一致，期待：true，实际：" + session.isEnableQueryRedirection();
            // 测试是否开启客户端重定向
            assert session.isEnableRedirection() : "期待值和实际不一致，期待：true，实际：" + session.isEnableRedirection();
            session.setEnableRedirection(false);
            assert !session.isEnableRedirection() : "期待值和实际不一致，期待：false，实际：" + session.isEnableRedirection();
            // 测试获取FetchSize
            session.setFetchSize(session.getFetchSize());
            assert session.getFetchSize() == 5000 : "期待值和实际不一致，期待：5000，实际：" + session.getFetchSize();
            session.setFetchSize(500);
            assert session.getFetchSize() == 500 : "期待值和实际不一致，期待：500，实际：" + session.getFetchSize();
            session.setFetchSize(50000);
            assert session.getFetchSize() == 50000 : "期待值和实际不一致，期待：50000，实际：" + session.getFetchSize();
            // 测试获取版本
            session.setVersion(session.getVersion());
            assert session.getVersion() == Version.V_1_0 : "期待值和实际不一致，期待：Version.V_1_0，实际：" + session.getVersion();
            // 测试获取时区（会发送给服务端）
            session.setTimeZone(session.getTimeZone());
            assert session.getTimeZone().equals("Asia/Shanghai") : "期待值和实际不一致，期待：Asia/Shanghai，实际：" + session.getTimeZone();
            session.setTimeZone("America/New_York");
            assert session.getTimeZone().equals("America/New_York") : "期待值和实际不一致，期待：America/New_York，实际：" + session.getTimeZone();
            session.setTimeZone("UTC-05:00");
            assert session.getTimeZone().equals("UTC-05:00") : "期待值和实际不一致，期待：UTC-05:00，实际：" + session.getTimeZone();
            // 测试获取时区（仅更改会话对象的成员变量，而不将其发送到服务器）
            session.setTimeZoneOfSession("Asia/Shanghai");
            assert session.getTimeZone().equals("Asia/Shanghai") : "期待值和实际不一致，期待：Asia/Shanghai，实际：" + session.getTimeZone();
            // 测试获取查询超时
            session.setQueryTimeout(session.getQueryTimeout());
            assert session.getQueryTimeout() == -1 : "期待值和实际不一致，期待：-1，实际：" + session.getQueryTimeout();
            session.setQueryTimeout(100000);
            assert session.getQueryTimeout() == 100000 : "期待值和实际不一致，期待：100000，实际：" + session.getQueryTimeout();
            // 测试获取时间精度（TODO：测试的说默认配置情况的IoTDB时间精度，若修改了精度，当前测试需要停止或修改）
            assert session.getTimestampPrecision().equals("ms") : "期待值和实际不一致，期待：ms，实际：" + session.getTimestampPrecision();
            testValidity(session, "testSessionOtherMethod1");
        }
        // 2、测试不同的open参数
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")))
        ) {
            session.open(false, 0);
            testValidity(session, "testSessionOtherMethod2");
        }
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")))
        ) {
            session.open(false, 0, new ConcurrentHashMap<>(), null);
            testValidity(session, "testSessionOtherMethod3");
        }
        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")))
        ) {
            session.open(false, 0, new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), null);
            testValidity(session, "testSessionOtherMethod4");
        }
    }

//    /**
//     * 测试移除损坏的会话连接（只有在启用重定向功能（enableRedirection 为 true）时才执行清理操，默认开启）TODO：存在问题，需要修改测试代码
//     */
//    @Test(priority = 30)
//    public void testSessionRemove() throws IoTDBConnectionException {
//        try (Session session = new Session(config.getValue("host"), Integer.parseInt(config.getValue("port")))
//        ) {
//            session.open(false);
//            // 从 Session 的内部映射中移除损坏的连接
//            session.removeBrokenSessionConnection(
//                    new SessionConnection(
//                            session,
//                            ZoneId.of("Asia/Shanghai"),
//                            () -> {
//                                List<TEndPoint> endpoints = new ArrayList<>();
//                                String[] hostPort = (config.getValue("host") + ":" + config.getValue("port")).split(":");
//                                endpoints.add(new TEndPoint(hostPort[0], Integer.parseInt(hostPort[1])));
//                                return endpoints;
//                            },
//                            60,
//                            500,
//                            "tree",
//                            "root.testSessionOtherMethod1"));
//        }
//    }

}
