package org.apache.iotdb.api.test.tree.session;


import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;


/**
 * <p>Title：测试 SessionPool 接口正常情况的会话构建功能<p/>
 * <p>Describe：测试树模型各种构建参数建立连接是否正常，测试构建连接后部分操作session配置的方法是否正常<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2025/11<p/>
 */
public class TestSessionPoolNormal {
    // 读取配置文件
    private static ReadConfig config;

    // 在测试类之前读取配置文件
    @BeforeClass
    public void beforeClass() throws IOException {
        config = ReadConfig.getInstance();
    }

    /**
     * 测试 SessionPool 会话是否构建正常
     */
    private void testValidity(SessionPool sessionPool, String databaseNameSuffix) throws IoTDBConnectionException, StatementExecutionException {
        String databaseName = "root.testSessionPoolNormal" + "." + databaseNameSuffix;
        String deviceId = databaseName + "." + "fdq";
        String measurementName = "s_boolean";
        String path = deviceId + "." + measurementName;
        try {
            sessionPool.createDatabase(databaseName);
            sessionPool.createTimeseries(path, TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.LZ4);
            sessionPool.insertRecord(deviceId, 1, Collections.singletonList("s_boolean"), Collections.singletonList(TSDataType.BOOLEAN), Collections.singletonList(true));
            SessionDataSetWrapper dataSet = sessionPool.executeQueryStatement("select s_boolean from " + deviceId, 60000);
            while (dataSet.hasNext()) {
                RowRecord record = dataSet.next();
                assert record.getTimestamp() == 1 : "期待值和实际不一致，期待：1，实际：" + record.getTimestamp();
                assert record.getField(0).getBoolV() : "期待值和实际不一致，期待：true，实际：" + record.getField(1).getBoolV();
            }
            sessionPool.deleteData(path, Long.MAX_VALUE);
            sessionPool.deleteTimeseries(path);
            dataSet.close();
            sessionPool.deleteDatabase(databaseName);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试使用全部参数构建SessionPool
     */
    @Test(priority = 10)
    public void testSessionPoolGenerate() {
        try {
            SessionPool sessionPool1 = new SessionPool(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE);
            testValidity(sessionPool1, "Generate1");
            sessionPool1.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool2 = new SessionPool(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE);
            testValidity(sessionPool2, "Generate2");
            sessionPool2.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool3 = new SessionPool(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, false);
            testValidity(sessionPool3, "Generate3");
            sessionPool3.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool4 = new SessionPool(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, false);
            testValidity(sessionPool4, "Generate4");
            sessionPool4.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool5 = new SessionPool(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, false, SessionConfig.DEFAULT_REDIRECTION_MODE);
            testValidity(sessionPool5, "Generate5");
            sessionPool5.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool6 = new SessionPool(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, false, SessionConfig.DEFAULT_REDIRECTION_MODE);
            testValidity(sessionPool6, "Generate6");
            sessionPool6.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool7 = new SessionPool(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, null);
            testValidity(sessionPool7, "Generate7");
            sessionPool7.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool8 = new SessionPool(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, null);
            testValidity(sessionPool8, "Generate8");
            sessionPool8.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool9 = new SessionPool(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, SessionConfig.DEFAULT_FETCH_SIZE, 60_000, false, null, true, SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS, SessionConfig.DEFAULT_VERSION, SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY, SessionConfig.DEFAULT_MAX_FRAME_SIZE);
            testValidity(sessionPool9, "Generate9");
            sessionPool9.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool10 = new SessionPool(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, SessionConfig.DEFAULT_FETCH_SIZE, 60_000, false, null, true, SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS, SessionConfig.DEFAULT_VERSION, SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY, SessionConfig.DEFAULT_MAX_FRAME_SIZE);
            testValidity(sessionPool10, "Generate10");
            sessionPool10.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            SessionPool sessionPool11 = new SessionPool(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE, SessionConfig.DEFAULT_FETCH_SIZE, 60_000, false, null, true, SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS, SessionConfig.DEFAULT_VERSION, SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY, SessionConfig.DEFAULT_MAX_FRAME_SIZE, false, "", "");
            testValidity(sessionPool11, "Generate11");
            sessionPool11.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试 SessionPool 其他方法
     */
    @Test(priority = 20)
    public void testSessionPoolOther() throws IoTDBConnectionException, StatementExecutionException {
        SessionPool sessionPool1 = new SessionPool(config.getValue("host"), Integer.parseInt(config.getValue("port")), config.getValue("user"), config.getValue("password"), SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE);
        // 1. 测试设置FetchSize
        sessionPool1.setFetchSize(1000);
        assert sessionPool1.getFetchSize() == 1000 : "期待值和实际不一致，期待：1000，实际：" + sessionPool1.getFetchSize();
        // 2. 测试是否开启查询重定向
        assert !sessionPool1.isEnableQueryRedirection() : "期待值和实际不一致，期待：false，实际：" + sessionPool1.isEnableQueryRedirection();
        sessionPool1.setEnableQueryRedirection(true);
        assert sessionPool1.isEnableQueryRedirection() : "期待值和实际不一致，期待：true，实际：" + sessionPool1.isEnableQueryRedirection();
        // 测试是否开启客户端重定向
        assert sessionPool1.isEnableRedirection() : "期待值和实际不一致，期待：true，实际：" + sessionPool1.isEnableRedirection();
        sessionPool1.setEnableRedirection(false);
        assert !sessionPool1.isEnableRedirection() : "期待值和实际不一致，期待：false，实际：" + sessionPool1.isEnableRedirection();
        // 测试获取FetchSize
        sessionPool1.setFetchSize(sessionPool1.getFetchSize());
        assert sessionPool1.getFetchSize() == 1000 : "期待值和实际不一致，期待：1000，实际：" + sessionPool1.getFetchSize();
        sessionPool1.setFetchSize(500);
        assert sessionPool1.getFetchSize() == 500 : "期待值和实际不一致，期待：500，实际：" + sessionPool1.getFetchSize();
        sessionPool1.setFetchSize(50000);
        assert sessionPool1.getFetchSize() == 50000 : "期待值和实际不一致，期待：50000，实际：" + sessionPool1.getFetchSize();
        // 测试获取版本
        sessionPool1.setVersion(sessionPool1.getVersion());
        assert sessionPool1.getVersion() == Version.V_1_0 : "期待值和实际不一致，期待：Version.V_1_0，实际：" + sessionPool1.getVersion();
        // 测试获取时区（会发送给服务端）
        sessionPool1.setTimeZone("America/New_York");
        assert sessionPool1.getZoneId().equals(ZoneId.of("America/New_York")) : "期待值和实际不一致，期待：America/New_York，实际：" + sessionPool1.getZoneId();
        sessionPool1.setTimeZone("UTC-05:00");
        assert sessionPool1.getZoneId().equals(ZoneId.of("UTC-05:00")) : "期待值和实际不一致，期待：UTC-05:00，实际：" + sessionPool1.getZoneId();
        // 测试获取查询超时
        sessionPool1.setQueryTimeout(sessionPool1.getQueryTimeout());
        assert sessionPool1.getQueryTimeout() == -1 : "期待值和实际不一致，期待：-1，实际：" + sessionPool1.getQueryTimeout();
        sessionPool1.setQueryTimeout(100000);
        assert sessionPool1.getQueryTimeout() == 100000 : "期待值和实际不一致，期待：100000，实际：" + sessionPool1.getQueryTimeout();
        // 测试获取时间精度（TODO：测试的说默认配置情况的IoTDB时间精度，若修改了精度，当前测试需要停止或修改）
        assert sessionPool1.getTimestampPrecision().equals("ms") : "期待值和实际不一致，期待：ms，实际：" + sessionPool1.getTimestampPrecision();
        sessionPool1.close();
    }

}
