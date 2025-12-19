package org.apache.iotdb.api.test.tree.session;


import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;


/**
 * <p>Title：测试 SessionBuilder 接口正常情况的会话构建功能<p/>
 * <p>Describe：测试树模型各种构建参数建立连接是否正常，测试构建连接后部分操作session配置的方法是否正常<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2025/11<p/>
 */
public class TestSessionBuilderNormal {
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
    private void testValidity(Session session, String databaseNameSuffix) {
        String databaseName = "root.testSessionBuilderNormal" + "." + databaseNameSuffix;
        String deviceId = databaseName + "." + "fdq";
        String measurementName = "s_boolean";
        String path = deviceId + "." + measurementName;
        try {
            session.createDatabase(databaseName);
            session.createTimeseries(path, TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.LZ4);
            session.insertRecord(deviceId, 1, Collections.singletonList("s_boolean"), Collections.singletonList(TSDataType.BOOLEAN), Collections.singletonList(true));
            try(SessionDataSet dataSet = session.executeQueryStatement("select s_boolean from " + deviceId, 60000)) {
                while (dataSet.hasNext()) {
                    RowRecord record = dataSet.next();
                    assert record.getTimestamp() == 1 : "期待值和实际不一致，期待：1，实际：" + record.getTimestamp();
                    assert record.getField(0).getBoolV() : "期待值和实际不一致，期待：true，实际：" + record.getField(1).getBoolV();
                }
                session.deleteData(path, Long.MAX_VALUE);
                session.deleteTimeseries(path);
            }
            session.deleteDatabase(databaseName);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试使用全部参数构建Session（通过Builder）
     */
    @Test(priority = 10)
    public void testSessionBuilderAllParam() throws IoTDBConnectionException, StatementExecutionException {
        // 1、nodeUrls 为空时，使用host和port构建
        try (Session session1 = new Session.Builder()
                .host(config.getValue("host"))
                .port(Integer.parseInt(config.getValue("port")))
                .username(config.getValue("user"))
                .password(config.getValue("password"))
                .fetchSize(5000)
                .zoneId(null)
                .enableIoTDBRpcCompression(true)
                .enableThriftRpcCompression(false)
                .thriftDefaultBufferSize(1024)
                .thriftMaxFrameSize(67108864)
                .enableRedirection(true)
                .enableRecordsAutoConvertTablet(true)
                .nodeUrls(null)
                .version(Version.V_1_0)
                .timeOut(60000)
                .enableAutoFetch(true)
                .maxRetryCount(60)
                .retryIntervalInMs(500)
                .sqlDialect("tree")
                .database(null)
                .useSSL(false)
                .trustStore("")
                .trustStorePwd("")
                .build()) {
            session1.open(false);
            testValidity(session1, "testSessionBuilderAllParam1");
        }
        // 2、nodeUrls 非空时，使用nodeUrls构建
        try (Session session2 = new Session.Builder()
                .nodeUrls(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")))
                .username(config.getValue("user"))
                .password(config.getValue("password"))
                .fetchSize(5000)
                .zoneId(null)
                .enableIoTDBRpcCompression(true)
                .enableThriftRpcCompression(false)
                .thriftDefaultBufferSize(1024)
                .thriftMaxFrameSize(67108864)
                .enableRedirection(true)
                .enableRecordsAutoConvertTablet(true)
                .version(Version.V_1_0)
                .timeOut(60000)
                .enableAutoFetch(true)
                .maxRetryCount(60)
                .retryIntervalInMs(500)
                .sqlDialect("tree")
                .database(null)
                .useSSL(false)
                .trustStore("")
                .trustStorePwd("")
                .build()) {
            session2.open(false);
            testValidity(session2, "testSessionBuilderAllParam2");
        }
    }

    /**
     * 测试使用各种参数构建Session TODO：待完善
     */
    @Test(priority = 20)
    public void testSessionBuilderPartParam() throws IoTDBConnectionException, StatementExecutionException {
        // 1、enableAutoFetch为false
        try (Session session1 = new Session.Builder()
                .enableAutoFetch(false)
                .build()) {
            session1.open(false);
            testValidity(session1, "testSessionBuilderPartParam1");
        }
    }


}
