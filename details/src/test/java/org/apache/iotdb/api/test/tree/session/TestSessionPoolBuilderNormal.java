package org.apache.iotdb.api.test.tree.session;


import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
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
import java.util.Collections;


/**
 * <p>Title：测试 SessionPoolBuilder 接口正常情况的会话构建功能<p/>
 * <p>Describe：测试树模型各种构建参数建立连接是否正常，测试构建连接后部分操作session配置的方法是否正常<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2025/11<p/>
 */
public class TestSessionPoolBuilderNormal {
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
    private void testValidity(SessionPool sessionPool, String databaseNameSuffix) {
        String databaseName = "root.testSessionPoolBuilderNormal" + "." + databaseNameSuffix;
        String deviceId = databaseName + "." + "fdq";
        String measurementName = "s_boolean";
        String path = deviceId + "." + measurementName;
        try {
            sessionPool.createDatabase(databaseName);
            sessionPool.createTimeseries(path, TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.LZ4);
            sessionPool.insertRecord(deviceId, 1, Collections.singletonList("s_boolean"), Collections.singletonList(TSDataType.BOOLEAN), Collections.singletonList(true));
            try (SessionDataSetWrapper dataSet = sessionPool.executeQueryStatement("select s_boolean from " + deviceId)) {
                while (dataSet.hasNext()) {
                    RowRecord record = dataSet.next();
                    assert record.getTimestamp() == 1 : "期待值和实际不一致，期待：1，实际：" + record.getTimestamp();
                    assert record.getField(0).getBoolV() : "期待值和实际不一致，期待：true，实际：" + record.getField(1).getBoolV();
                }
                sessionPool.deleteData(path, Long.MAX_VALUE);
                sessionPool.deleteTimeseries(path);
            }
            sessionPool.deleteDatabase(databaseName);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试使用全部参数构建Session（通过Builder）
     */
    @Test(priority = 10)
    public void testSessionPoolBuilderAllParam() {
        SessionPool sessionPool = null;
        // 1、nodeUrls 为空时，使用host和port构建
        try {
            sessionPool = new SessionPool.Builder()
                    .host(config.getValue("host"))
                    .port(Integer.parseInt(config.getValue("port")))
                    .user(config.getValue("user"))
                    .password(config.getValue("password"))
                    .useSSL(false)
                    .trustStore("")
                    .trustStorePwd("")
                    .maxSize(SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE)
                    .fetchSize(SessionConfig.DEFAULT_FETCH_SIZE)
                    .zoneId(null)
                    .waitToGetSessionTimeoutInMs(60_000)
                    .thriftDefaultBufferSize(SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY)
                    .thriftMaxFrameSize(SessionConfig.DEFAULT_MAX_FRAME_SIZE)
                    .enableThriftRpcCompaction(false)
                    .enableIoTDBRpcCompression(true)
                    .enableRedirection(SessionConfig.DEFAULT_REDIRECTION_MODE)
                    .enableRecordsAutoConvertTablet(SessionConfig.DEFAULT_RECORDS_AUTO_CONVERT_TABLET)
                    .connectionTimeoutInMs(SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS)
                    .version(SessionConfig.DEFAULT_VERSION)
                    .enableAutoFetch(SessionConfig.DEFAULT_ENABLE_AUTO_FETCH)
                    .maxRetryCount(SessionConfig.MAX_RETRY_COUNT)
                    .retryIntervalInMs(SessionConfig.RETRY_INTERVAL_IN_MS)
                    .queryTimeoutInMs(SessionConfig.DEFAULT_QUERY_TIME_OUT)
                    .build();
            testValidity(sessionPool, "AllParam1");
        } finally {
            assert sessionPool != null : "sessionPool为null";
            sessionPool.close();
        }

        // 2、nodeUrls 非空时，使用nodeUrls构建
        try {
            sessionPool = new SessionPool.Builder()
                    .nodeUrls(Collections.singletonList(config.getValue("host") + ":" + config.getValue("port")))
                    .user(config.getValue("user"))
                    .password(config.getValue("password"))
                    .useSSL(false)
                    .trustStore("")
                    .trustStorePwd("")
                    .maxSize(SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE)
                    .fetchSize(SessionConfig.DEFAULT_FETCH_SIZE)
                    .zoneId(null)
                    .waitToGetSessionTimeoutInMs(60_000)
                    .thriftDefaultBufferSize(SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY)
                    .thriftMaxFrameSize(SessionConfig.DEFAULT_MAX_FRAME_SIZE)
                    .enableThriftRpcCompaction(false)
                    .enableIoTDBRpcCompression(true)
                    .enableRedirection(SessionConfig.DEFAULT_REDIRECTION_MODE)
                    .enableRecordsAutoConvertTablet(SessionConfig.DEFAULT_RECORDS_AUTO_CONVERT_TABLET)
                    .connectionTimeoutInMs(SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS)
                    .version(SessionConfig.DEFAULT_VERSION)
                    .enableAutoFetch(SessionConfig.DEFAULT_ENABLE_AUTO_FETCH)
                    .maxRetryCount(SessionConfig.MAX_RETRY_COUNT)
                    .retryIntervalInMs(SessionConfig.RETRY_INTERVAL_IN_MS)
                    .queryTimeoutInMs(SessionConfig.DEFAULT_QUERY_TIME_OUT)
                    .build();
            testValidity(sessionPool, "AllParam2");
        } finally {
            assert sessionPool != null : "sessionPool为null";
            sessionPool.close();
        }
    }

    /**
     * 测试使用各种参数构建SessionPool TODO：待完善
     */
    @Test(priority = 20)
    public void testSessionBuilderPartParam() {
        SessionPool sessionPool = null;
        // 1. 测试只使用host和port构建Session
        try {
            sessionPool = new SessionPool.Builder()
                    .host(config.getValue("host"))
                    .port(Integer.parseInt(config.getValue("port")))
                    .build();
            testValidity(sessionPool, "PartParam1");
        } finally {
            assert sessionPool != null : "sessionPool为null";
            sessionPool.close();
        }
    }

}
