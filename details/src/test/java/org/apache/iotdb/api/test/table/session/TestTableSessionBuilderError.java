package org.apache.iotdb.api.test.table.session;

import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Title：测试 TableSessionBuilder 接口
 * Describe：测试抽象出现新接口 TableSessionBuilder
 * Author：肖林捷
 * Date：2024/12/29
 */
public class TestTableSessionBuilderError {
    // 用于获取配置文件中的配置
    private static ReadConfig config;
    // url
    private static String LOCAL_URLS;
    private static String LOCAL_URL;

    // 初始化
    static {
        try {
            config = ReadConfig.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 判断是否是集群
        if (config.getValue("is_cluster").equals("true")) {
            LOCAL_URLS = config.getValue("host_nodes");
        } else {
            LOCAL_URL = config.getValue("url");
        }
    }

    /**
     * 测试错误的 nodeUrls 的host
     */
    @Test(priority = 10)
    public void testErrorNodeUrlsHost() {
        if (config.getValue("is_cluster").equals("false")) {
            // 一个节点的url单机
            try {
                ITableSession session =
                        new TableSessionBuilder()
                                .nodeUrls(Collections.singletonList("192.0.0.1:6667"))
                                .build();
            } catch (Exception e) {
                assert e instanceof IoTDBConnectionException : "Error and expectation are inconsistent, expect: IoTDBConnectionException, actual: " + e.getClass().getName();
                assert e.getMessage().contains("Fail to reconnect to server. Please check server status.192.0.0.1:6667") : "Error and expectation are inconsistent, expect: Fail to reconnect to server. Please check server status.192.0.0.1:6667, actual: " + e.getMessage();
            }
        } else {
            // 多个节点的url集群
            try {
                ITableSession session =
                        new TableSessionBuilder()
                                .nodeUrls(Arrays.asList("192.0.0.1:6667", "192.0.0.2:6667", "192.0.0.3:6667"))
                                .build();
            } catch (Exception e) {
                assert e instanceof IoTDBConnectionException : "Error and expectation are inconsistent, expect: IoTDBConnectionException, actual: " + e.getClass().getName();
//                assert e.getMessage().contains("Fail to reconnect to server. Please check server status.192.0.0.1:6667") : "Error and expectation are inconsistent, expect: Fail to reconnect to server. Please check server status.192.0.0.1:6667, actual: " + e.getMessage();
            }
        }
    }

    /**
     * 测试错误的 nodeUrls 的端口
     */
    @Test(priority = 20)
    public void testErrorNodeUrlsPort() {
        if (config.getValue("is_cluster").equals("false")) {
            // 一个节点的url单机
            try {
                ITableSession session =
                        new TableSessionBuilder()
                                .nodeUrls(Collections.singletonList("127.0.0.1:9999"))
                                .build();
            } catch (Exception e) {
                assert e instanceof IoTDBConnectionException : "Error and expectation are inconsistent, expect: IoTDBConnectionException, actual: " + e.getClass().getName();
                assert e.getMessage().contains("Fail to reconnect to server. Please check server status.127.0.0.1:9999") : "Error and expectation are inconsistent, expect: Fail to reconnect to server. Please check server status.127.0.0.1:9999, actual: " + e.getMessage();
            }
        } else {
            // 多个节点的url集群
            try {
                ITableSession session =
                        new TableSessionBuilder()
                                .nodeUrls(Arrays.asList("127.0.0.1:9997", "127.0.0.2:9998", "127.0.0.3:9999"))
                                .build();
            } catch (Exception e) {
                assert e instanceof IoTDBConnectionException : "Error and expectation are inconsistent, expect: IoTDBConnectionException, actual: " + e.getClass().getName();
//                assert e.getMessage().contains("Fail to reconnect to server. Please check server status.127.0.0.1:9997") : "Error and expectation are inconsistent, expect: Fail to reconnect to server. Please check server status.127.0.0.1:9997, actual: " + e.getMessage();
            }
        }
    }

    /**
     * 测试错误的用户名和密码
     */
    @Test(priority = 30)
    public void testErrorUserNameAndPassword() {
        if (config.getValue("is_cluster").equals("false")) {
            try {
                ITableSession session =
                        new TableSessionBuilder()
                                .username("error")
                                .password("error")
                                .build();
            } catch (Exception e) {
                assert e instanceof IoTDBConnectionException : "Error and expectation are inconsistent, expect: IoTDBConnectionException, actual: " + e.getClass().getName();
                assert e.getMessage().contains("804: The user error does not exist.") : "Error and expectation are inconsistent, expect: 804: The user error does not exist., actual: " + e.getMessage();
            }
        } else {
            try {
                ITableSession session =
                        new TableSessionBuilder()
                                .username("error")
                                .password("error")
                                .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                .build();
            } catch (Exception e) {
                assert e instanceof IoTDBConnectionException : "Error and expectation are inconsistent, expect: IoTDBConnectionException, actual: " + e.getClass().getName();
                assert e.getMessage().contains("804: The user error does not exist.") : "Error and expectation are inconsistent, expect: 804: The user error does not exist., actual: " + e.getMessage();
            }
        }

    }

}