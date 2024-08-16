package org.apache.iotdb.api.test.utils;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import java.io.IOException;
import java.util.Arrays;

/**
 * Title：连接数据库
 * Describe：用于调用 Java Session 连接iotdb数据库
 */
public class PrepareConnection {
    private static ReadConfig config;
    private static Session session;

    static {
        try {
            config = ReadConfig.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 用于获取Session对象（表模型）
     */
    public static Session getSession_TableModel() throws IoTDBConnectionException {
        Session session = null;

        // 非集群模式下，使用单个节点的URL和端口创建Session。
        session = new Session.Builder()
                .host(config.getValue("host")) // 设置主机地址
                .port(Integer.parseInt(config.getValue("port"))) // 设置端口号
                .username(config.getValue("user")) // 设置用户名
                .password(config.getValue("password")) // 设置密码
                .version(Version.V_1_0)           // 版本
                .sqlDialect("table")              // 表模型标识符
                .enableRedirection(false) // 设置是否启用重定向
                .maxRetryCount(0) // 设置最大重试次数
                .build();

        // 打开Session，并设置获取数据时的批量大小。
        session.open(false);
        session.setFetchSize(10000);
        return session;
    }

    /**
     * 用于获取Session对象（树模型）
     */
    public static Session getSession() throws IoTDBConnectionException {
        Session session = null;
        // 判断是否是集群
        if (config.getValue("is_cluster").equals("true")) {
            String host_nodes_str = config.getValue("host_nodes");
            session = new Session.Builder()
                    .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                    .username(config.getValue("user"))
                    .password(config.getValue("password"))
                    .enableRedirection(false)
                    .maxRetryCount(0)
                    .build();
        } else {
            session = new Session.Builder()
                    .host(config.getValue("host"))
                    .port(Integer.parseInt(config.getValue("port")))
                    .username(config.getValue("user"))
                    .password(config.getValue("password"))
                    .enableRedirection(false)
                    .maxRetryCount(0)
                    .build();
        }
        session.open(false);
        // set session fetchSize
        session.setFetchSize(10000);
        return session;
    }

    public static SessionPool getSessionPool() {
        SessionPool sessionPool = null;
        if (config.getValue("is_cluster").equals("true")) {
            String host_nodes_str = config.getValue("host_nodes");
            sessionPool = new SessionPool.Builder()
                    .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                    .user(config.getValue("user"))
                    .password(config.getValue("password"))
                    .maxSize(10)
                    .maxRetryCount(0)
//                    .timeOut(Long.parseLong(config.getValue("session_timeout")))
                    .build();
        } else {
            sessionPool = new SessionPool.Builder()
                    .host(config.getValue("host"))
                    .port(Integer.parseInt(config.getValue("port")))
                    .user(config.getValue("user"))
                    .password(config.getValue("password"))
                    .maxSize(10)
                    .maxRetryCount(0)
//                    .timeOut(Long.parseLong(config.getValue("session_timeout")))
                    .build();
        }

        // set session fetchSize
        sessionPool.setFetchSize(10000);
        return sessionPool;
    }

    public static void main(String[] args) throws IOException, IoTDBConnectionException, StatementExecutionException {
        String ROOT_SG1_D1 = "root.multi.d1";
        String host="172.20.70.45";
        long timestamp = 601L;

    }
}
