package org.apache.iotdb.api.test.utils;

import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.iotdb.isession.ITableSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Title：连接数据库
 * Describe：用于调用 Java Session 连接iotdb数据库
 */
public class PrepareConnection {
    private static ReadConfig config;

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
    public static ITableSession getSession_TableModel() throws IoTDBConnectionException {
        ITableSession session = null;
        // 判断是否是集群
        if (config.getValue("is_cluster").equals("true")) {
            String host_nodes_str = config.getValue("host_nodes");
            session = new TableSessionBuilder()
                    .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                    .username(config.getValue("user"))
//                    .password(config.getValue("password")) TODO：需要确定最终默认密码之后测试
                    .build();
        } else {
            session = new TableSessionBuilder()
                    .nodeUrls(Collections.singletonList(config.getValue("url")))
                    .username(config.getValue("user"))
//                    .password(config.getValue("password")) TODO：需要确定最终默认密码之后测试
                    .build();
        }
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
//                    .password(config.getValue("password")) TODO：需要确定最终默认密码之后测试
                    .build();
        } else {
            session = new Session.Builder()
                    .host(config.getValue("host"))
                    .port(Integer.parseInt(config.getValue("port")))
                    .username(config.getValue("user"))
//                    .password(config.getValue("password")) TODO：需要确定最终默认密码之后测试
                    .build();
        }
        session.open(false);
        // set session fetchSize
        session.setFetchSize(10000);
        return session;
    }

    /**
     * 从sessionPool中获取Session对象（树模型）
     */
    public static SessionPool getSessionPool() {
        SessionPool sessionPool = null;
        if (config.getValue("is_cluster").equals("true")) {
            String host_nodes_str = config.getValue("host_nodes");
            sessionPool = new SessionPool.Builder()
                    .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                    .user(config.getValue("user"))
//                    .password(config.getValue("password")) TODO：需要确定最终默认密码之后测试
                    .build();
        } else {
            sessionPool = new SessionPool.Builder()
                    .host(config.getValue("host"))
                    .port(Integer.parseInt(config.getValue("port")))
                    .user(config.getValue("user"))
//                    .password(config.getValue("password")) TODO：需要确定最终默认密码之后测试
                    .build();
        }
        return sessionPool;
    }

    /**
     * 从sessionPool中获取Session对象（表模型）
     */
    public static ITableSessionPool getSessionPool_TableModel() throws IoTDBConnectionException {
        ITableSessionPool sessionPool = null;
        // 判断是否是集群
        if (config.getValue("is_cluster").equals("true")) {
            String host_nodes_str = config.getValue("host_nodes");
            sessionPool = new TableSessionPoolBuilder()
                    .nodeUrls(Arrays.asList(host_nodes_str.split(",")))
                    .user(config.getValue("user"))
//                    .password(config.getValue("password")) TODO：需要确定最终默认密码之后测试
                    .build();
        } else {
            sessionPool = new TableSessionPoolBuilder()
                    .nodeUrls(Collections.singletonList(config.getValue("url")))
                    .user(config.getValue("user"))
//                    .password(config.getValue("password")) TODO：需要确定最终默认密码之后测试
                    .build();
        }
        return sessionPool;
    }

    public static void main(String[] args) throws IOException, IoTDBConnectionException, StatementExecutionException {
        String ROOT_SG1_D1 = "root.multi.d1";
        String host = "172.20.70.45";
        long timestamp = 601L;

    }
}
