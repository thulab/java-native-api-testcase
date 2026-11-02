package org.apache.iotdb.api.test.utils;



import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.lang.System.out;

public class ReadConfig {
//    private static final String configPath = "resources/config.properties";
//    private final String configPath = System.getProperty("user.dir") + "/conf/application.properties";
    private static volatile ReadConfig config;

    private Properties properties = null;

    /*
     * 单例获取实例（加锁双重检查，避免多线程首次访问时创建多个实例或读到半初始化的 properties）
     */
    public static synchronized ReadConfig getInstance() throws IOException {
        if (null == config) {
            config = new ReadConfig();
        }
//        out.println(new File(configPath).getAbsolutePath());
        return config;
    }

    private ReadConfig() throws IOException {
        properties = new Properties();
        // 加载完成后立即关闭流，避免 InputStream 泄漏。
        try (InputStream in = ReadConfig.class.getClassLoader().getResourceAsStream("config.properties")) {
            properties.load(in);
        }
        applyEnvironmentOverrides();
    }

    /**
     * PLM / CI 连接参数覆盖：仅当对应环境变量非空时生效。
     * 不设置任何环境变量时，行为与原先完全一致（全部取自 config.properties）。
     */
    private void applyEnvironmentOverrides() {
        String host = env("IOTDB_HOST");
        String port = env("IOTDB_PORT");
        if (host != null || port != null) {
            String effectiveHost = host != null ? host : properties.getProperty("host");
            String effectivePort = port != null ? port : properties.getProperty("port");
            properties.setProperty("host", effectiveHost);
            properties.setProperty("port", effectivePort);
            properties.setProperty("url", effectiveHost + ":" + effectivePort);
            properties.setProperty("hosts", effectiveHost + ":" + effectivePort);
            properties.setProperty("host_nodes", effectiveHost + ":" + effectivePort);
        }
        String hostNodes = env("IOTDB_HOST_NODES");
        if (hostNodes != null) {
            properties.setProperty("host_nodes", hostNodes);
        }
        String user = env("IOTDB_USER");
        if (user != null) {
            properties.setProperty("user", user);
        }
        String password = env("IOTDB_PASSWORD");
        if (password != null) {
            properties.setProperty("password", password);
        }
        String cluster = env("IOTDB_IS_CLUSTER");
        if (cluster != null) {
            properties.setProperty("is_cluster", cluster);
        }
    }

    private static String env(String key) {
        String value = System.getenv(key);
        if (value == null) {
            return null;
        }
        value = value.trim();
        return value.isEmpty() ? null : value;
    }
    public String getValue(String key) {
        return properties.getProperty(key);
    }
    public void close() throws IOException {
        // 配置流在构造时即已关闭，这里保留空实现以兼容历史调用方。
    }

    public static void main(String[] args) throws IOException {
        out.println(ReadConfig.getInstance().getValue("host"));
    }
}
