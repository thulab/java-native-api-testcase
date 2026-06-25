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
