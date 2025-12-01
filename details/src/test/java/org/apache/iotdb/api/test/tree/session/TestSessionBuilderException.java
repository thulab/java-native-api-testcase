package org.apache.iotdb.api.test.tree.session;


import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.session.Session;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;


/**
 * <p>Title：SessionBuilder接口异常情况的功能测试<p/>
 * <p>Describe：测试非法参数构建Session连接是否报错<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2025/11<p/>
 */
public class TestSessionBuilderException {
    // 读取配置文件
    private static ReadConfig config;

    // 在测试类之前读取配置文件
    @BeforeClass
    public void beforeClass() throws IOException {
        config = ReadConfig.getInstance();
    }

    /**
     * 测试同时使用(host + rpcPort）和nodeUrls构建Session的异常情况
     */
    @Test(priority = 10)
    public void testSessionBuilderParamException1() {
        try {
            Session session = new Session.Builder()
                    .host(config.getValue("host"))
                    .port(Integer.parseInt(config.getValue("port")))
                    .nodeUrls(Collections.singletonList(config.getValue("nodeUrl")))
                    .build();
            session.open(false);
            session.close();
            assert false : "期待效果和实际效果不一致，期待：执行报错，实际：未执行报错";
        } catch (Exception e) {
            assert e.getMessage().contains("You should specify either nodeUrls or (host + rpcPort), but not both") : "期待值和实际不一致，期待：You should specify either nodeUrls or (host + rpcPort), but not both，实际：" + e.getMessage();
        }
    }
    /**
     * 测试nodeUrls为空集合构建Session的异常情况
     */
    @Test(priority = 20)
    public void testSessionBuilderParamException2() {
        try {
            Session session = new Session.Builder()
                    .nodeUrls(Collections.emptyList())
                    .build();
            session.open(false);
            session.close();
            assert false : "期待效果和实际效果不一致，期待：执行报错，实际：未执行报错";
        } catch (Exception e) {
            assert e.getMessage().contains("nodeUrls shouldn't be empty.") : "期待值和实际不一致，期待：nodeUrls shouldn't be empty.，实际：" + e.getMessage();
        }
    }


}
