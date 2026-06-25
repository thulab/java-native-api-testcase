package org.apache.iotdb.api.test;

import org.testng.TestNG;
import org.testng.xml.Parser;
import org.testng.xml.XmlSuite;

import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

public class TestRunner {
    private static final String SUITE_RESOURCE = "testng.xml";
    private static final String SUITE_FILE = "details/src/main/resources/testng.xml";

    public static void main(String[] args) throws Exception {
        TestNG testNG = new TestNG();
        // 优先用 details 模块的 testng.xml 文件（DetailSuite，真正的用例集）。
        // 不能优先走 classpath：shade 打包时 classpath 根的 testng.xml 会被 common 模块的同名占位套件
        // (GenerateData，空套件) 覆盖，从而导致 "Total tests run: 0"。
        if (new File(SUITE_FILE).exists()) {
            testNG.setTestSuites(Collections.singletonList(SUITE_FILE));
        } else {
            // 回退：从 classpath 加载（IDE 或非项目根目录运行时）。
            try (InputStream in = TestRunner.class.getClassLoader().getResourceAsStream(SUITE_RESOURCE)) {
                if (in == null) {
                    throw new IllegalStateException("找不到 testng.xml：" + SUITE_FILE + " 与 classpath 均不存在");
                }
                List<XmlSuite> suites = new Parser(in).parseToList();
                testNG.setXmlSuites(suites);
            }
        }
        testNG.run();
    }
}
