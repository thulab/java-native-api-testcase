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
        // 优先从 classpath 加载套件，避免硬编码相对路径导致在 IDE 或非项目根目录运行时找不到文件。
        try (InputStream in = TestRunner.class.getClassLoader().getResourceAsStream(SUITE_RESOURCE)) {
            if (in != null) {
                List<XmlSuite> suites = new Parser(in).parseToList();
                testNG.setXmlSuites(suites);
            } else if (new File(SUITE_FILE).exists()) {
                // 回退：从项目根目录的相对路径加载。
                testNG.setTestSuites(Collections.singletonList(SUITE_FILE));
            } else {
                throw new IllegalStateException("找不到 testng.xml：classpath 与 " + SUITE_FILE + " 均不存在");
            }
        }
        testNG.run();
    }
}
