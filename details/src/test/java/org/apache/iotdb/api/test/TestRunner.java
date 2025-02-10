package org.apache.iotdb.api.test;

import org.testng.TestNG;

import java.util.Collections;

public class TestRunner {
    public static void main(String[] args) {
        TestNG testNG = new TestNG();
        testNG.setTestSuites(Collections.singletonList("details/src/main/resources/testng.xml"));
        testNG.run();
    }
}
