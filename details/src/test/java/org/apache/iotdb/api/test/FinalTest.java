package org.apache.iotdb.api.test;

import org.testng.annotations.Test;

public class FinalTest {

    @Test
    public void test() {
        System.out.println();
        System.out.println("|---------- 测试完毕 ----------|");
        try {
            // 暂停线程 60 秒
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            System.out.println("暂停失败！");
            e.printStackTrace();
        }
    }

}
