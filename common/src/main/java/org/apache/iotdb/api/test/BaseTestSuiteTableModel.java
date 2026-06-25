package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.isession.ITableSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;

/**
 * Title：基础测试工具（表面性）
 * Describe：用于操作表模型 session 连接
 * Author：肖林捷
 * Date：2024/8/9
 */
public class BaseTestSuiteTableModel {
    // 表模型session会话
    public static ITableSession session = null;
    public static ITableSessionPool sessionPool = null;

    /**
     * 获取表模型的session
     */
    @BeforeClass
    public void beforeSuite() throws IoTDBConnectionException, IOException {
        // 判断是否使用sessionPool
        if (ReadConfig.getInstance().getValue("is_sessionPool").equals("false")) {
            session = PrepareConnection.getSessionTableModel();
        } else {
            session = PrepareConnection.getSessionTableModel();
            sessionPool = PrepareConnection.getSessionPoolTableModel();
        }

    }

    /**
     * 关闭表模型的session
     */
    @AfterClass
    public void afterSuite() throws IoTDBConnectionException, IOException {
        // 加空值检查：若 beforeSuite 在建立 session/sessionPool 过程中抛异常，
        // 这里直接 close 会 NPE 并掩盖前置失败的真实原因。
        if (session != null) {
            session.close();
        }
        if (sessionPool != null) {
            sessionPool.close();
        }
    }

}
