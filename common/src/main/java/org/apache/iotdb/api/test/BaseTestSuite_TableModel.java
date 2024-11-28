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
public class BaseTestSuite_TableModel {
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
            // 获取表模型的session
            session = PrepareConnection.getSession_TableModel();
        } else {
            // 从sessionPool中获取
            sessionPool = PrepareConnection.getSessionPool_TableModel();
            session = sessionPool.getSession();
        }

    }

    /**
     * 关闭表模型的session
     */
    @AfterClass
    public void afterSuite() throws IoTDBConnectionException, IOException {
        // 判断是否使用sessionPool
        if (ReadConfig.getInstance().getValue("is_sessionPool").equals("false")) {
            // 关闭表模型的session
            session.close();
        } else {
            // 关闭表模型的session
            sessionPool = PrepareConnection.getSessionPool_TableModel();
            session = sessionPool.getSession();
            session.close();
            sessionPool.close();
        }

    }

}
