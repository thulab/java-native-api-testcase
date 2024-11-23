package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.isession.ITableSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.log4testng.Logger;


/**
 * Title：基础测试工具（表面性）
 * Describe：用于操作表模型 session 连接
 * Author：肖林捷
 * Date：2024/8/9
 */
public class BaseTestSuite_TableModel {
    // 日志记录器，用于记录日志信息
    public Logger logger = Logger.getLogger(BaseTestSuite_TableModel.class);
    // 表模型session会话
    public static ITableSession session = null;

    /**
     * 获取表模型的session
     */
    @BeforeClass
    public void beforeSuite() throws IoTDBConnectionException {
        // 获取表模型的session
        session = PrepareConnection.getSession_TableModel();
    }

    @AfterClass
    public void afterSuite() throws IoTDBConnectionException {
        // 关闭表模型的session
        session.close();
    }

}
