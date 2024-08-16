package org.apache.iotdb.api.test.tablemodel.normal.database_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Title：测试数据库操作-正常情况
 * Describe：基于表模型V1版本，测试数据库所有的操作，包括创建数据库、查看数据库、使用数据库和删除数据库的正常情况
 * Author：肖林捷
 * Date：2024/8/8
 */
public class TestDataBase_V1_Normal extends BaseTestSuite_TableModel {

    /**
     * 创建测试环境
     */
    @BeforeClass
    public void afterTest() throws IoTDBConnectionException, StatementExecutionException {
        // 判断是否存在数据库
        try (SessionDataSet dataSet = session.executeQueryStatement("show databases")) {
            while (dataSet.hasNext()) {
                // 删除数据库
                session.executeNonQueryStatement("drop database " + dataSet.next().getFields().get(0));
            }
        }
    }

    /**
     * 获取正确的数据并解析文档
     */
    @DataProvider(name = "database")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/database_normal.csv", false).getData();
    }

    /**
     * 测试创建数据库
     */
    @Test(priority = 10)
    public void createDataBase() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] dataBaseIds = it.next();
            for (Object dataBaseId : dataBaseIds) {
                expect++;
                // 创建数据库
                session.executeNonQueryStatement("create database " + dataBaseId);
            }
        }
        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("show databases")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestDataBase_V1_Normal的createDataBase实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试查看数据库
     */
    @Test(priority = 20)
    public void showDataBase() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] dataBaseIds = it.next();
            // 计算期待表的数量
            for (int i = 0; i < dataBaseIds.length; i++) {
                expect++;
            }
        }
        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("show databases")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestDataBase_V1_Normal的showDataBase实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用数据库
     */
    @Test(priority = 30)
    public void useDataBase() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] dataBaseIds = it.next();
            // 获取每行每列的数据
            for (Object dataBaseId : dataBaseIds) {
                // 使用数据库
                session.executeNonQueryStatement("use " + dataBaseId);
            }
        }
    }


    /**
     * 测试删除数据库
     */
    @Test(priority = 40)
    public void dropDataBase() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] dataBaseIds = it.next();
            // 获取每行每列的数据
            for (Object dataBaseId : dataBaseIds) {
                // 删除数据库
                session.executeNonQueryStatement("drop database " + dataBaseId);
            }
        }
        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("show databases")) {
            // 判断下一个是否有数据
            while (dataSet.hasNext()) {
                // 切换到下一个
                dataSet.next();
                // 增加实际数量
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestDataBase_V1_Normal的dropDataBase实际不一致期待：" + expect + "，实际：" + actual;
    }

}
