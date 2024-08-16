package org.apache.iotdb.api.test.tablemodel.normal.table_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Title：测试表操作—正常情况
 * Describe：基于表模型V1版本，测试表所有的操作，包括创建表、查看表、查看表结构
 * Author：肖林捷
 * Date：2024/8/8
 */
public class TestTable_V1_Normal extends BaseTestSuite_TableModel {
    /**
     * 创建测试环境
     */
    @BeforeClass
    public void afterTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        try {
            session.executeNonQueryStatement("create database TestTable");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database TestTable");
            session.executeNonQueryStatement("create database TestTable");
        }
        try {
            session.executeNonQueryStatement("create database otherDataBase");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database otherDataBase");
            session.executeNonQueryStatement("create database otherDataBase");
        }
        // 使用数据库
        session.executeNonQueryStatement("use TestTable");
    }

    /**
     * 获取正确的数据并解析文档
     */
    @DataProvider(name = "table")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/table_normal.csv", true).getData();
    }

    /**
     * 测试创建表
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void createTable() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待表的数量
        int expect = 0;
        // 实际表的数量（先默认未0）
        int actual = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行创建表的SQL语句
            Object[] tableSQLs = it.next();
            for (Object tableSQL : tableSQLs) {
                expect++;
                // 创建表
                session.executeNonQueryStatement((String) tableSQL);
            }
        }
        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("show tables")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestTable_V1_Normal的createTable实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试查看表
     */
    @Test(priority = 20) // 测试执行的优先级为10
    public void showTables() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 方式一
        session.executeNonQueryStatement("SHOW TABLES");
        session.executeNonQueryStatement("SHOW TABLES in TestTable");
        session.executeNonQueryStatement("SHOW TABLES in otherDataBase");
        // 方式二
        ArrayList<String> showTableNames = new ArrayList<>();
        ArrayList<String> createTableNames = new ArrayList<>();
        try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
            while (dataSet.hasNext()) {
//                System.out.println(dataSet.next().getFields().get(0));
                showTableNames.add(String.valueOf(dataSet.next().getFields().get(0)));
            }
        }
//        System.out.println("**********************");
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行创建表的SQL语句
            Object[] tableSQLs = it.next();
            // 获取每行每列的数据
            for (Object tableSQL : tableSQLs) {
                // 正则表达式，匹配"table"之后直到左括号的部分的表名
                Matcher matcher = Pattern.compile("create table (.*?)\\s*\\(", Pattern.CASE_INSENSITIVE).matcher(tableSQL.toString());
                assert matcher.find() : "未找到表名";
                String tableName = matcher.group(1).replaceAll("\\s+", "").toLowerCase();
                if (tableName.contains("testtable.")) {
                    tableName = tableName.substring("testtable.".length());
                }
//                System.out.println(tableName);
                createTableNames.add(tableName);
            }
        }
        // 比较是否符合预期
        int i = 0;
        int j = 0;
        while (true) {
            // 判断是否有相等的
            if (showTableNames.get(i).equals(createTableNames.get(j))) {
                // 若相等则下一个createTableNames
                j++;
                // 重新开始
                i = 0;
                // 若测完了则退出
                if (j == createTableNames.size()) {
                    break;
                }
            } else {
                // 若不相等则下一个showTableNames
                i++;
            }
            // 若showTableNames没有一个和期望一致，则断言
            assert i != showTableNames.size() : " TestTable_V1_Normal 的 showTables 实际表名与期望的不一致，期望的" + createTableNames.get(j) + "不存在";
        }
    }

    /**
     * 测试查看表结构
     */
    @Test(priority = 30) // 测试执行的优先级为10
    public void describe() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待表结构数量
        int expect = 171;
        // 实际表结构数量（先默认未0）
        int actual = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行创建表的SQL语句
            Object[] tableSQLs = it.next();
            // 获取每行每列的数据
            for (Object tableSQL : tableSQLs) {
                // 正则表达式，匹配"table"之后直到左括号的部分的表名
                Matcher matcher = Pattern.compile("create table (.*?)\\s*\\(", Pattern.CASE_INSENSITIVE).matcher(tableSQL.toString());
                assert matcher.find() : "未找到表名";
                String tableName = matcher.group(1).replaceAll("\\s+", "");
                // 查看表结构
                session.executeNonQueryStatement("describe " + tableName);
                session.executeNonQueryStatement("desc " + tableName);
                // 计算实际表结构数量
                try (SessionDataSet dataSet = session.executeQueryStatement("describe " + tableName)) {
                    while (dataSet.hasNext()) {
                        dataSet.next();
                        actual++;
                    }
                }
            }
        }
        // 比较是否符合预期
//        assert expect == actual : "TestTable_V1_Normal的describe实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 清空测试环境
     */
    @AfterClass
    public void beforeTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        session.executeNonQueryStatement("drop database TestTable");
        session.executeNonQueryStatement("drop database otherDataBase");
    }
}
