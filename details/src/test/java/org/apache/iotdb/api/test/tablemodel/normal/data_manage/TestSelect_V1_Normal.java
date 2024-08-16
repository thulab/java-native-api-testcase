package org.apache.iotdb.api.test.tablemodel.normal.data_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Title：测试数据查询—正常情况
 * Describe：基于表模型V1版本，测试各种方式的数据查询的操作
 * Author：肖林捷
 * Date：2024/8/12
 */
public class TestSelect_V1_Normal extends BaseTestSuite_TableModel {
    /**
     * 创建测试环境
     */
    @BeforeClass
    public void afterTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        try {
            session.executeNonQueryStatement("create database TestSelect");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database TestSelect");
            session.executeNonQueryStatement("create database TestSelect");
        }
        // 使用数据库
        session.executeNonQueryStatement("use TestSelect");
        // 创建表
        session.executeNonQueryStatement("create table table1 (device_id string id, ATTRIBUTE STRING ATTRIBUTE, string string MEASUREMENT, text text MEASUREMENT, DOUBLE DOUBLE MEASUREMENT, FLOAT FLOAT MEASUREMENT, INT64 INT64 MEASUREMENT, blob blob MEASUREMENT, BOOLEAN BOOLEAN MEASUREMENT, INT32 INT32 MEASUREMENT)");
        // 插入数据
        session.executeNonQueryStatement("insert into table1 (device_id, ATTRIBUTE, string, text,  DOUBLE, FLOAT, INT64, blob, BOOLEAN, int32) values('1', '1', 'string', 'text',  111.111, 111.111, 10, x'696F746462', true, 10)");
        session.executeNonQueryStatement("insert into table1 (device_id, ATTRIBUTE, string, text,  DOUBLE, FLOAT, INT64, blob, BOOLEAN, int32) values('11', '1', 'string', 'text',  666.666, 666.666, 10, x'696F746462', true, 10)");
        session.executeNonQueryStatement("insert into TestSelect.table1 (time, device_id, ATTRIBUTE, string, text,  DOUBLE, FLOAT, INT64, blob, BOOLEAN, int32) values(1, '2', '1', 'qwertyuiopasdfghjklzxcvbnm', 'qwertyuiopasdfghjklzxcvbnm',  1.0111, 1.0111, 10, x'696F746462', true, 10)");
        session.executeNonQueryStatement("insert into table1 (time, device_id, ATTRIBUTE, string, text,  DOUBLE, FLOAT, INT64, blob, BOOLEAN, int32) values(2, '3', '2', '1234567890', '1234567890',  -111.111, -111.111, -10, x'696F746462', false, -10)");
        session.executeNonQueryStatement("insert into TestSelect.table1 (time, device_id, ATTRIBUTE, string, text,  DOUBLE, FLOAT, INT64, blob, BOOLEAN, int32) values(3, '4', '3', '!@#$%^&*()_+`~！@#￥%……&*（）——+{}|', '!@#$%^&*()_+`~！@#￥%……&*（）——+{}|',  22222.7776931348623157, 11111.7776931348623157, 9223372036854775807, x'696F746462', TRUE, 2147483647)");
        session.executeNonQueryStatement("insert into table1 (time, device_id, ATTRIBUTE, string, text,  DOUBLE, FLOAT, INT64, blob, BOOLEAN, int32) values(4, '5', '4', '没问题的', '没问题的',  -22222.7976931348623157, -11111.7976931348623157, -9223372036854775807, x'696F746462', True, -2147483647)");
        session.executeNonQueryStatement("insert into TestSelect.table1 (time, device_id, ATTRIBUTE, string, text,  DOUBLE, FLOAT, INT64, blob, BOOLEAN, int32) values(6, '6', '0', '', '',  0, 0, 0, x'', FALSE, 0)");
        session.executeNonQueryStatement("insert into TestSelect.table1 (time, device_id, ATTRIBUTE) values(7, '7', '0')");
    }

    /**
     * 获取正确的数据并解析文档
     */
    @DataProvider(name = "select")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/select_normal.csv", true).getData();
    }


    /**
     * 测试查询数据
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void select() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行的SQL语句
            Object[] selectSQLs = it.next();
            // 获取每行每列的数据
            for (Object selectSQL : selectSQLs) {
                // 查询数据
                session.executeNonQueryStatement((String) selectSQL);
            }
        }
    }

    /**
     * 清空测试环境
     */
    @AfterClass
    public void beforeTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        session.executeNonQueryStatement("drop database TestSelect");
    }
}
