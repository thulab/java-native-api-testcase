package org.apache.iotdb.api.test.table.data_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Title：测试数据查询—正常情况
 * Describe：测试各种方式的数据查询的操作
 * Author：肖林捷
 * Date：2024/12/29
 */
public class TestSelectNormal extends BaseTestSuite_TableModel {
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
        session.executeNonQueryStatement("create table t1 (" +
                "    TAG1 string tag," +
                "    attr1 string ATTRIBUTE," +
                "    s1 BOOLEAN FIELD," +
                "    s2 INT32 FIELD," +
                "    s3 INT64 FIELD," +
                "    s4 FLOAT FIELD," +
                "    s5 DOUBLE FIELD," +
                "    s6 TEXT FIELD," +
                "    s7 STRING FIELD," +
                "    S8 BLOB FIELD," +
                "    s9 DATE FIELD," +
                "    s10 TIMESTAMP FIELD" +
                ")");
        session.executeNonQueryStatement("create table t2 (" +
                "    TAG1 string tag," +
                "    attr1 string ATTRIBUTE," +
                "    s1 BOOLEAN FIELD," +
                "    s2 INT32 FIELD," +
                "    s3 INT64 FIELD," +
                "    s4 FLOAT FIELD," +
                "    s5 DOUBLE FIELD," +
                "    s6 TEXT FIELD," +
                "    s7 STRING FIELD," +
                "    S8 BLOB FIELD," +
                "    s9 DATE FIELD," +
                "    s10 TIMESTAMP FIELD" +
                ")");
        // 插入数据
        List<String> measurementList = new ArrayList<>();
        measurementList.add("TAG1");
        measurementList.add("attr1");
        measurementList.add("s1");
        measurementList.add("s2");
        measurementList.add("s3");
        measurementList.add("s4");
        measurementList.add("s5");
        measurementList.add("s6");
        measurementList.add("s7");
        measurementList.add("S8");
        measurementList.add("s9");
        measurementList.add("s10");
        List<TSDataType> dataTypeList = new ArrayList<>();
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.BOOLEAN);
        dataTypeList.add(TSDataType.INT32);
        dataTypeList.add(TSDataType.INT64);
        dataTypeList.add(TSDataType.FLOAT);
        dataTypeList.add(TSDataType.DOUBLE);
        dataTypeList.add(TSDataType.TEXT);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.BLOB);
        dataTypeList.add(TSDataType.DATE);
        dataTypeList.add(TSDataType.TIMESTAMP);
        List<ColumnCategory> columnCategoryList = new ArrayList<>();
        columnCategoryList.add(ColumnCategory.TAG);
        columnCategoryList.add(ColumnCategory.ATTRIBUTE);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        columnCategoryList.add(ColumnCategory.FIELD);
        // 构造tablet对象
        Tablet tablet = new Tablet("t1", measurementList, dataTypeList, columnCategoryList, 100);
        int time = 0;
        // 插入有效值
        for (int row = 0; row < 10; row++) {
            tablet.addTimestamp(row, time++);
            if (row % 2 == 0) {
                tablet.addValue(measurementList.get(0), row, "tag1");
                tablet.addValue(measurementList.get(1), row, "attr1");
                tablet.addValue(measurementList.get(2), row, true);
                tablet.addValue(measurementList.get(3), row, 1);
                tablet.addValue(measurementList.get(4), row, 1L);
                tablet.addValue(measurementList.get(5), row, 1.1F);
                tablet.addValue(measurementList.get(6), row, 1.1);
                tablet.addValue(measurementList.get(7), row, "text");
                tablet.addValue(measurementList.get(8), row, "string");
                tablet.addValue(measurementList.get(9), row, new Binary("blob", Charset.defaultCharset()));
                tablet.addValue(measurementList.get(10), row, LocalDate.of(2000, 1, 1));
                tablet.addValue(measurementList.get(11), row, 1L);
            }
        }
        session.insert(tablet);
    }

    /**
     * 获取正确的数据并解析文档
     */
    @DataProvider(name = "select")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/table/select_normal.csv", true).getData();
    }

    /**
     * 测试使用DataIterator获取查询数据
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void test1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        SessionDataSet dataSet = session.executeQueryStatement("select tag1,attr1,s1,s2,s3,s4,s5,s6,s7,S8,s9,s10 from t1");
        List<String> columnNameList = Arrays.asList("tag1", "attr1", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "S8", "s9", "s10");
        List<String> columnTypeList = Arrays.asList("STRING", "STRING", "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT", "STRING", "BLOB", "DATE", "TIMESTAMP");
        SessionDataSet.DataIterator dataIterator = dataSet.new DataIterator();
        for (int i = 0; i < columnNameList.size(); i++) {
//            assert columnNameList[i]
        }
        assert dataIterator.getColumnNameList().size() == 12;
        assert dataIterator.getColumnTypeList().size() == 12;
        while (dataIterator.next()) {
            assert dataIterator.getString("tag1").equals(dataIterator.getString(1));
            assert dataIterator.getString("attr1").equals(dataIterator.getString(2));
            assert dataIterator.getBoolean("s1") == dataIterator.getBoolean(3);
            dataIterator.getInt("s2");
            dataIterator.getLong("s3");
            dataIterator.getFloat("s4");
            dataIterator.getDouble("s5");
            dataIterator.getString("s6");
            dataIterator.getString("s7");
            dataIterator.getBlob("S8");
            dataIterator.getDate("s9");
            dataIterator.getTimestamp("s10");
            dataIterator.getObject("tag1");
            dataIterator.getObject("attr1");
            dataIterator.getObject("s1");
            dataIterator.getObject("s2");
            dataIterator.getObject("s3");
            dataIterator.getObject("s4");
            dataIterator.getObject("s5");
            dataIterator.getObject("s6");
            dataIterator.getObject("s7");
            dataIterator.getObject("S8");
            dataIterator.getObject("s9");
            dataIterator.getObject("s10");
            //
            dataIterator.getInt(4);
            dataIterator.getLong(5);
            dataIterator.getFloat(6);
            dataIterator.getDouble(7);
            dataIterator.getString(8);
            dataIterator.getString(9);
            dataIterator.getBlob(10);
            dataIterator.getDate(11);
            dataIterator.getTimestamp(12);
            dataIterator.getObject(1);
            dataIterator.getObject(2);
            dataIterator.getObject(3);
            dataIterator.getObject(4);
            dataIterator.getObject(5);
            dataIterator.getObject(6);
            dataIterator.getObject(7);
            dataIterator.getObject(8);
            dataIterator.getObject(9);
            dataIterator.getObject(10);
            dataIterator.getObject(11);
            dataIterator.getObject(12);
            // 其他方法
            dataIterator.findColumn("s1");
            dataIterator.isNull(1);
            dataIterator.isNull(2);
            dataIterator.isNull(3);
            dataIterator.isNull(4);
            dataIterator.isNull(5);
            dataIterator.isNull(6);
            dataIterator.isNull(7);
            dataIterator.isNull(8);
            dataIterator.isNull(9);
            dataIterator.isNull(10);
            dataIterator.isNull(11);
            dataIterator.isNull(12);
            dataIterator.isNull("tag1");
            dataIterator.isNull("attr1");
            dataIterator.isNull("s1");
            dataIterator.isNull("s2");
            dataIterator.isNull("s3");
            dataIterator.isNull("s4");
            dataIterator.isNull("s5");
            dataIterator.isNull("s6");
            dataIterator.isNull("s7");
            dataIterator.isNull("S8");
            dataIterator.isNull("s9");
            dataIterator.isNull("s10");
            // 其他类型都可以使用getString
            dataIterator.getString(1);
            dataIterator.getString(2);
            dataIterator.getString(3);
            dataIterator.getString(4);
            dataIterator.getString(5);
            dataIterator.getString(6);
            dataIterator.getString(7);
            dataIterator.getString(8);
            dataIterator.getString(9);
            dataIterator.getString(10);
            dataIterator.getString(11);
            dataIterator.getString(12);
            dataIterator.getString("tag1");
            dataIterator.getString("attr1");
            dataIterator.getString("s1");
            dataIterator.getString("s2");
            dataIterator.getString("s3");
            dataIterator.getString("s4");
            dataIterator.getString("s5");
            dataIterator.getString("s6");
            dataIterator.getString("s7");
            dataIterator.getString("S8");
            dataIterator.getString("s9");
            dataIterator.getString("s10");
            // 错误情况
            try {
                dataIterator.getString("None");
            }catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * 测试使用Field获取查询数据
     */
//    @Test(priority = 20) // 测试执行的优先级为10
    public void test2() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 查询数据
        SessionDataSet dataSet = session.executeQueryStatement("select * from t1");
        System.out.println(dataSet.getColumnNames());
        System.out.println(dataSet.getColumnTypes());
    }

    /**
     * 测试执行各种查询SQL
     */
    @Test(priority = 100) // 测试执行的优先级为10
    public void test3() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行的SQL语句
            Object[] selectSQLs = it.next();
            // 获取每行每列的数据
            for (Object selectSQL : selectSQLs) {
                // 查询数据
                SessionDataSet dataSet = session.executeQueryStatement((String) selectSQL);
                while (dataSet.hasNext()) {
                    dataSet.next();
                }
                dataSet.closeOperationHandle();
                dataSet.close();
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
