package org.apache.iotdb.api.test.table.data_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Title：测试插入数据—异常情况
 * Describe：测试插入的数据和注册的数据类型不一致情况
 * Author：肖林捷
 * Date：2024/12/29
 */
public class TestInsertError extends BaseTestSuite_TableModel {
    /**
     * 创建测试环境
     */
    @BeforeClass
    public void afterTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        try {
            session.executeNonQueryStatement("create database TestInsert");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database TestInsert");
            session.executeNonQueryStatement("create database TestInsert");
        }
        // 使用数据库
        session.executeNonQueryStatement("use TestInsert");
        // 创建表
        session.executeNonQueryStatement("create table insert_error (" +
                "device_id string TAG," +
                "attribute STRING ATTRIBUTE," +
                "boolean boolean FIELD," +
                "int32 int32 FIELD," +
                "int64 int64 FIELD," +
                "float float FIELD," +
                "double double FIELD," +
                "text text FIELD," +
                "string string FIELD," +
                "blob blob FIELD," +
                "timestamp01 timestamp FIELD," +
                "date date FIELD)");
    }

    /**
     * 获取不正确的数据并解析文档
     */
//    @DataProvider(name = "insertTablet")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insert_error.csv", false).getData();
    }

    /**
     * 测试插入数据类型不一致情况
     */
    @Test(priority = 10)
    public void insertError1(){
        // 列名
        List<String> measurementList = new ArrayList<>();
        measurementList.add("device_id");
        measurementList.add("attribute");
        measurementList.add("boolean");
        measurementList.add("int32");
        measurementList.add("int64");
        measurementList.add("FLOAT");
        measurementList.add("double");
        measurementList.add("text");
        measurementList.add("string");
        measurementList.add("blob");
        measurementList.add("timestamp");
        measurementList.add("date");
        // 值类型
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
        dataTypeList.add(TSDataType.TIMESTAMP);
        dataTypeList.add(TSDataType.DATE);
        // 列类型
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
        Tablet tablet = new Tablet("insert_error", measurementList, dataTypeList, columnCategoryList, 10);
        try {
            int rowIndex = 0;
            // 获取解析后的数据
            for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
                // 获取每行的SQL语句
                Object[] line = it.next();
                // 添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 获取每行每列的数据
                try {
                    for (int i = 0; i < tablet.getRowSize(); i++) {
                        // 根据数据类型添加值到tablet
                        switch (dataTypeList.get(i)) {
                            case BOOLEAN:
                                tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                                break;
                            case INT32:
                                tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                                break;
                            case INT64:
                            case TIMESTAMP:
                                tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                                break;
                            case FLOAT:
                                tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                                break;
                            case DOUBLE:
                                tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                                break;
                            case TEXT:
                            case STRING:
                                tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                                break;
                            case BLOB:
                                tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                                break;
                            case DATE:
                                tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                                break;
                        }
                    }
                } catch (Exception e) {
                    assert "java.lang.IllegalArgumentException".equals(e.getClass().getName()) : "其他错误：" + e;
                }
                rowIndex++;
            }
            // 插入数据
            session.insert(tablet);
        } catch (IoTDBConnectionException | IOException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试写入不包含FIELD
     */
    @Test(priority = 20)
    public void insertError2() {
        // 列名
        List<String> measurementList = new ArrayList<>();
        measurementList.add("device_id");
        measurementList.add("attribute");
        // 值类型
        List<TSDataType> dataTypeList = new ArrayList<>();
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        // 列类型
        List<ColumnCategory> columnCategoryList = new ArrayList<>();
        columnCategoryList.add(ColumnCategory.TAG);
        columnCategoryList.add(ColumnCategory.ATTRIBUTE);
        // 构造tablet对象
        Tablet tablet = new Tablet("insert_error2", measurementList, dataTypeList, columnCategoryList, 10);
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            // 添加时间戳
            tablet.addTimestamp(i, timestamp++);
            // 添加值
            tablet.addValue("device_id", i, "device_id" + i);
            tablet.addValue("attribute", i, "attribute" + i);
        }
        // 插入数据并判断报错是否符合预期：org.apache.iotdb.rpc.StatementExecutionException: 507: No Field column present, please check the request
        try {
            session.insert(tablet);
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            assert "org.apache.iotdb.rpc.StatementExecutionException: 507: No Field column present, please check the request".equals(e.toString()) : "其他错误：" + e;
        }
    }

    /**
     * 清空测试环境
     */
    @AfterClass
    public void beforeTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        session.executeNonQueryStatement("drop database TestInsert");
    }
}
