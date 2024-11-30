package org.apache.iotdb.api.test.tablemodel.error.data_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Title：测试插入数据—异常情况
 * Describe：基于表模型V1版本，测试各种方式的数据插入的操作
 * Author：肖林捷
 * Date：2024/8/9
 */
public class TestInsert_V2_Error extends BaseTestSuite_TableModel {
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
        session.executeNonQueryStatement("create table table2 (" +
                "device_id string id," +
                "attribute STRING ATTRIBUTE," +
                "boolean boolean MEASUREMENT," +
                "int32 int32 MEASUREMENT," +
                "int64 int64 MEASUREMENT," +
                "float float MEASUREMENT," +
                "double double MEASUREMENT," +
                "text text MEASUREMENT," +
                "string string MEASUREMENT," +
                "blob blob MEASUREMENT," +
                "timestamp01 timestamp MEASUREMENT," +
                "date date MEASUREMENT)");
    }

    /**
     * 获取不正确的数据并解析文档
     */
//    @DataProvider(name = "insertTablet")
    public Iterator<Object[]> getData2() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/insertRelationalTablet_error.csv", false).getData();
    }

    /**
     * 测试使用 insertRelationalTablet 插入数据
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertRelationalTablet() throws IoTDBConnectionException, StatementExecutionException, IOException {
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
        List<Tablet.ColumnCategory> columnCategoryList = new ArrayList<>();
        columnCategoryList.add(Tablet.ColumnCategory.ID);
        columnCategoryList.add(Tablet.ColumnCategory.ATTRIBUTE);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        columnCategoryList.add(Tablet.ColumnCategory.MEASUREMENT);
        // 构造tablet对象
        Tablet tablet = new Tablet("table2", measurementList, dataTypeList, columnCategoryList, 10);
        try {
            int rowIndex = 0;
            // 获取解析后的数据
            for (Iterator<Object[]> it = getData2(); it.hasNext(); ) {
                // 获取每行的SQL语句
                Object[] line = it.next();
                // 添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
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
                    assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "其他错误：" + e;
                }
                rowIndex++;
            }
            // 插入数据
            session.insert(tablet);
        } catch (Exception e) {
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
