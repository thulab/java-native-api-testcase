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
        // 准备列
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("device_id", TSDataType.STRING));
        schemas.add(new MeasurementSchema("attribute", TSDataType.STRING));
        schemas.add(new MeasurementSchema("boolean", TSDataType.BOOLEAN));
        schemas.add(new MeasurementSchema("int32", TSDataType.INT32));
        schemas.add(new MeasurementSchema("int64", TSDataType.INT64));
        schemas.add(new MeasurementSchema("float", TSDataType.FLOAT));
        schemas.add(new MeasurementSchema("double", TSDataType.DOUBLE));
        schemas.add(new MeasurementSchema("text", TSDataType.TEXT));
        schemas.add(new MeasurementSchema("string", TSDataType.STRING));
        schemas.add(new MeasurementSchema("blob", TSDataType.BLOB));
        schemas.add(new MeasurementSchema("timestamp01", TSDataType.TIMESTAMP));
        schemas.add(new MeasurementSchema("date", TSDataType.DATE));
        // 准备列类型
        List<Tablet.ColumnType> columnTypes = Arrays.asList(
                Tablet.ColumnType.ID,
                Tablet.ColumnType.ATTRIBUTE,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT);
        // 构造tablet对象
        Tablet tablet = new Tablet("table2", schemas, columnTypes, 10);
        try {
            // 获取解析后的数据
            for (Iterator<Object[]> it = getData2(); it.hasNext(); ) {
                // 获取每行的SQL语句
                Object[] line = it.next();
                // 实例化有效行并切换行索引
                int rowIndex = tablet.rowSize++;
                // 添加时间戳
                tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
                // 获取每行每列的数据
                try {
                    for (int i = 0; i < schemas.size(); i++) {
                        // 根据数据类型添加值到tablet
                        switch (schemas.get(i).getType()) {
                            case BOOLEAN:
                                tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                                break;
                            case INT32:
                                tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                                break;
                            case INT64:
                            case TIMESTAMP:
                                tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                                break;
                            case FLOAT:
                                tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                                break;
                            case DOUBLE:
                                tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                                break;
                            case TEXT:
                            case STRING:
                                tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                                break;
                            case BLOB:
                                tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                                break;
                            case DATE:
                                tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex, line[i + 1]);
                                break;
                        }
                    }
                } catch (Exception e) {
                    assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "其他错误：" + e;
                }
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
