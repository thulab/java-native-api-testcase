package org.apache.iotdb.api.test.tablemodel.normal.data_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Title：测试插入数据—正常情况
 * Describe：基于表模型V1版本，测试各种方式的数据插入的操作，以及自动创建情况
 * Author：肖林捷
 * Date：2024/8/9
 */
public class TestInsert_V2_Normal extends BaseTestSuite_TableModel {
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
        session.executeNonQueryStatement("create table table1 (" +
                "device_id string TAG, " +
                "ATTRIBUTE STRING ATTRIBUTE, " +
                "string string FIELD, " +
                "text text FIELD," +
                " DOUBLE DOUBLE FIELD," +
                " FLOAT FLOAT FIELD, " +
                "INT64 INT64 FIELD, " +
                "blob blob FIELD, " +
                "BOOLEAN BOOLEAN FIELD, " +
                "INT32 INT32 FIELD," +
                "timestamp timestamp FIELD," +
                "date date FIELD)");
        session.executeNonQueryStatement("create table \"table\" (" +
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
                "timestamp timestamp FIELD," +
                "date date FIELD)");
        session.executeNonQueryStatement("create table Table2 (" +
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
                "timestamp timestamp FIELD," +
                "date date FIELD)");
        session.executeNonQueryStatement("create table autoCreateColumn (" +
                "device_id string TAG," +
                "attribute STRING ATTRIBUTE," +
                "boolean boolean FIELD," +
                "DOUBLE DOUBLE FIELD," +
                "text text FIELD," +
                "string string FIELD," +
                "blob blob FIELD," +
                "timestamp timestamp FIELD," +
                "date date FIELD)");
    }

    /**
     * 获取正确的数据并解析文档
     */
//    @DataProvider(name = "insert")
    public Iterator<Object[]> getData1() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/insert_normal.csv", true).getData();
    }

    /**
     * 获取正确的数据并解析文档
     */
//    @DataProvider(name = "insertTablet")
    public Iterator<Object[]> getData2() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/insertRelationalTablet_normal.csv", false).getData();
    }

    /**
     * 获取正确的数据并解析文档
     */
//    @DataProvider(name = "insertTablet")
    public Iterator<Object[]> getData3() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/insertRelationalTablet_auto_normal.csv", false).getData();
    }


    /**
     * 测试使用SQL语句插入数据
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertSQL() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData1(); it.hasNext(); ) {
            // 获取每行的SQL语句
            Object[] insertSQLs = it.next();
            // 获取每行每列的数据
            for (Object insertSQL : insertSQLs) {
                expect++;
                // 插入数据
                session.executeNonQueryStatement((String) insertSQL);
            }
        }
//         计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from table1")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from \"table\"")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
//         判断是否符合预期
        assert expect == actual : "TestInsert_V1_Normal 的 insert 实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用 insert 插入数据（无空值）
     */
    @Test(priority = 20)
    public void insert_NoNull() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
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
        columnCategoryList.add(Tablet.ColumnCategory.TAG);
        columnCategoryList.add(Tablet.ColumnCategory.ATTRIBUTE);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        // 构造tablet对象
        Tablet tablet = new Tablet("Table2", measurementList, dataTypeList, columnCategoryList, 10);
        
        int rowIndex = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData2(); it.hasNext(); ) {
            expect++;
            // 获取每行的SQL语句
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取每行每列的数据
            for (int i = 0; i < tablet.getRowSize(); i++) {
                // 根据数据类型添加值到tablet
                switch (dataTypeList.get(i)) {
                    case BOOLEAN:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-08-15") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from table2")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断数量是否符合预期
        assert expect == actual : "TestInsert_V1_Normal 的 insert 实际不一致期待：" + expect + "，实际：" + actual;
    }
    
    /**
     * 测试使用 insert 插入数据——自动创建元数据
     */
    @Test(priority = 30)
    public void insert_autoTable() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
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
        columnCategoryList.add(Tablet.ColumnCategory.TAG);
        columnCategoryList.add(Tablet.ColumnCategory.ATTRIBUTE);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        // 构造tablet对象
        Tablet tablet = new Tablet("autoTable", measurementList, dataTypeList, columnCategoryList, 10);
        int rowIndex = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData2(); it.hasNext(); ) {
            expect++;
            // 获取每行的SQL语句
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取每行每列的数据
            for (int i = 0; i < tablet.getRowSize(); i++) {
                // 根据数据类型添加值到tablet
                switch (dataTypeList.get(i)) {
                    case BOOLEAN:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-08-15") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from autoTable")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsert_V1_Normal 的 insert_autoTable 实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用 insert 插入数据——自动创建标识、属性和测点列
     */
    @Test(priority = 40)
    public void insert_autoCreateColumn() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
        // 列名
        List<String> measurementList = new ArrayList<>();
        measurementList.add("device_id1");
        measurementList.add("device_id2");
        measurementList.add("attribute1");
        measurementList.add("attribute2");
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
        columnCategoryList.add(Tablet.ColumnCategory.TAG);
        columnCategoryList.add(Tablet.ColumnCategory.TAG);
        columnCategoryList.add(Tablet.ColumnCategory.ATTRIBUTE);
        columnCategoryList.add(Tablet.ColumnCategory.ATTRIBUTE);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        columnCategoryList.add(Tablet.ColumnCategory.FIELD);
        // 构造tablet对象
        Tablet tablet = new Tablet("autoCreateColumn", measurementList, dataTypeList, columnCategoryList,10);
        
        int rowIndex = 0;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData3(); it.hasNext(); ) {
            expect++;
            // 获取每行的SQL语句
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取每行每列的数据
            for (int i = 0; i < tablet.getRowSize(); i++) {
                // 根据数据类型添加值到tablet
                switch (dataTypeList.get(i)) {
                    case BOOLEAN:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-08-15") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from autoCreateColumn")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsert_V1_Normal 的 insert_autoTable 实际不一致期待：" + expect + "，实际：" + actual;
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
