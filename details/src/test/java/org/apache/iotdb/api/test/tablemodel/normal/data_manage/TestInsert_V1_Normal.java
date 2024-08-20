package org.apache.iotdb.api.test.tablemodel.normal.data_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
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
public class TestInsert_V1_Normal extends BaseTestSuite_TableModel {
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
                "device_id string id, " +
                "ATTRIBUTE STRING ATTRIBUTE, " +
                "string string MEASUREMENT, " +
                "text text MEASUREMENT," +
                " DOUBLE DOUBLE MEASUREMENT," +
                " FLOAT FLOAT MEASUREMENT, " +
                "INT64 INT64 MEASUREMENT, " +
                "blob blob MEASUREMENT, " +
                "BOOLEAN BOOLEAN MEASUREMENT, " +
                "INT32 INT32 MEASUREMENT," +
                "timestamp timestamp MEASUREMENT," +
                "date date MEASUREMENT)");
        session.executeNonQueryStatement("create table \"table\" (" +
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
                "timestamp timestamp MEASUREMENT," +
                "date date MEASUREMENT)");
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
                "timestamp timestamp MEASUREMENT," +
                "date date MEASUREMENT)");
        session.executeNonQueryStatement("create table autocolumn (" +
                "device_id string id," +
                "attribute STRING ATTRIBUTE," +
                "boolean boolean MEASUREMENT," +
                "DOUBLE DOUBLE MEASUREMENT," +
                "text text MEASUREMENT," +
                "string string MEASUREMENT," +
                "blob blob MEASUREMENT," +
                "timestamp timestamp MEASUREMENT," +
                "date date MEASUREMENT)");
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
     * 测试使用 insertRelationalTablet 插入数据
     */
    @Test(priority = 20)
    public void insertRelationalTablet() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
        // 准备列
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("device_id", TSDataType.STRING));
        schemas.add(new MeasurementSchema("attribute", TSDataType.STRING));
        schemas.add(new MeasurementSchema("boolean", TSDataType.BOOLEAN));
        schemas.add(new MeasurementSchema("int32", TSDataType.INT32));
        schemas.add(new MeasurementSchema("int64", TSDataType.INT64));
        schemas.add(new MeasurementSchema("FLOAT", TSDataType.FLOAT));
        schemas.add(new MeasurementSchema("double", TSDataType.DOUBLE));
        schemas.add(new MeasurementSchema("text", TSDataType.TEXT));
        schemas.add(new MeasurementSchema("string", TSDataType.STRING));
        schemas.add(new MeasurementSchema("blob", TSDataType.BLOB));
        schemas.add(new MeasurementSchema("timestamp", TSDataType.TIMESTAMP));
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
        Tablet tablet = new Tablet("table2", schemas, columnTypes,10);
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData2(); it.hasNext(); ) {
            expect++;
            // 获取每行的SQL语句
            Object[] line = it.next();
            // 实例化有效行并切换行索引
            int rowIndex = tablet.rowSize++;
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取每行每列的数据
            for (int i = 0; i < schemas.size(); i++) {
                // 根据数据类型添加值到tablet
                switch (schemas.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-08-15") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
        }
        // 插入数据
        session.insertRelationalTablet(tablet, true);

        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from table2")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断数量是否符合预期
        assert expect == actual : "TestInsert_V1_Normal 的 insertRelationalTablet 实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用 insertRelationalTablet 插入数据——自动创建元数据
     */
    @Test(priority = 30)
    public void insertRelationalTablet_autoTable() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
        // 准备列
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("device_id", TSDataType.STRING));
        schemas.add(new MeasurementSchema("attribute", TSDataType.STRING));
        schemas.add(new MeasurementSchema("boolean", TSDataType.BOOLEAN));
        schemas.add(new MeasurementSchema("int32", TSDataType.INT32));
        schemas.add(new MeasurementSchema("int64", TSDataType.INT64));
        schemas.add(new MeasurementSchema("FLOAT", TSDataType.FLOAT));
        schemas.add(new MeasurementSchema("double", TSDataType.DOUBLE));
        schemas.add(new MeasurementSchema("text", TSDataType.TEXT));
        schemas.add(new MeasurementSchema("string", TSDataType.STRING));
        schemas.add(new MeasurementSchema("blob", TSDataType.BLOB));
        schemas.add(new MeasurementSchema("timestamp", TSDataType.TIMESTAMP));
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
        Tablet tablet = new Tablet("autotable", schemas, columnTypes,10);
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData2(); it.hasNext(); ) {
            expect++;
            // 获取每行的SQL语句
            Object[] line = it.next();
            // 实例化有效行并切换行索引
            int rowIndex = tablet.rowSize++;
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取每行每列的数据
            for (int i = 0; i < schemas.size(); i++) {
                // 根据数据类型添加值到tablet
                switch (schemas.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-08-15") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
        }
        // 插入数据
        session.insertRelationalTablet(tablet, true);

        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from autoTable")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsert_V1_Normal 的 insertRelationalTablet_autoTable 实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用 insertRelationalTablet 插入数据——自动创建标识、属性和测点列
     */
    @Test(priority = 40)
    public void insertRelationalTablet_autoColumn() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据库的数量
        int expect = 0;
        // 实际数据库的数量（先默认未0）
        int actual = 0;
        // 准备列
        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("device_id", TSDataType.STRING));
        schemas.add(new MeasurementSchema("auto_id", TSDataType.STRING));
        schemas.add(new MeasurementSchema("attribute", TSDataType.STRING));
        schemas.add(new MeasurementSchema("auto_attribute", TSDataType.STRING));
        schemas.add(new MeasurementSchema("boolean", TSDataType.BOOLEAN));
        schemas.add(new MeasurementSchema("auto_Int32", TSDataType.INT32));
        schemas.add(new MeasurementSchema("auto_Int64", TSDataType.INT64));
        schemas.add(new MeasurementSchema("auto_FLOAT", TSDataType.FLOAT));
        schemas.add(new MeasurementSchema("double", TSDataType.DOUBLE));
        schemas.add(new MeasurementSchema("text", TSDataType.TEXT));
        schemas.add(new MeasurementSchema("string", TSDataType.STRING));
        schemas.add(new MeasurementSchema("blob", TSDataType.BLOB));
        schemas.add(new MeasurementSchema("timestamp", TSDataType.TIMESTAMP));
        schemas.add(new MeasurementSchema("date", TSDataType.DATE));
        // 准备列类型
        List<Tablet.ColumnType> columnTypes = Arrays.asList(
                Tablet.ColumnType.ID,
                Tablet.ColumnType.ID,
                Tablet.ColumnType.ATTRIBUTE,
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
        Tablet tablet = new Tablet("autocolumn", schemas, columnTypes,10);
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData3(); it.hasNext(); ) {
            expect++;
            // 获取每行的SQL语句
            Object[] line = it.next();
            // 实例化有效行并切换行索引
            int rowIndex = tablet.rowSize++;
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取每行每列的数据
            for (int i = 0; i < schemas.size(); i++) {
                // 根据数据类型添加值到tablet
                switch (schemas.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-08-15") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
        }
        // 插入数据
        session.insertRelationalTablet(tablet, true);

        // 计算实际表的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from autocolumn")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsert_V1_Normal 的 insertRelationalTablet_autoTable 实际不一致期待：" + expect + "，实际：" + actual;
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
