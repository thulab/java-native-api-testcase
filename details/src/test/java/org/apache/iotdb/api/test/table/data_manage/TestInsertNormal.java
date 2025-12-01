package org.apache.iotdb.api.test.table.data_manage;

import org.apache.iotdb.api.test.BaseTestSuiteTableModel;
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
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Title：测试插入数据—正常情况
 * Describe：测试各种方式的数据插入的操作，包括SQL语句插入、插入含空和不含空、自动创建情况等等
 * Author：肖林捷
 * Date：2024/12/29
 */
public class TestInsertNormal extends BaseTestSuiteTableModel {
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
        session.executeNonQueryStatement("create table insertSQL (" +
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
        session.executeNonQueryStatement("create table insertNoNull (" +
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
        session.executeNonQueryStatement("create table insertNull (" +
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
        session.executeNonQueryStatement("create table insertAutoCreateColumn (" +
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
    }

    /**
     * 清空测试环境
     */
    @AfterClass
    public void beforeTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        session.executeNonQueryStatement("drop database TestInsert");
    }

    /**
     * 提供SQL语句数据
     */
    public Iterator<Object[]> getSQLData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insertSQL_normal.csv", true).getData();
    }
    /**
     * 提供无空值数据
     */
    public Iterator<Object[]> getNoNullData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insertNoNull_normal.csv", false).getData();
    }
    /**
     * 提供含空值数据
     */
    public Iterator<Object[]> getNullData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insertNull_normal.csv", false).getData();
    }
    /**
     * 提供用于自动创建数据
     */
    public Iterator<Object[]> getAutoData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insert_auto_normal.csv", false).getData();
    }
    /**
     * 提供全为空数据
     */
    public Iterator<Object[]> getAllNullData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insertAllNull_normal.csv", false).getData();
    }
    /**
     * 提供仅Tag数据
     */
    public Iterator<Object[]> getOnlyTagData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insertOnlyTag_normal.csv", false).getData();
    }
    /**
     * 提供仅Attr数据
     */
    public Iterator<Object[]> getOnlyAttrData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insertOnlyAttr_normal.csv", false).getData();
    }
    /**
     * 提供仅Tag和Attr数据
     */
    public Iterator<Object[]> getOnlyTAGAndAttrData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insertOnlyTagAndAttr_normal.csv", false).getData();
    }
    /**
     * 提供仅Field数据
     */
    public Iterator<Object[]> getOnlyFieldData() throws IOException {
        return new CustomDataProvider().load_table("data/table/insertOnlyField_normal.csv", false).getData();
    }

    /**
     * 测试使用executeQueryStatement方法执行SQL语句插入数据
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertSQL() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 获取解析后每行的数据
        for (Iterator<Object[]> it = getSQLData(); it.hasNext(); ) {
            // 获取该行的SQL语句
            Object[] insertSQLs = it.next();
            // 获取该行每列的数据
            for (Object insertSQL : insertSQLs) {
                // 统计期待数据的行数
                expect++;
                // 插入数据
                session.executeNonQueryStatement((String) insertSQL);
            }
        }
        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertSQL")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertSQL方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法插入无空值数据
     */
    @Test(priority = 20)
    public void insertNoNull() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 10;
        // 准备列名
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
        // 准备值类型
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
        // 准备列类型
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
        Tablet tablet = new Tablet("insertNoNull", measurementList, dataTypeList, columnCategoryList, maxRowSize);

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getNoNullData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行的SQL语句
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 获取该行每列的数据
            for (int i = 0; i < measurementList.size(); i++) {
                // 根据数据类型添加值到tablet，对于获取到的null值会设置默认值确保无null值
                switch (dataTypeList.get(i)) {
                    case BOOLEAN:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet.addValue(measurementList.get(i), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
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
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际每行的数量
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertNoNull")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断数量是否符合预期
        assert expect == actual : "TestInsertNormal类的insertNoNull方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法插入有空值数据
     */
    @Test(priority = 30)
    public void insertNull() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 18;
        // 准备列名
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
        // 准备值类型
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
        // 准备列类型
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
        Tablet tablet = new Tablet("insertNull", measurementList, dataTypeList, columnCategoryList, maxRowSize);
        tablet.initBitMaps();

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getNullData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行数据
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 获取该行每列的数据
            for (int i = 0; i < measurementList.size(); i++) {
                // 判断是否为空，为空就不添加值
                if (line[i + 1] != null) {
                    // 不为空，则根据数据类型添加值到tablet
                    switch (dataTypeList.get(i)) {
                        case BOOLEAN:
                            tablet.addValue(measurementList.get(i), rowIndex, Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(measurementList.get(i), rowIndex, Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(measurementList.get(i), rowIndex, Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(measurementList.get(i), rowIndex, Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(measurementList.get(i), rowIndex, Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(measurementList.get(i), rowIndex, new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(measurementList.get(i), rowIndex, LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertNull")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertNull方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法自动创建标识、属性和测点列 TODO:补充Java原生接口insertTablet自动创建元数据表名带多个引号的测试用例
     */
    @Test(priority = 40)
    public void insertAutoCreateColumn() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 10;
        // 准备列名
        List<String> measurementList = new ArrayList<>();
        measurementList.add("device_id");
        measurementList.add("auto_device_id");
        measurementList.add("attribute");
        measurementList.add("auto_attribute");
        measurementList.add("boolean");
        measurementList.add("auto_boolean");
        measurementList.add("int32");
        measurementList.add("auto_int32");
        measurementList.add("int64");
        measurementList.add("auto_int64");
        measurementList.add("FLOAT");
        measurementList.add("auto_FLOAT");
        measurementList.add("double");
        measurementList.add("auto_double");
        measurementList.add("text");
        measurementList.add("auto_text");
        measurementList.add("string");
        measurementList.add("auto_string");
        measurementList.add("blob");
        measurementList.add("auto_blob");
        measurementList.add("timestamp");
        measurementList.add("auto_timestamp");
        measurementList.add("date");
        measurementList.add("auto_date");
        // 准备值类型
        List<TSDataType> dataTypeList = new ArrayList<>();
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.BOOLEAN);
        dataTypeList.add(TSDataType.BOOLEAN);
        dataTypeList.add(TSDataType.INT32);
        dataTypeList.add(TSDataType.INT32);
        dataTypeList.add(TSDataType.INT64);
        dataTypeList.add(TSDataType.INT64);
        dataTypeList.add(TSDataType.FLOAT);
        dataTypeList.add(TSDataType.FLOAT);
        dataTypeList.add(TSDataType.DOUBLE);
        dataTypeList.add(TSDataType.DOUBLE);
        dataTypeList.add(TSDataType.TEXT);
        dataTypeList.add(TSDataType.TEXT);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.BLOB);
        dataTypeList.add(TSDataType.BLOB);
        dataTypeList.add(TSDataType.TIMESTAMP);
        dataTypeList.add(TSDataType.TIMESTAMP);
        dataTypeList.add(TSDataType.DATE);
        dataTypeList.add(TSDataType.DATE);
        // 准备列类型
        List<ColumnCategory> columnCategoryList = new ArrayList<>();
        columnCategoryList.add(ColumnCategory.TAG);
        columnCategoryList.add(ColumnCategory.TAG);
        columnCategoryList.add(ColumnCategory.ATTRIBUTE);
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
        Tablet tablet = new Tablet("insertAutoCreateColumn", measurementList, dataTypeList, columnCategoryList, maxRowSize);

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getAutoData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行的SQL语句
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取该行每列的数据
            for (int i = 0; i < tablet.getRowSize(); i++) {
                // 根据数据类型添加值到tablet，对于获取到的null值会设置默认值确保无null值
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
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertAutoCreateColumn")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertAutoCreateColumn方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法自动创建table
     */
    @Test(priority = 50)
    public void insertAutoCreateTable() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 10;
        // 准备列名
        List<String> measurementList = new ArrayList<>();
        measurementList.add("device_id");
        measurementList.add("auto_device_id");
        measurementList.add("attribute");
        measurementList.add("auto_attribute");
        measurementList.add("boolean");
        measurementList.add("auto_boolean");
        measurementList.add("int32");
        measurementList.add("auto_int32");
        measurementList.add("int64");
        measurementList.add("auto_int64");
        measurementList.add("FLOAT");
        measurementList.add("auto_FLOAT");
        measurementList.add("double");
        measurementList.add("auto_double");
        measurementList.add("text");
        measurementList.add("auto_text");
        measurementList.add("string");
        measurementList.add("auto_string");
        measurementList.add("blob");
        measurementList.add("auto_blob");
        measurementList.add("timestamp");
        measurementList.add("auto_timestamp");
        measurementList.add("date");
        measurementList.add("auto_date");
        // 准备值类型
        List<TSDataType> dataTypeList = new ArrayList<>();
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.BOOLEAN);
        dataTypeList.add(TSDataType.BOOLEAN);
        dataTypeList.add(TSDataType.INT32);
        dataTypeList.add(TSDataType.INT32);
        dataTypeList.add(TSDataType.INT64);
        dataTypeList.add(TSDataType.INT64);
        dataTypeList.add(TSDataType.FLOAT);
        dataTypeList.add(TSDataType.FLOAT);
        dataTypeList.add(TSDataType.DOUBLE);
        dataTypeList.add(TSDataType.DOUBLE);
        dataTypeList.add(TSDataType.TEXT);
        dataTypeList.add(TSDataType.TEXT);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.STRING);
        dataTypeList.add(TSDataType.BLOB);
        dataTypeList.add(TSDataType.BLOB);
        dataTypeList.add(TSDataType.TIMESTAMP);
        dataTypeList.add(TSDataType.TIMESTAMP);
        dataTypeList.add(TSDataType.DATE);
        dataTypeList.add(TSDataType.DATE);
        // 准备列类型
        List<ColumnCategory> columnCategoryList = new ArrayList<>();
        columnCategoryList.add(ColumnCategory.TAG);
        columnCategoryList.add(ColumnCategory.TAG);
        columnCategoryList.add(ColumnCategory.ATTRIBUTE);
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
        Tablet tablet = new Tablet("insertAutoCreateTable", measurementList, dataTypeList, columnCategoryList, maxRowSize);

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getAutoData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行的SQL语句
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取该行每列的数据
            for (int i = 0; i < tablet.getRowSize(); i++) {
                // 根据数据类型添加值到tablet，对于获取到的null值会设置默认值确保无null值
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
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertAutoCreateTable")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertAutoCreateColumn方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法插入全空值数据
     */
    @Test(priority = 60)
    public void insertAllNull() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 5;
        // 准备列名
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
        // 准备值类型
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
        // 准备列类型
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
        Tablet tablet = new Tablet("insertAllNull", measurementList, dataTypeList, columnCategoryList, maxRowSize);
        tablet.initBitMaps();

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getAllNullData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行数据
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertAllNull")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertAllNull方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法插入仅标识列数据
     */
    @Test(priority = 70)
    public void insertOnlyTag() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 5;
        // 准备列名
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
        // 准备值类型
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
        // 准备列类型
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
        Tablet tablet = new Tablet("insertOnlyTag", measurementList, dataTypeList, columnCategoryList, maxRowSize);
        tablet.initBitMaps();

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getOnlyTagData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行数据
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 获取该行每列的数据
            for (int i = 0; i < measurementList.size(); i++) {
                // 判断是否为空，为空就不添加值
                if (line[i + 1] != null) {
                    // 不为空，则根据数据类型添加值到tablet
                    switch (dataTypeList.get(i)) {
                        case BOOLEAN:
                            tablet.addValue(measurementList.get(i), rowIndex, Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(measurementList.get(i), rowIndex, Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(measurementList.get(i), rowIndex, Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(measurementList.get(i), rowIndex, Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(measurementList.get(i), rowIndex, Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(measurementList.get(i), rowIndex, new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(measurementList.get(i), rowIndex, LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertOnlyTag")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertOnlyTag方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法插入仅属性列数据
     */
    @Test(priority = 80)
    public void insertOnlyAttr() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 5;
        // 准备列名
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
        // 准备值类型
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
        // 准备列类型
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
        Tablet tablet = new Tablet("insertOnlyAttr", measurementList, dataTypeList, columnCategoryList, maxRowSize);
        tablet.initBitMaps();

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getOnlyAttrData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行数据
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 获取该行每列的数据
            for (int i = 0; i < measurementList.size(); i++) {
                // 判断是否为空，为空就不添加值
                if (line[i + 1] != null) {
                    // 不为空，则根据数据类型添加值到tablet
                    switch (dataTypeList.get(i)) {
                        case BOOLEAN:
                            tablet.addValue(measurementList.get(i), rowIndex, Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(measurementList.get(i), rowIndex, Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(measurementList.get(i), rowIndex, Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(measurementList.get(i), rowIndex, Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(measurementList.get(i), rowIndex, Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(measurementList.get(i), rowIndex, new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(measurementList.get(i), rowIndex, LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertOnlyAttr")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertOnlyAttr方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法插入仅属性列数据
     */
    @Test(priority = 90)
    public void insertOnlyTagAndAttr() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 5;
        // 准备列名
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
        // 准备值类型
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
        // 准备列类型
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
        Tablet tablet = new Tablet("insertOnlyTagAndAttr", measurementList, dataTypeList, columnCategoryList, maxRowSize);
        tablet.initBitMaps();

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getOnlyTAGAndAttrData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行数据
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 获取该行每列的数据
            for (int i = 0; i < measurementList.size(); i++) {
                // 判断是否为空，为空就不添加值
                if (line[i + 1] != null) {
                    // 不为空，则根据数据类型添加值到tablet
                    switch (dataTypeList.get(i)) {
                        case BOOLEAN:
                            tablet.addValue(measurementList.get(i), rowIndex, Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(measurementList.get(i), rowIndex, Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(measurementList.get(i), rowIndex, Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(measurementList.get(i), rowIndex, Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(measurementList.get(i), rowIndex, Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(measurementList.get(i), rowIndex, new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(measurementList.get(i), rowIndex, LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertOnlyTagAndAttr")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertOnlyTagAndAttr方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试使用insert方法插入仅属性列数据
     */
    @Test(priority = 90)
    public void insertOnlyField() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待数据的行数
        int expect = 0;
        // 实际数据的行数
        int actual = 0;
        // 最大行数
        int maxRowSize = 5;
        // 准备列名
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
        // 准备值类型
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
        // 准备列类型
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
        Tablet tablet = new Tablet("insertOnlyField", measurementList, dataTypeList, columnCategoryList, maxRowSize);
        tablet.initBitMaps();

        // 设置初始索引为0，表示从0行开始添加值
        int rowIndex = 0;
        // 获取解析后的每行数据
        for (Iterator<Object[]> it = getOnlyFieldData(); it.hasNext(); ) {
            // 统计期待数据的行数
            expect++;
            // 获取该行数据
            Object[] line = it.next();
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 获取该行每列的数据
            for (int i = 0; i < measurementList.size(); i++) {
                // 判断是否为空，为空就不添加值
                if (line[i + 1] != null) {
                    // 不为空，则根据数据类型添加值到tablet
                    switch (dataTypeList.get(i)) {
                        case BOOLEAN:
                            tablet.addValue(measurementList.get(i), rowIndex, Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(measurementList.get(i), rowIndex, Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(measurementList.get(i), rowIndex, Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(measurementList.get(i), rowIndex, Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(measurementList.get(i), rowIndex, Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(measurementList.get(i), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(measurementList.get(i), rowIndex, new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(measurementList.get(i), rowIndex, LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            // 切换到下一行
            rowIndex++;
        }
        // 插入数据
        session.insert(tablet);

        // 计算实际数据的行数
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from insertOnlyField")) {
            while (dataSet.hasNext()) {
                dataSet.next();
                actual++;
            }
        }
        // 判断是否符合预期
        assert expect == actual : "TestInsertNormal类的insertOnlyField方法实际与期待数量不一致，期待：" + expect + "，实际：" + actual;
    }

}
