package org.apache.iotdb.api.test.table.data_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSession;
import org.apache.iotdb.session.TableSessionBuilder;
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
import java.time.ZoneId;
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

    private final List<String> measurementList = new ArrayList<>();
    private final List<TSDataType> dataTypeList = new ArrayList<>();
    private final List<ColumnCategory> columnCategoryList = new ArrayList<>();

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
                "    S2 INT32 FIELD," +
                "    s3 INT64 FIELD," +
                "    S4 FLOAT FIELD," +
                "    s5 DOUBLE FIELD," +
                "    S6 TEXT FIELD," +
                "    s7 STRING FIELD," +
                "    S8 BLOB FIELD," +
                "    s9 DATE FIELD," +
                "    S10 TIMESTAMP FIELD" +
                ")");
        session.executeNonQueryStatement("create table t2 (" +
                "    TAG1 string tag," +
                "    attr1 string ATTRIBUTE," +
                "    s1 BOOLEAN FIELD," +
                "    S2 INT32 FIELD," +
                "    s3 INT64 FIELD," +
                "    S4 FLOAT FIELD," +
                "    s5 DOUBLE FIELD," +
                "    S6 TEXT FIELD," +
                "    s7 STRING FIELD," +
                "    S8 BLOB FIELD," +
                "    s9 DATE FIELD," +
                "    S10 TIMESTAMP FIELD" +
                ")");
        // 插入列名
        measurementList.add("TAG1");
        measurementList.add("attr1");
        measurementList.add("s1");
        measurementList.add("S2");
        measurementList.add("s3");
        measurementList.add("S4");
        measurementList.add("s5");
        measurementList.add("S6");
        measurementList.add("s7");
        measurementList.add("S8");
        measurementList.add("s9");
        measurementList.add("S10");
        // 插入数据类型
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
        // 插入列类型
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
        // 插入有效值（部分为空）
        for (int row = 0; row < 10; row++) {
            tablet.addTimestamp(row, time++);
            if (row % 2 == 0) {
                tablet.addValue(measurementList.get(0), row, "TAG1");
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
        session.executeNonQueryStatement("flush");
    }

    /**
     * 清空测试环境
     */
    @AfterClass
    public void beforeTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        session.executeNonQueryStatement("drop database TestSelect");
    }

    /**
     * 获取正确的数据并解析文档
     */
    @DataProvider(name = "select")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/table/select_normal.csv", true).getData();
    }

    /**
     * 测试使用 DataIterator 获取查询数据
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void test1() throws IoTDBConnectionException, StatementExecutionException {
        // 构造 DataIterator 对象（查询列名大写，则返回的列名也是大写）
        SessionDataSet dataSet = session.executeQueryStatement("select time,TAG1,attr1,s1,S2,s3,S4,s5,S6,s7,S8,s9,S10 from t1 order by time");
        SessionDataSet.DataIterator dataIterator = dataSet.new DataIterator();
        // 验证第一列为time列
        assert "time".equals(dataIterator.getColumnNameList().get(0)) : "期待列名和实际不一致，期待：time，实际：" + dataIterator.getColumnNameList().get(0);
        assert "TIMESTAMP".equals(dataIterator.getColumnTypeList().get(0)) : "期待数据类型名和实际不一致，期待：TIMESTAMP，实际：" + dataIterator.getColumnTypeList().get(0);
        // 验证其他列
        for (int i = 0; i < measurementList.size(); i++) {
            assert measurementList.get(i).equals(dataIterator.getColumnNameList().get(i + 1)) : "期待列名和实际不一致，期待：" + measurementList.get(i) + "，实际：" + dataIterator.getColumnNameList().get(i + 1);
        }
        for (int i = 0; i < dataTypeList.size(); i++) {
            switch (dataTypeList.get(i)) {
                case STRING:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("STRING") : "期待数据类型和实际不一致，期待：STRING，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case BOOLEAN:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("BOOLEAN") : "期待数据类型和实际不一致，期待：BOOLEAN，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case INT32:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("INT32") : "期待数据类型和实际不一致，期待：INT32，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case INT64:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("INT64") : "期待数据类型和实际不一致，期待：INT64，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case FLOAT:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("FLOAT") : "期待数据类型和实际不一致，期待：FLOAT，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case DOUBLE:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("DOUBLE") : "期待数据类型和实际不一致，期待：DOUBLE，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case TEXT:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("TEXT") : "期待数据类型和实际不一致，期待：TEXT，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case BLOB:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("BLOB") : "期待数据类型和实际不一致，期待：BLOB，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case DATE:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("DATE") : "期待数据类型和实际不一致，期待：DATE，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                case TIMESTAMP:
                    assert dataIterator.getColumnTypeList().get(i + 1).equals("TIMESTAMP") : "期待数据类型和实际不一致，期待：TIMESTAMP，实际：" + dataIterator.getColumnTypeList().get(i + 1);
                    break;
                default:
                    assert false : "未知数据类型";
            }
        }
        // 验证值
        while (dataIterator.next()) {
            if (!(dataIterator.getString("TAG1") == null)) {
                assert dataIterator.getString("TAG1").equals(dataIterator.getString(2)) : "期待值和实际不一致，期待：" + dataIterator.getString("TAG1") + "，实际：" + dataIterator.getString(2);
                assert dataIterator.getString("attr1").equals(dataIterator.getString(3)) : "期待值和实际不一致，期待：" + dataIterator.getString("attr1") + "，实际：" + dataIterator.getString(3);
                assert dataIterator.getBoolean("s1") == dataIterator.getBoolean(4) : "期待值和实际不一致，期待：" + dataIterator.getBoolean("s1") + "，实际：" + dataIterator.getString(4);
            }
            dataIterator.getInt("S2");
            dataIterator.getLong("s3");
            dataIterator.getFloat("S4");
            dataIterator.getDouble("s5");
            dataIterator.getString("S6");
            dataIterator.getString("s7");
            dataIterator.getBlob("S8");
            dataIterator.getDate("s9");
            dataIterator.getTimestamp("S10");
            dataIterator.getObject("TAG1");
            dataIterator.getObject("attr1");
            dataIterator.getObject("s1");
            dataIterator.getObject("S2");
            dataIterator.getObject("s3");
            dataIterator.getObject("S4");
            dataIterator.getObject("s5");
            dataIterator.getObject("S6");
            dataIterator.getObject("s7");
            dataIterator.getObject("S8");
            dataIterator.getObject("s9");
            dataIterator.getObject("S10");
            //
            dataIterator.getInt(5);
            dataIterator.getLong(6);
            dataIterator.getFloat(7);
            dataIterator.getDouble(8);
            dataIterator.getString(9);
            dataIterator.getString(10);
            dataIterator.getBlob(11);
            dataIterator.getDate(12);
            dataIterator.getTimestamp(13);
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
            dataIterator.getObject(13);
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
            dataIterator.isNull(13);
            dataIterator.isNull("TAG1");
            dataIterator.isNull("attr1");
            dataIterator.isNull("s1");
            dataIterator.isNull("S2");
            dataIterator.isNull("s3");
            dataIterator.isNull("S4");
            dataIterator.isNull("s5");
            dataIterator.isNull("S6");
            dataIterator.isNull("s7");
            dataIterator.isNull("S8");
            dataIterator.isNull("s9");
            dataIterator.isNull("S10");
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
            dataIterator.getString(13);
            dataIterator.getString("TAG1");
            dataIterator.getString("attr1");
            dataIterator.getString("s1");
            dataIterator.getString("S2");
            dataIterator.getString("s3");
            dataIterator.getString("S4");
            dataIterator.getString("s5");
            dataIterator.getString("S6");
            dataIterator.getString("s7");
            dataIterator.getString("S8");
            dataIterator.getString("s9");
            dataIterator.getString("S10");
            // 错误情况
            try {
                dataIterator.getString("None");
            } catch (Exception e) {
                assert e.getMessage().equals("Unknown column name: None");
            }
        }
    }

    /**
     * 测试使用Field获取查询数据 TODO：待完善
     */
//    @Test(priority = 20) // 测试执行的优先级为10
    public void test2() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 查询数据
        SessionDataSet dataSet = session.executeQueryStatement("select * from t1");
//        System.out.println(dataSet.getColumnNames());
//        System.out.println(dataSet.getColumnTypes());
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
     * 测试 Extract 时间函数在不同时区下的变化
     */
    @Test(priority = 110)
    public void test4() {
        // 在 UTC+8 时区下插入数据并查询
        try (ITableSession session_utc8 = new TableSessionBuilder().zoneId(ZoneId.of("UTC+8")).build()) {
            session_utc8.executeNonQueryStatement("drop database if exists test_extract");
            session_utc8.executeNonQueryStatement("create database test_extract");
            session_utc8.executeNonQueryStatement("use test_extract");
            session_utc8.executeNonQueryStatement("create table table1(t1 STRING TAG,s1 TIMESTAMP FIELD, S2 INT64 FIELD)");
            // 插入时间 2025-01-01 08:00:00.000 (UTC+8时区)
            session_utc8.executeNonQueryStatement("INSERT INTO table1(time,t1,s1,S2) values(2025-01-01 08:00:00.000, 't1', 2025-01-01 08:00:00.000, 1)");

            SessionDataSet dataSet = session_utc8.executeQueryStatement("select extract(hour from s1) from table1");
            while (dataSet.hasNext()) {
                long value = dataSet.next().getField(0).getLongV();
                assert value == 8 : "在UTC+8时区下应该提取到小时数为8，实际为: " + value;
            }
            dataSet.closeOperationHandle();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }

        // 在 UTC+0 时区下查询相同数据
        try (ITableSession session_utc0 = new TableSessionBuilder().zoneId(ZoneId.of("UTC+0")).build()) {
            session_utc0.executeNonQueryStatement("use test_extract");
            SessionDataSet dataSet = session_utc0.executeQueryStatement("select extract(hour from s1) from table1");
            while (dataSet.hasNext()) {
                long value = dataSet.next().getField(0).getLongV();
                assert value == 0 : "在UTC+0时区下应该提取到小时数为0，实际为: " + value;
            }
            dataSet.closeOperationHandle();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }

        // 在 UTC-8 时区下查询相同数据
        try (ITableSession session_utc0 = new TableSessionBuilder().zoneId(ZoneId.of("UTC-8")).build()) {
            session_utc0.executeNonQueryStatement("use test_extract");
            SessionDataSet dataSet = session_utc0.executeQueryStatement("select extract(hour from s1) from table1");
            while (dataSet.hasNext()) {
                long value = dataSet.next().getField(0).getLongV();
                assert value == 16 : "在UTC+0时区下应该提取到小时数为16，实际为: " + value;
            }
            dataSet.closeOperationHandle();
            session_utc0.executeNonQueryStatement("drop database if exists test_extract");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
