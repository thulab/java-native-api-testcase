package org.apache.iotdb.api.test.table.data_manage;

import org.apache.iotdb.api.test.BaseTestSuiteTableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
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
import java.util.Iterator;
import java.util.List;

/**
 * Title：测试数据查询—正常情况
 * Describe：测试各种方式的数据查询的操作
 * Author：肖林捷
 * Date：2024/12/29
 */
public class TestSelectNormal extends BaseTestSuiteTableModel {

    private final List<String> measurementList = new ArrayList<>();
    private final List<TSDataType> dataTypeList = new ArrayList<>();
    private final List<ColumnCategory> columnCategoryList = new ArrayList<>();
    private final int expectNum1 = 10;
    private final int expectNum2 = 0;

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
        session.executeNonQueryStatement("create table Table1 (" +
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
        session.executeNonQueryStatement("create table Table2 (" +
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
        session.executeNonQueryStatement("create table Table_Empty (" +
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
        Tablet tablet = new Tablet("Table1", measurementList, dataTypeList, columnCategoryList, 100);
        int time = 0;
        // 插入有效值（部分为空）
        for (int row = 0; row < expectNum1; row++) {
            tablet.addTimestamp(row, time++);
            if (row % 2 == 0) {
                tablet.addValue(measurementList.get(0), row, "TAG1");
                tablet.addValue(measurementList.get(1), row, measurementList.get(1));
                tablet.addValue(measurementList.get(2), row, true);
                tablet.addValue(measurementList.get(3), row, 1);
                tablet.addValue(measurementList.get(4), row, 1L);
                tablet.addValue(measurementList.get(5), row, 1.1F);
                tablet.addValue(measurementList.get(6), row, 1.1);
                tablet.addValue(measurementList.get(7), row, "text2");
                tablet.addValue(measurementList.get(8), row, "string2");
                tablet.addValue(measurementList.get(9), row, new Binary("blob2", Charset.defaultCharset()));
                tablet.addValue(measurementList.get(10), row, LocalDate.of(2000, 1, 2));
                tablet.addValue(measurementList.get(11), row, 1L);
            }
        }
        session.insert(tablet);
        session.executeNonQueryStatement("flush");
    }

    /**
     * 获取正确的数据并解析文档
     */
    @DataProvider(name = "select")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/table/select_normal.csv", true).getData();
    }

    /**
     * DataIterator 正常情况测试
     */
    @Test(priority = 10)
    public void testDataIteratorNormal() throws IoTDBConnectionException, StatementExecutionException {
        int actualNum = 0;
        StringBuilder columns = new StringBuilder("time");
        // 构造查询列名
        for (String s : measurementList) {
            columns.append(",").append(s);
        }
        // 构造 DataIterator 对象（查询列名大写，则返回的列名也是大写）
        SessionDataSet dataSet = session.executeQueryStatement("select " + columns + " from Table1 order by time");
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
            // TAG列
            if (!dataIterator.isNull(measurementList.get(0))) { // 非空值
                assert dataIterator.getString(measurementList.get(0)).equals(dataIterator.getString(2)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(0)) + "，实际：" + dataIterator.getString(2);
                assert dataIterator.getString(measurementList.get(0)).equals(dataIterator.getObject(2)) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(0)) + "，实际：" + dataIterator.getObject(2);
                assert dataIterator.getString(measurementList.get(0)).equals(dataIterator.getObject(measurementList.get(0))) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(0)) + "，实际：" + dataIterator.getObject(measurementList.get(0));
                assert dataIterator.isNull(measurementList.get(0)) == dataIterator.isNull(2) : "期待值和实际不一致，期待：" + measurementList.get(0) + "，实际：" + dataIterator.isNull(2);
                assert 2 == dataIterator.findColumn(measurementList.get(0)) : "期待值和实际不一致，期待：" + 2 + "，实际：" + dataIterator.findColumn(measurementList.get(0));
                assert dataIterator.getString(measurementList.get(0)).equals(dataIterator.getString(2)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(0)) + "，实际：" + dataIterator.getString(2);
            } else if (dataIterator.isNull(2)) { // 空值
                assert dataIterator.getString(measurementList.get(0)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(0));
                assert dataIterator.getString(2) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(2);
                assert dataIterator.getObject(2) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(2);
                assert dataIterator.getObject(measurementList.get(0)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(0));
                assert 2 == dataIterator.findColumn(measurementList.get(0)) : "期待值和实际不一致，期待：" + 2 + "，实际：" + dataIterator.findColumn(measurementList.get(0));
                assert dataIterator.getString(measurementList.get(0)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(0));
                assert dataIterator.getString(2) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(2);
            }
            // ATTRIBUTE列
            if (!dataIterator.isNull(measurementList.get(1))) { // 非空值
                assert dataIterator.getString(measurementList.get(1)).equals(dataIterator.getString(3)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(1)) + "，实际：" + dataIterator.getString(3);
                assert dataIterator.getString(measurementList.get(1)).equals(dataIterator.getObject(3)) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(1)) + "，实际：" + dataIterator.getObject(3);
                assert dataIterator.getString(measurementList.get(1)).equals(dataIterator.getObject(measurementList.get(1))) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(1)) + "，实际：" + dataIterator.getObject(measurementList.get(1));
                assert dataIterator.isNull(measurementList.get(0)) == dataIterator.isNull(3) : "期待值和实际不一致，期待：" + measurementList.get(1) + "，实际：" + dataIterator.isNull(3);
                assert 3 == dataIterator.findColumn(measurementList.get(1)) : "期待值和实际不一致，期待：" + 3 + "，实际：" + dataIterator.findColumn(measurementList.get(1));
                assert dataIterator.getString(measurementList.get(1)).equals(dataIterator.getString(3)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(1)) + "，实际：" + dataIterator.getString(3);
            } else if (dataIterator.isNull(3)) { // 空值
                assert dataIterator.getString(measurementList.get(1)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(1));
                assert dataIterator.getString(3) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(3);
                assert dataIterator.getObject(3) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(3);
                assert dataIterator.getObject(measurementList.get(1)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(1));
                assert 3 == dataIterator.findColumn(measurementList.get(1)) : "期待值和实际不一致，期待：" + 3 + "，实际：" + dataIterator.findColumn(measurementList.get(1));
                assert dataIterator.getString(measurementList.get(1)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(1));
                assert dataIterator.getString(3) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(3);
            }
            // BOOLEAN类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(2))) { // 非空值
                assert dataIterator.getBoolean(measurementList.get(2)) == dataIterator.getBoolean(4) : "期待值和实际不一致，期待：" + dataIterator.getBoolean(measurementList.get(2)) + "，实际：" + dataIterator.getBoolean(4);
                assert dataIterator.getBoolean(measurementList.get(2)) == (Boolean) dataIterator.getObject(4) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(2)) + "，实际：" + dataIterator.getObject(4);
                assert dataIterator.getString(measurementList.get(2)).equals(dataIterator.getObject(measurementList.get(2)).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(2)) + "，实际：" + dataIterator.getObject(measurementList.get(2));
                assert dataIterator.isNull(measurementList.get(2)) == dataIterator.isNull(4) : "期待值和实际不一致，期待：" + measurementList.get(2) + "，实际：" + dataIterator.isNull(4);
                assert 4 == dataIterator.findColumn(measurementList.get(2)) : "期待值和实际不一致，期待：" + 4 + "，实际：" + dataIterator.findColumn(measurementList.get(2));
                assert dataIterator.getString(measurementList.get(2)).equals(dataIterator.getString(4)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(2)) + "，实际：" + dataIterator.getString(4);
            } else if (dataIterator.isNull(4)) { // 空值
                assert !dataIterator.getBoolean(measurementList.get(2)) : "期待值和实际不一致，期待：" + false + "，实际：" + dataIterator.getBoolean(measurementList.get(2));
                assert !dataIterator.getBoolean(4) : "期待值和实际不一致，期待：" + false + "，实际：" + dataIterator.getBoolean(4);
                assert dataIterator.getObject(4) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(4);
                assert dataIterator.getObject(measurementList.get(2)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(2));
                assert 4 == dataIterator.findColumn(measurementList.get(2)) : "期待值和实际不一致，期待：" + 4 + "，实际：" + dataIterator.findColumn(measurementList.get(2));
                assert dataIterator.getString(measurementList.get(2)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(2));
                assert dataIterator.getString(4) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(4);
            }
            // INT32类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(3))) { // 非空值
                assert dataIterator.getInt(measurementList.get(3)) == dataIterator.getInt(5) : "期待值和实际不一致，期待：" + dataIterator.getInt(measurementList.get(3)) + "，实际：" + dataIterator.getInt(5);
                assert dataIterator.getInt(measurementList.get(3)) == (Integer) dataIterator.getObject(5) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(3)) + "，实际：" + dataIterator.getObject(5);
                assert dataIterator.getString(measurementList.get(3)).equals(dataIterator.getObject(measurementList.get(3)).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(3)) + "，实际：" + dataIterator.getObject(measurementList.get(3));
                assert dataIterator.isNull(measurementList.get(3)) == dataIterator.isNull(5) : "期待值和实际不一致，期待：" + measurementList.get(3) + "，实际：" + dataIterator.isNull(5);
                assert 5 == dataIterator.findColumn(measurementList.get(3)) : "期待值和实际不一致，期待：" + 5 + "，实际：" + dataIterator.findColumn(measurementList.get(3));
                assert dataIterator.getString(measurementList.get(3)).equals(dataIterator.getString(5)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(3)) + "，实际：" + dataIterator.getString(5);
            } else if (dataIterator.isNull(5)) { // 空值
                assert dataIterator.getInt(measurementList.get(3)) == 0 : "期待值和实际不一致，期待：" + 0 + "，实际：" + dataIterator.getInt(measurementList.get(3));
                assert dataIterator.getInt(5) == 0 : "期待值和实际不一致，期待：" + 0 + "，实际：" + dataIterator.getInt(5);
                assert dataIterator.getObject(5) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(5);
                assert dataIterator.getObject(measurementList.get(3)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(3));
                assert 5 == dataIterator.findColumn(measurementList.get(3)) : "期待值和实际不一致，期待：" + 5 + "，实际：" + dataIterator.findColumn(measurementList.get(3));
                assert dataIterator.getString(measurementList.get(3)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(3));
                assert dataIterator.getString(5) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(5);
            }
            // INT64类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(4))) { // 非空值
                assert dataIterator.getLong(measurementList.get(4)) == dataIterator.getLong(6) : "期待值和实际不一致，期待：" + dataIterator.getLong(measurementList.get(4)) + "，实际：" + dataIterator.getLong(6);
                assert dataIterator.getLong(measurementList.get(4)) == (Long) dataIterator.getObject(6) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(4)) + "，实际：" + dataIterator.getObject(6);
                assert dataIterator.getString(measurementList.get(4)).equals(dataIterator.getObject(measurementList.get(4)).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(4)) + "，实际：" + dataIterator.getObject(measurementList.get(4));
                assert dataIterator.isNull(measurementList.get(4)) == dataIterator.isNull(6) : "期待值和实际不一致，期待：" + measurementList.get(4) + "，实际：" + dataIterator.isNull(6);
                assert 6 == dataIterator.findColumn(measurementList.get(4)) : "期待值和实际不一致，期待：" + 6 + "，实际：" + dataIterator.findColumn(measurementList.get(4));
                assert dataIterator.getString(measurementList.get(4)).equals(dataIterator.getString(6)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(4)) + "，实际：" + dataIterator.getString(6);
            } else if (dataIterator.isNull(6)) { // 空值
                assert dataIterator.getLong(measurementList.get(4)) == 0 : "期待值和实际不一致，期待：" + 0 + "，实际：" + dataIterator.getLong(measurementList.get(4));
                assert dataIterator.getLong(6) == 0 : "期待值和实际不一致，期待：" + 0 + "，实际：" + dataIterator.getLong(6);
                assert dataIterator.getObject(6) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(6);
                assert dataIterator.getObject(measurementList.get(4)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(4));
                assert 6 == dataIterator.findColumn(measurementList.get(4)) : "期待值和实际不一致，期待：" + 6 + "，实际：" + dataIterator.findColumn(measurementList.get(4));
                assert dataIterator.getString(measurementList.get(4)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(4));
                assert dataIterator.getString(6) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(6);
            }
            // FLOAT类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(5))) { // 非空值
                assert dataIterator.getFloat(measurementList.get(5)) == dataIterator.getFloat(7) : "期待值和实际不一致，期待：" + dataIterator.getFloat(measurementList.get(5)) + "，实际：" + dataIterator.getFloat(7);
                assert dataIterator.getFloat(measurementList.get(5)) == (Float) dataIterator.getObject(7) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(5)) + "，实际：" + dataIterator.getObject(7);
                assert dataIterator.getString(measurementList.get(5)).equals(dataIterator.getObject(measurementList.get(5)).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(5)) + "，实际：" + dataIterator.getObject(measurementList.get(5));
                assert dataIterator.isNull(measurementList.get(5)) == dataIterator.isNull(7) : "期待值和实际不一致，期待：" + measurementList.get(5) + "，实际：" + dataIterator.isNull(7);
                assert 7 == dataIterator.findColumn(measurementList.get(5)) : "期待值和实际不一致，期待：" + 7 + "，实际：" + dataIterator.findColumn(measurementList.get(5));
                assert dataIterator.getString(measurementList.get(5)).equals(dataIterator.getString(7)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(5)) + "，实际：" + dataIterator.getString(7);
            } else if (dataIterator.isNull(7)) { // 空值
                assert dataIterator.getFloat(measurementList.get(5)) == 0 : "期待值和实际不一致，期待：" + 0 + "，实际：" + dataIterator.getFloat(measurementList.get(5));
                assert dataIterator.getFloat(7) == 0 : "期待值和实际不一致，期待：" + 0 + "，实际：" + dataIterator.getFloat(7);
                assert dataIterator.getObject(7) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(7);
                assert dataIterator.getObject(measurementList.get(5)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(5));
                assert 7 == dataIterator.findColumn(measurementList.get(5)) : "期待值和实际不一致，期待：" + 7 + "，实际：" + dataIterator.findColumn(measurementList.get(5));
                assert dataIterator.getString(measurementList.get(5)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(5));
                assert dataIterator.getString(7) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(7);
            }
            // DOUBLE类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(6))) { // 非空值
                assert dataIterator.getDouble(measurementList.get(6)) == dataIterator.getDouble(8) : "期待值和实际不一致，期待：" + dataIterator.getDouble(measurementList.get(6)) + "，实际：" + dataIterator.getDouble(8);
                assert dataIterator.getDouble(measurementList.get(6)) == (Double) dataIterator.getObject(8) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(6)) + "，实际：" + dataIterator.getObject(8);
                assert dataIterator.getString(measurementList.get(6)).equals(dataIterator.getObject(measurementList.get(6)).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(6)) + "，实际：" + dataIterator.getObject(measurementList.get(6));
                assert dataIterator.isNull(measurementList.get(6)) == dataIterator.isNull(8) : "期待值和实际不一致，期待：" + measurementList.get(6) + "，实际：" + dataIterator.isNull(8);
                assert 8 == dataIterator.findColumn(measurementList.get(6)) : "期待值和实际不一致，期待：" + 8 + "，实际：" + dataIterator.findColumn(measurementList.get(6));
                assert dataIterator.getString(measurementList.get(6)).equals(dataIterator.getString(8)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(6)) + "，实际：" + dataIterator.getString(8);
            } else if (dataIterator.isNull(8)) { // 空值
                assert dataIterator.getDouble(measurementList.get(6)) == 0 : "期待值和实际不一致，期待：" + 0 + "，实际：" + dataIterator.getDouble(measurementList.get(6));
                assert dataIterator.getDouble(8) == 0 : "期待值和实际不一致，期待：" + 0 + "，实际：" + dataIterator.getDouble(8);
                assert dataIterator.getObject(8) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(8);
                assert dataIterator.getObject(measurementList.get(6)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(6));
                assert 8 == dataIterator.findColumn(measurementList.get(6)) : "期待值和实际不一致，期待：" + 8 + "，实际：" + dataIterator.findColumn(measurementList.get(6));
                assert dataIterator.getString(measurementList.get(6)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(6));
                assert dataIterator.getString(8) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(8);
            }
            // TEXT类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(7))) { // 非空值
                assert dataIterator.getString(measurementList.get(7)).equals(dataIterator.getString(9)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(7)) + "，实际：" + dataIterator.getString(9);
                assert dataIterator.getString(measurementList.get(7)).equals(dataIterator.getObject(9).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(7)) + "，实际：" + dataIterator.getObject(9);
                assert dataIterator.isNull(measurementList.get(7)) == dataIterator.isNull(9) : "期待值和实际不一致，期待：" + measurementList.get(7) + "，实际：" + dataIterator.isNull(9);
                assert 9 == dataIterator.findColumn(measurementList.get(7)) : "期待值和实际不一致，期待：" + 9 + "，实际：" + dataIterator.findColumn(measurementList.get(7));
                assert dataIterator.getString(measurementList.get(7)).equals(dataIterator.getString(9)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(7)) + "，实际：" + dataIterator.getString(9);
            } else if (dataIterator.isNull(9)) { // 空值
                assert dataIterator.getString(measurementList.get(7)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(7));
                assert dataIterator.getString(9) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(9);
                assert dataIterator.getObject(9) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(9);
                assert dataIterator.getObject(measurementList.get(7)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(7));
                assert 9 == dataIterator.findColumn(measurementList.get(7)) : "期待值和实际不一致，期待：" + 9 + "，实际：" + dataIterator.findColumn(measurementList.get(7));
                assert dataIterator.getString(measurementList.get(7)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(7));
                assert dataIterator.getString(9) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(9);
            }
            // STRING类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(8))) { // 非空值
                assert dataIterator.getString(measurementList.get(8)).equals(dataIterator.getString(10)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(8)) + "，实际：" + dataIterator.getString(10);
                assert dataIterator.getString(measurementList.get(8)).equals(dataIterator.getObject(10).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(8)) + "，实际：" + dataIterator.getObject(10);
                assert dataIterator.isNull(measurementList.get(8)) == dataIterator.isNull(10) : "期待值和实际不一致，期待：" + measurementList.get(8) + "，实际：" + dataIterator.isNull(10);
                assert 10 == dataIterator.findColumn(measurementList.get(8)) : "期待值和实际不一致，期待：" + 10 + "，实际：" + dataIterator.findColumn(measurementList.get(8));
                assert dataIterator.getString(measurementList.get(8)).equals(dataIterator.getString(10)) : "期待值和实际不一致，期待：" + dataIterator.getString(measurementList.get(8)) + "，实际：" + dataIterator.getString(10);
            } else if (dataIterator.isNull(10)) { // 空值
                assert dataIterator.getString(measurementList.get(8)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(8));
                assert dataIterator.getString(10) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(10);
                assert dataIterator.getObject(10) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(10);
                assert dataIterator.getObject(measurementList.get(8)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(8));
                assert 10 == dataIterator.findColumn(measurementList.get(8)) : "期待值和实际不一致，期待：" + 10 + "，实际：" + dataIterator.findColumn(measurementList.get(8));
                assert dataIterator.getString(measurementList.get(8)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(measurementList.get(8));
                assert dataIterator.getString(10) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getString(10);
            }
            // BLOB类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(9))) { // 非空值
                assert dataIterator.getBlob(measurementList.get(9)).equals(dataIterator.getBlob(11)) : "期待值和实际不一致，期待：" + dataIterator.getBlob(measurementList.get(9)) + "，实际：" + dataIterator.getBlob(11);
                assert dataIterator.getBlob(measurementList.get(9)).toString().equals(convertHexStringToString(dataIterator.getObject(11).toString())) : "期待值和实际不一致，期待：" + dataIterator.getBlob(measurementList.get(9)).toString() + "，实际：" + dataIterator.getObject(11).toString();
                assert dataIterator.isNull(measurementList.get(9)) == dataIterator.isNull(11) : "期待值和实际不一致，期待：" + measurementList.get(9) + "，实际：" + dataIterator.isNull(11);
                assert 11 == dataIterator.findColumn(measurementList.get(9)) : "期待值和实际不一致，期待：" + 11 + "，实际：" + dataIterator.findColumn(measurementList.get(9));
                assert dataIterator.getBlob(measurementList.get(9)).equals(dataIterator.getBlob(11)) : "期待值和实际不一致，期待：" + dataIterator.getBlob(measurementList.get(9)) + "，实际：" + dataIterator.getBlob(11);
            } else if (dataIterator.isNull(11)) { // 空值
                assert dataIterator.getBlob(measurementList.get(9)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getBlob(measurementList.get(9));
                assert dataIterator.getBlob(11) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getBlob(11);
                assert dataIterator.getObject(11) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(11);
                assert dataIterator.getObject(measurementList.get(9)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(9));
                assert 11 == dataIterator.findColumn(measurementList.get(9)) : "期待值和实际不一致，期待：" + 11 + "，实际：" + dataIterator.findColumn(measurementList.get(9));
                assert dataIterator.getBlob(measurementList.get(9)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getBlob(measurementList.get(9));
                assert dataIterator.getBlob(11) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getBlob(11);
            }
            // DATE类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(10))) { // 非空值
                assert dataIterator.getDate(measurementList.get(10)).equals(dataIterator.getDate(12)) : "期待值和实际不一致，期待：" + dataIterator.getDate(measurementList.get(10)) + "，实际：" + dataIterator.getDate(12);
                assert dataIterator.getDate(measurementList.get(10)).toString().equals(dataIterator.getObject(12).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(10)).toString() + "，实际：" + dataIterator.getObject(12).toString();
                assert dataIterator.isNull(measurementList.get(10)) == dataIterator.isNull(12) : "期待值和实际不一致，期待：" + measurementList.get(10) + "，实际：" + dataIterator.isNull(12);
                assert 12 == dataIterator.findColumn(measurementList.get(10)) : "期待值和实际不一致，期待：" + 12 + "，实际：" + dataIterator.findColumn(measurementList.get(10));
                assert dataIterator.getDate(measurementList.get(10)).equals(dataIterator.getDate(12)) : "期待值和实际不一致，期待：" + dataIterator.getDate(measurementList.get(10)) + "，实际：" + dataIterator.getDate(12);
            } else if (dataIterator.isNull(12)) { // 空值
                assert dataIterator.getDate(measurementList.get(10)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getDate(measurementList.get(10));
                assert dataIterator.getDate(12) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getDate(12);
                assert dataIterator.getObject(12) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(12);
                assert dataIterator.getObject(measurementList.get(10)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(10));
                assert 12 == dataIterator.findColumn(measurementList.get(10)) : "期待值和实际不一致，期待：" + 12 + "，实际：" + dataIterator.findColumn(measurementList.get(10));
                assert dataIterator.getDate(measurementList.get(10)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getDate(measurementList.get(10));
                assert dataIterator.getDate(12) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getDate(12);
            }
            // TIMESTAMP类型的FIELD列
            if (!dataIterator.isNull(measurementList.get(11))) { // 非空值
                assert dataIterator.getTimestamp(measurementList.get(11)).equals(dataIterator.getTimestamp(13)) : "期待值和实际不一致，期待：" + dataIterator.getTimestamp(measurementList.get(11)) + "，实际：" + dataIterator.getTimestamp(13);
                assert dataIterator.getTimestamp(measurementList.get(11)).toString().equals(dataIterator.getObject(13).toString()) : "期待值和实际不一致，期待：" + dataIterator.getObject(measurementList.get(11)).toString() + "，实际：" + dataIterator.getObject(13).toString();
                assert dataIterator.isNull(measurementList.get(11)) == dataIterator.isNull(13) : "期待值和实际不一致，期待：" + measurementList.get(11) + "，实际：" + dataIterator.isNull(13);
                assert 13 == dataIterator.findColumn(measurementList.get(11)) : "期待值和实际不一致，期待：" + 13 + "，实际：" + dataIterator.findColumn(measurementList.get(11));
                assert dataIterator.getTimestamp(measurementList.get(11)).equals(dataIterator.getTimestamp(13)) : "期待值和实际不一致，期待：" + dataIterator.getTimestamp(measurementList.get(11)) + "，实际：" + dataIterator.getTimestamp(13);
            } else if (dataIterator.isNull(13)) { // 空值
                assert dataIterator.getTimestamp(measurementList.get(11)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getTimestamp(measurementList.get(11));
                assert dataIterator.getTimestamp(13) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getTimestamp(13);
                assert dataIterator.getObject(13) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(13);
                assert dataIterator.getObject(measurementList.get(11)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getObject(measurementList.get(11));
                assert 13 == dataIterator.findColumn(measurementList.get(11)) : "期待值和实际不一致，期待：" + 13 + "，实际：" + dataIterator.findColumn(measurementList.get(11));
                assert dataIterator.getTimestamp(measurementList.get(11)) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getTimestamp(measurementList.get(11));
                assert dataIterator.getTimestamp(13) == null : "期待值和实际不一致，期待：null，实际：" + dataIterator.getTimestamp(13);
            }
            actualNum++;
        }
        assert expectNum1 == actualNum : "期待值和实际不一致，期待：" + expectNum1 + "，实际：" + actualNum;
        dataSet.closeOperationHandle();
    }

    // 将Blob类型的十六进制字符串转换为字符串
    private static String convertHexStringToString(String s) {
        s = s.substring(2);
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return new String(data, Charset.defaultCharset());
    }

    /**
     * DataIterator 异常清情况测试
     */
    @Test(priority = 11)
    public void testDataIteratorException() throws IoTDBConnectionException, StatementExecutionException {
        StringBuilder columns = new StringBuilder("time");
        // 构造查询列名
        for (String s : measurementList) {
            columns.append(",").append(s);
        }
        // 构造 DataIterator 对象（查询列名大写，则返回的列名也是大写）
        SessionDataSet dataSet = session.executeQueryStatement("select " + columns + " from Table1 order by time");
        SessionDataSet.DataIterator dataIterator = dataSet.new DataIterator();
        // 验证值
        while (dataIterator.next()) {
            // 错误情况
            try {
                dataIterator.getString("None");
            } catch (Exception e) {
                assert e.getMessage().equals("Unknown column name: None");
            }
        }
    }

    /**
     * 测试使用 Field 获取查询数据 TODO：待完善，补充验证值的一致性，以及代码覆盖率
     */
    @Test(priority = 20)
    public void testFieldNormal() throws IoTDBConnectionException, StatementExecutionException, IOException {
        int actualNum = 0;
        // 列名
        StringBuilder columns = new StringBuilder("time");
        // 构造查询列名
        for (int i = 0; i < measurementList.size(); i++) {
            columns.append(",").append(measurementList.get(i));
        }
        // 构造 DataIterator 对象（查询列名大写，则返回的列名也是大写）
        SessionDataSet dataSet = session.executeQueryStatement("select " + columns + " from Table1 order by time");
        // 验证第一列为time列
        assert "time".equals(dataSet.getColumnNames().get(0)) : "期待列名和实际不一致，期待：time，实际：" + dataSet.getColumnNames().get(0);
        assert "TIMESTAMP".equals(dataSet.getColumnTypes().get(0)) : "期待数据类型名和实际不一致，期待：TIMESTAMP，实际：" + dataSet.getColumnTypes().get(0);;
        // 验证其他列名
        for (int i = 0; i < measurementList.size(); i++) {
            assert measurementList.get(i).equals(dataSet.getColumnNames().get(i + 1)) : "期待列名和实际不一致，期待：" + measurementList.get(i) + "，实际：" + dataSet.getColumnNames().get(i + 1);
        }
        for (int i = 0; i < dataTypeList.size(); i++) {
            switch (dataTypeList.get(i)) {
                case STRING:
                    assert dataSet.getColumnTypes().get(i + 1).equals("STRING") : "期待数据类型和实际不一致，期待：STRING，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case BOOLEAN:
                    assert dataSet.getColumnTypes().get(i + 1).equals("BOOLEAN") : "期待数据类型和实际不一致，期待：BOOLEAN，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case INT32:
                    assert dataSet.getColumnTypes().get(i + 1).equals("INT32") : "期待数据类型和实际不一致，期待：INT32，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case INT64:
                    assert dataSet.getColumnTypes().get(i + 1).equals("INT64") : "期待数据类型和实际不一致，期待：INT64，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case FLOAT:
                    assert dataSet.getColumnTypes().get(i + 1).equals("FLOAT") : "期待数据类型和实际不一致，期待：FLOAT，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case DOUBLE:
                    assert dataSet.getColumnTypes().get(i + 1).equals("DOUBLE") : "期待数据类型和实际不一致，期待：DOUBLE，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case TEXT:
                    assert dataSet.getColumnTypes().get(i + 1).equals("TEXT") : "期待数据类型和实际不一致，期待：TEXT，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case BLOB:
                    assert dataSet.getColumnTypes().get(i + 1).equals("BLOB") : "期待数据类型和实际不一致，期待：BLOB，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case DATE:
                    assert dataSet.getColumnTypes().get(i + 1).equals("DATE") : "期待数据类型和实际不一致，期待：DATE，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                case TIMESTAMP:
                    assert dataSet.getColumnTypes().get(i + 1).equals("TIMESTAMP") : "期待数据类型和实际不一致，期待：TIMESTAMP，实际：" + dataSet.getColumnTypes().get(i + 1);
                    break;
                default:
                    assert false : "未知数据类型";
            }
        }
        dataSet.setFetchSize(dataSet.getFetchSize());
        while (dataSet.hasNext()) {
            dataSet.next();
            actualNum++;
//            RowRecord records = dataSet.next();
//            records.getField(1).getStringValue();
//            records.getField(2).getStringValue();
        }
        assert expectNum1 == actualNum : "期待：" + expectNum1 + "，实际：" + actualNum;
    }

    /**
     * 测试执行各种查询SQL
     */
    @Test(priority = 100) // 测试执行的优先级为10
    public void testSQL() throws IoTDBConnectionException, StatementExecutionException, IOException {
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
    public void testExtractTimeFunction() {
        // 在 UTC+8 时区下插入数据并查询
        try (ITableSession session_utc8 = new TableSessionBuilder().zoneId(ZoneId.of("UTC+8")).build()) {
            session_utc8.executeNonQueryStatement("drop database if exists test_extract");
            session_utc8.executeNonQueryStatement("create database test_extract");
            session_utc8.executeNonQueryStatement("use test_extract");
            session_utc8.executeNonQueryStatement("create table Extract_table(t1 STRING TAG,s1 TIMESTAMP FIELD, S2 INT64 FIELD)");
            // 插入时间 2025-01-01 08:00:00.000 (UTC+8时区)
            session_utc8.executeNonQueryStatement("INSERT INTO Extract_table(time,t1,s1,S2) values(2025-01-01 08:00:00.000, 't1', 2025-01-01 08:00:00.000, 1)");

            SessionDataSet dataSet = session_utc8.executeQueryStatement("select extract(hour from s1) from Extract_table");
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
            SessionDataSet dataSet = session_utc0.executeQueryStatement("select extract(hour from s1) from Extract_table");
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
            SessionDataSet dataSet = session_utc0.executeQueryStatement("select extract(hour from s1) from Extract_table");
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

    @Test(priority = 120)
    public void testOther() throws IoTDBConnectionException, StatementExecutionException {
        // 1. 测试查询空表
        int actualNum1 = 0;
        SessionDataSet dataSet1 = session.executeQueryStatement("select * from Table_Empty order by time");
        while (dataSet1.hasNext()) {
            dataSet1.next();
            actualNum1++;
        }
        dataSet1.next();
        assert expectNum2 == actualNum1 : "期待查询结果行数：" + expectNum2 + "，实际查询结果行数：" + actualNum1;
        // 2、测试查询不输出第一行，输出第二行及以后的数据，所以实际查询结果行数应该比期待结果行数少一行，需要加1
        int actualNum2 = 0;
        SessionDataSet dataSet2 = session.executeQueryStatement("select * from Table1 order by time");
        dataSet2.next();
        while (dataSet2.hasNext()) {
            dataSet2.next();
            actualNum2++;
        }
        assert expectNum1 == (actualNum2 + 1) : "期待查询结果行数：" + expectNum1 + "，实际查询结果行数：" + (actualNum2 + 1);
        // 3、executeQueryStatement 方法第二种构造方式，timeoutInMs 为-1代表不设置超时时间
        int actualNum3 = 0;
        SessionDataSet dataSet3 = session.executeQueryStatement("select * from Table1 order by time", 6000);
        while (dataSet3.hasNext()) {
            dataSet3.next();
            actualNum3++;
        }
        assert expectNum1 == actualNum3 : "期待查询结果行数：" + expectNum1 + "，实际查询结果行数：" + actualNum3;
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
