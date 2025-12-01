package org.apache.iotdb.api.test.tree.data.insert;


import org.apache.iotdb.api.test.TestInsertUtil;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

/**
 * <p>Title：测试插入数据—异常情况<p/>
 * <p>Describe：测试使用各种接口插入各种数据类型的数据，包括BOOLEAN、INT32、INT64、FLOAT、DOUBLE、TEXT、STRING、BLOB、TIMESTAMP和DATE<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2024/12/29<p/>
 */
public class TestInsertError extends TestInsertUtil {

    /**
     * 测试 insertTablet 的错误情况：元数据和值的类型不一致
     */
    @Test(priority = 10)
    public void testInsertTabletError1() throws IoTDBConnectionException, StatementExecutionException {
        // TODO：初始化createTimeSeries()方法内部变量：方式一：使用局部变量来操作createTimeSeries(变量1，变量2)；方式二：再写个方法用于初始化createTimeSeries()内的所有变量；方式三：使用单例模式或工厂模式
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        // 2、行索引初始化为0
        int rowIndex = 0;
        try {
            // 3、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
                    // 处理null值
                    if (line[i + 1] == null) {
                        continue;
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                        case TEXT:
                        case STRING:
                        case DOUBLE:
                        case INT32:
                        case INT64:
                        case TIMESTAMP:
                        case FLOAT:
                        case BLOB:
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, line[i + 1]);
                            break;
                    }
                }
                // 切换行索引
                rowIndex++;
            }
            // 插入数据
            session.insertTablet(tablet);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "java.lang.IllegalArgumentException".equals(e.getClass().getName()) : "InsertTabletError1 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablet 的错误情况：元数据和值的类型不一致
     */
    @Test(priority = 11)
    public void testInsertAlignedTabletError1() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 测试插入接口
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
                    // 处理null值
                    if (line[i + 1] == null) {
                        continue;
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                        case TEXT:
                        case STRING:
                        case DOUBLE:
                        case INT32:
                        case INT64:
                        case TIMESTAMP:
                        case FLOAT:
                        case BLOB:
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, line[i + 1]);
                            break;
                    }
                }
                // 实例化有效行并切换行索引
                rowIndex++;
            }
            // 插入数据
            session.insertAlignedTablet(tablet);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "java.lang.IllegalArgumentException".equals(e.getClass().getName()) : "InsertAlignedTabletError1 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertTablet 的错误情况：数值超出范围、数据格式不合法、含空值且未使用BitMap参数
     */
    @Test(priority = 12)
    public void testInsertTabletError2() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        // 3、行索引初始化为0
        int rowIndex = 0;
        // 执行测试
        try {
            // 4、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
                    // 处理null值
                    if (line[i + 1] == null) {
                        continue;
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                rowIndex++;
            }
            // 插入数据
            session.insertTablet(tablet);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertTabletError2 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablet 的错误情况：数值超出范围、数据格式不合法、含空值且未使用BitMap参数
     */
    @Test(priority = 13)
    public void testInsertAlignedTabletError2() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        // 2、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 3、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
                    // 处理null值
                    if (line[i + 1] == null) {
                        continue;
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                rowIndex++;
            }
            // 插入数据
            session.insertAlignedTablet(tablet);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedTabletError2 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertTablets 的错误情况：元数据和值的类型不一致
     */
    @Test(priority = 16)
    public void testInsertTabletsError1() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 3、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
                    // 处理null值
                    if (line[i + 1] == null) {
                        continue;
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                        case INT32:
                        case INT64:
                        case TIMESTAMP:
                        case FLOAT:
                        case DOUBLE:
                        case TEXT:
                        case STRING:
                        case BLOB:
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, line[i + 1]);
                            break;
                    }
                }
                rowIndex++;
            }
            tablets.put("1", tablet);
            // 插入数据
            session.insertTablets(tablets);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "java.lang.IllegalArgumentException".equals(e.getClass().getName()) : "InsertTabletsError1 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablets 的错误情况：元数据和值的类型不一致
     */
    @Test(priority = 17)
    public void testInsertAlignedTabletsError1() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 测试插入接口
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 3、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
                    // 处理null值
                    if (line[i + 1] == null) {
                        continue;
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                        case INT32:
                        case INT64:
                        case TIMESTAMP:
                        case FLOAT:
                        case DOUBLE:
                        case TEXT:
                        case STRING:
                        case BLOB:
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, line[i + 1]);
                            break;
                    }
                }
                rowIndex++;
            }
            tablets.put("1", tablet);
            // 插入数据
            session.insertAlignedTablets(tablets);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "java.lang.IllegalArgumentException".equals(e.getClass().getName()) : "InsertAlignedTabletsError1 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertTablets 的错误情况：数值超出范围、数据格式不合法、含空值且未使用BitMap参数
     */
    @Test(priority = 18)
    public void testInsertTabletsError2() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(deviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 3、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
                    // 处理null值
                    if (line[i + 1] == null) {
                        continue;
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                rowIndex++;
            }
            tablets.put("1", tablet);
            // 插入数据
            session.insertTablets(tablets);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertTabletsError2 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertAlignedTablets 的错误情况：数值超出范围、数据格式不合法、含空值且未使用BitMap参数
     */
    @Test(priority = 19)
    public void testInsertAlignedTabletsError2() throws IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        // 1、创建一个新的tablet实例
        Tablet tablet = new Tablet(alignedDeviceId, schemaList, 10);
        Map<String, Tablet> tablets = new HashMap<>();
        // 2、行索引初始化为0
        int rowIndex = 0;
        // 执行
        try {
            // 3、遍历获取的单行数据，进行数据处理
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 获取每行数据
                Object[] line = it.next();
                // 向tablet添加时间戳
                tablet.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < schemaList.size(); i++) {
                    // 处理null值
                    if (line[i + 1] == null) {
                        continue;
                    }
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex,
                                    line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                rowIndex++;
            }
            tablets.put("1", tablet);
            // 插入数据
            session.insertAlignedTablets(tablets);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedTabletsError2 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试 insertRecord 的错误情况:元数据和值的类型不一致
     */
    @Test(priority = 22)
    public void testInsertRecordError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                    case INT32:
                    case INT64:
                    case TIMESTAMP:
                    case FLOAT:
                    case DOUBLE:
                    case TEXT:
                    case STRING:
                    case BLOB:
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertRecord(deviceId, time, measurements, dataTypes, values);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertRecordError1 测试失败-其他错误:" + e;
                return;
            }
            // 清空容器
            values.clear();
        }
    }

    /**
     * 测试 insertRecord 的错误情况:数值超出范围、数据格式不合法、含空值
     */
//    @Test(priority = 23)  TODO：待完善
    public void testInsertRecordError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 执行
            try {
                // 遍历每行逐个物理量的数据
                for (int i = 0; i < measurements.size(); i++) {
                    // 根据数据类型添加值到values中
                    switch (dataTypes.get(i)) {
                        case BOOLEAN:
                            values.add(line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            values.add(line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            values.add(line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            values.add(line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            values.add(line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            values.add(line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            values.add(line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            values.add(line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                // 插入数据
                session.insertRecord(deviceId, time, measurements, dataTypes, values);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertRecordError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            values.clear();
        }
    }

    /**
     * 测试 InsertRecords 的错误情况:元数据和值的类型不一致
     */
    @Test(priority = 24)
    public void testInsertRecordsError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < measurementsList.get(0).size(); i++) {
                // 根据数据类型添加值到values中
                switch (typesList.get(0).get(i)) {
                    case BOOLEAN:
                    case INT32:
                    case INT64:
                    case TIMESTAMP:
                    case FLOAT:
                    case DOUBLE:
                    case TEXT:
                    case STRING:
                    case BLOB:
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesList.add(values);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertRecordsError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试 InsertRecords 的错误情况:数值超出范围、数据格式不合法、含空值
     */
//    @Test(priority = 25)  TODO：待完善
    public void testInsertRecordsError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 执行
            try {
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < measurementsList.get(0).size(); i++) {
                    // 根据数据类型添加值到values中
                    switch (typesList.get(0).get(i)) {
                        case BOOLEAN:
                            values.add(line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            values.add(line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            values.add(line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            values.add(line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            values.add(line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            values.add(line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            values.add(line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            values.add(line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
                // 插入数据
                session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertRecordsError2 测试失败-其他错误:" + e;
            }
            // 清理容器
            values.clear();
            valuesList.clear();
            times.clear();
        }
    }

    /**
     * 测试 InsertRecordsOfOneDevice 的错误情况:元数据和值的类型不一致
     */
    @Test(priority = 26)
    public void testInsertRecordsOfOneDeviceError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                    case INT32:
                    case INT64:
                    case TIMESTAMP:
                    case FLOAT:
                    case DOUBLE:
                    case TEXT:
                    case STRING:
                    case BLOB:
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesList.add(values);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertRecordsOfOneDeviceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试 InsertRecordsOfOneDevice 的错误情况:数值超出范围、数据格式不合法、含空值
     */
//    @Test(priority = 27)  TODO：待完善
    public void testInsertRecordsOfOneDeviceError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 执行
            try {
                // 遍历每行逐个物理量的数据
                for (int i = 0; i < measurements.size(); i++) {
                    // 根据数据类型添加值到values中
                    switch (dataTypes.get(i)) {
                        case BOOLEAN:
                            values.add(line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            values.add(line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            values.add(line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            values.add(line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            values.add(line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            values.add(line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            values.add(line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            values.add(line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
                // 插入数据
                session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertRecordsOfOneDeviceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            values.clear();
            valuesList.clear();
            times.clear();
        }
    }

    /**
     * 测试 insertAlignedRecord 的错误情况:元数据和值的类型不一致
     */
    @Test(priority = 28)
    public void testInsertAlignedRecordError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                    case INT32:
                    case INT64:
                    case TIMESTAMP:
                    case FLOAT:
                    case DOUBLE:
                    case TEXT:
                    case STRING:
                    case BLOB:
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecord(alignedDeviceId, time, measurements, dataTypes, values);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertAlignedRecordError1 测试失败-其他错误:" + e;
                return;
            }
            // 清空容器
            values.clear();
        }
    }

    /**
     * 测试 insertAlignedRecord 的错误情况:数值超出范围、数据格式不合法、含空值
     */
//    @Test(priority = 29)  TODO：待完善
    public void testInsertAlignedRecordError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 执行
            try {
                // 遍历每行逐个物理量的数据
                for (int i = 0; i < measurements.size(); i++) {
                    // 根据数据类型添加值到values中
                    switch (dataTypes.get(i)) {
                        case BOOLEAN:
                            values.add(line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            values.add(line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            values.add(line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            values.add(line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            values.add(line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            values.add(line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            values.add(line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            values.add(line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                // 插入数据
                session.insertAlignedRecord(alignedDeviceId, time, measurements, dataTypes, values);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedRecordError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            values.clear();
        }
    }

    /**
     * 测试 InsertAlignedRecords 的错误情况:元数据和值的类型不一致
     */
    @Test(priority = 30)
    public void testInsertAlignedRecordsError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < measurementsList.get(0).size(); i++) {
                // 根据数据类型添加值到values中
                switch (typesList.get(0).get(i)) {
                    case BOOLEAN:
                    case INT32:
                    case INT64:
                    case TIMESTAMP:
                    case FLOAT:
                    case DOUBLE:
                    case TEXT:
                    case STRING:
                    case BLOB:
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesList.add(values);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertAlignedRecordsError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试 InsertAlignedRecords 的错误情况:数值超出范围、数据格式不合法、含空值
     */
//    @Test(priority = 31)  TODO：待完善
    public void testInsertAlignedRecordsError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 执行
            try {
                // 遍历schemaList，为每列添加数据
                for (int i = 0; i < measurementsList.get(0).size(); i++) {
                    // 根据数据类型添加值到values中
                    switch (typesList.get(0).get(i)) {
                        case BOOLEAN:
                            values.add(line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            values.add(line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            values.add(line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            values.add(line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            values.add(line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            values.add(line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            values.add(line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            values.add(line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedRecordsError2 测试失败-其他错误:" + e;
            }
            // 清理容器
            values.clear();
            valuesList.clear();
            times.clear();
        }
    }

    /**
     * 测试 InsertAlignedRecords 的错误情况:元数据和值的类型不一致
     */
    @Test(priority = 32)
    public void testInsertAlignedRecordsOfOneDeviceError1() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                    case INT32:
                    case INT64:
                    case TIMESTAMP:
                    case FLOAT:
                    case DOUBLE:
                    case TEXT:
                    case STRING:
                    case BLOB:
                    case DATE:
                        values.add(line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesList.add(values);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecordsOfOneDevice(alignedDeviceId, times, measurementsList, typesList, valuesList);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.ClassCastException".equals(e.getClass().getName()) : "InsertAlignedRecordsOfOneDeviceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试 InsertAlignedRecords 的错误情况:数值超出范围、数据格式不合法、含空值
     */
//    @Test(priority = 33)  TODO：待完善
    public void testInsertAlignedRecordsOfOneDeviceError2() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 准备环境和数据
        createAlignedTimeSeries();
        List<Object> values = new ArrayList<>(10);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 执行
            try {
                // 遍历每行逐个物理量的数据
                for (int i = 0; i < measurements.size(); i++) {
                    // 根据数据类型添加值到values中
                    switch (dataTypes.get(i)) {
                        case BOOLEAN:
                            values.add(line[i + 1] != null && Boolean.parseBoolean((String) line[i + 1]));
                            break;
                        case INT32:
                            values.add(line[i + 1] == null ? 1 : Integer.parseInt((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            values.add(line[i + 1] == null ? 1L : Long.parseLong((String) line[i + 1]));
                            break;
                        case FLOAT:
                            values.add(line[i + 1] == null ? 1.01f : Float.parseFloat((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            values.add(line[i + 1] == null ? 1.0 : Double.parseDouble((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            values.add(line[i + 1] == null ? "stringnull" : line[i + 1]);
                            break;
                        case BLOB:
                            values.add(line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            values.add(line[i + 1] == null ? LocalDate.parse("2024-07-25") : LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
                // 添加值
                valuesList.add(values);
                times.add(Long.valueOf((String) line[0]));
                // 插入数据
                session.insertAlignedRecordsOfOneDevice(alignedDeviceId, times, measurementsList, typesList, valuesList);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "java.lang.NumberFormatException".equals(e.getClass().getName()) || "java.time.format.DateTimeParseException".equals(e.getClass().getName()) : "InsertAlignedRecordsOfOneDeviceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            values.clear();
            valuesList.clear();
            times.clear();
        }
    }


    /**
     * 测试带推断类型的 insertRecord 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 34)
    public void testInsertRecordInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertRecord(deviceId, time, measurements, valuesInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertRecordInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 InsertRecords 不合法的错误情况：数值超出范围、含空值
     */
//    @Test(priority = 35)  TODO：待完善
    public void testInsertRecordInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertRecord(deviceId, time, measurements, valuesInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertRecordInferenceError2 测试失败-其他错误:" + e;
                return;
            }
            // 清空容器
            valuesInference.clear();
        }
    }

    /**
     * 测试带推断类型的 InsertRecords 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 36)
    public void testInsertRecordsInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < measurementsList.get(0).size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, valuesListInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertRecordsInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 insertRecord 不合法的错误情况：数值超出范围、含空值
     */
//    @Test(priority = 37) TODO：待完善
    public void testInsertRecordsInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < measurementsList.get(0).size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertRecords(deviceIds, times, measurementsList, valuesListInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertRecordsInferenceError2 测试失败-其他错误:" + e;
            }
            // 初始化
            valuesInference.clear();
            valuesListInference.clear();
            times.clear();
        }
    }

    /**
     * 测试带推断类型的 InsertStringRecordsOfOneDevice 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 38)
    public void testInsertStringRecordsOfOneDeviceInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesListInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertStringRecordsOfOneDeviceInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 InsertStringRecordsOfOneDevice 不合法的错误情况：数值超出范围、含空值
     */
//    @Test(priority = 39) TODO：待完善
    public void testInsertStringRecordsOfOneDeviceInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的非对齐时间序列
        createTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesListInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertStringRecordsOfOneDeviceInferenceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            valuesInference.clear();
            valuesListInference.clear();
            times.clear();
        }
    }

    /**
     * 测试带推断类型的 insertAlignedRecord 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 40)
    public void testInsertAlignedRecordInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createAlignedTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecord(alignedDeviceId, time, measurements, valuesInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedRecordInferenceError1 测试失败-其他错误:" + e;
            }
        }
    }

    /**
     * 测试带推断类型的 InsertAlignedRecords 不合法的错误情况：数值超出范围、含空值
     */
//    @Test(priority = 41)  TODO：待完善
    public void testInsertAlignedRecordInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createAlignedTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecord(alignedDeviceId, time, measurements, valuesInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedRecordInferenceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            valuesInference.clear();
        }
    }

    /**
     * 测试带推断类型的 InsertAlignedRecords 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 42)
    public void testInsertAlignedRecordsInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建时间序列
        createAlignedTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < measurementsList.get(0).size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, valuesListInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedRecordsInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 insertAlignedRecord 不合法的错误情况：数值超出范围、含空值
     */
//    @Test(priority = 43)  TODO：待完善
    public void testInsertAlignedRecordsInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createAlignedTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 遍历schemaList，为每列添加数据
            for (int i = 0; i < measurementsList.get(0).size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedRecords(deviceIds, times, measurementsList, valuesListInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedRecordsInferenceError2 测试失败-其他错误:" + e;
            }
            // 初始化
            valuesInference.clear();
            valuesListInference.clear();
            times.clear();
        }
    }

    /**
     * 测试带推断类型的 InsertAlignedStringRecordsOfOneDevice 不合法的错误情况：对应的TS数据类型不一致
     */
    @Test(priority = 44)
    public void testInsertAlignedStringRecordsOfOneDeviceInferenceError1() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 创建对齐时间序列
        createAlignedTimeSeries();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedStringRecordsOfOneDevice(alignedDeviceId, times, measurementsList, valuesListInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedStringRecordsOfOneDeviceInferenceError1 测试失败-其他错误:" + e;
                return;
            }
        }
    }

    /**
     * 测试带推断类型的 InsertAlignedStringRecordsOfOneDevice 不合法的错误情况：数值超出范围、含空值
     */
//    @Test(priority = 45)  TODO：待完善
    public void testInsertAlignedStringRecordsOfOneDeviceInferenceError2() throws IoTDBConnectionException, IOException, StatementExecutionException {
        // 创建推断的时间序列
        createAlignedTimeSeriesInference();
        List<String> valuesInference = new ArrayList<>(10);
        List<List<String>> valuesListInference = new ArrayList<>(1);
        List<Long> times = new ArrayList<>(10);
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        valuesInference.add(line[i + 1] == null ? "false" : (String) line[i + 1]);
                        break;
                    case INT32:
                        valuesInference.add(line[i + 1] == null ? "1" : (String) line[i + 1]);
                        break;
                    case INT64:
                    case TIMESTAMP:
                        valuesInference.add(line[i + 1] == null ? "1L" : line[i + 1] + "L");
                        break;
                    case FLOAT:
                        valuesInference.add(line[i + 1] == null ? "1.01f" : line[i + 1] + "F");
                        break;
                    case DOUBLE:
                        valuesInference.add(line[i + 1] == null ? "1.0" : (String) line[i + 1]);
                        break;
                    case TEXT:
                    case STRING:
                        valuesInference.add(line[i + 1] == null ? "stringnull" : (String) line[i + 1]);
                        break;
                    case BLOB:
                        valuesInference.add("X'696f74646236'");
                        break;
                    case DATE:
                        valuesInference.add(line[i + 1] == null ? "2024-07-25" : (String) line[i + 1]);
                        break;
                }
            }
            // 添加值
            valuesListInference.add(valuesInference);
            times.add(Long.valueOf((String) line[0]));
            // 执行
            try {
                // 插入数据
                session.insertAlignedStringRecordsOfOneDevice(alignedDeviceId, times, measurementsList, valuesListInference);
                assert false : "Expecting an error exception, but running normally";
            } catch (Exception e) {
                assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "InsertAlignedStringRecordsOfOneDeviceInferenceError2 测试失败-其他错误:" + e;
            }
            // 清空容器
            valuesInference.clear();
            valuesListInference.clear();
            times.clear();
        }
    }

    /**
     * 测试TS不合法的错误情况：创建部分数据类型的时间序列不支持的编码和压缩类型
     */
    @Test(priority = 46)
    public void testTSError() throws IoTDBConnectionException, StatementExecutionException {
        // 设备名称
        String deviceId = database + ".fdq";

        // 存储路径
        List<String> paths = new ArrayList<>(10);
        // 存储数据类型
        List<TSDataType> dataTypes = new ArrayList<>(10);
        // 物理量类型信息
        Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(10);
        // 检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }
        // 创建数据库
        session.createDatabase(database);
        // 添加不同的数据类型
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int32", TSDataType.INT32);
        measureTSTypeInfos.put("s_int64", TSDataType.INT64);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);
        measureTSTypeInfos.put("s_string", TSDataType.STRING);
        measureTSTypeInfos.put("s_blob", TSDataType.BLOB);
        measureTSTypeInfos.put("s_timestamp", TSDataType.TIMESTAMP);
        measureTSTypeInfos.put("s_date", TSDataType.DATE);
        // 遍历measureTSTypeInfos，为时间序列添加路径、测量和数据类型
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId + "." + key);
            dataTypes.add(value);
        });
        // 为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.RLBE);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 测试
        try {
            // 创建多个非对齐时间序列
            session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                    null, null, null, null);
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert "org.apache.iotdb.rpc.BatchExecutionException".equals(e.getClass().getName()) : "TSError 测试失败：其他错误" + e.getMessage();
        }
    }

    /**
     * 测试 insertTablet 的错误情况：值和数据类型的类型不一致
     */
    @Test(priority = 47)
    public void testInsertRecordError3() {
        // 数据类型为 boolean 时值为非 boolean 的异常
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(1, true, true, true, true));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, 1L, true, true, true));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, 1.1F, true, true));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, true, 1.1, true));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, true, true, "true"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, true, true, new Binary("true", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, true, true, LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 int32 时值为非 int32 的异常
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(true, 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(1, 2L, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(1, 2, 1.1F, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(1.1, 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList("1", 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(LocalDate.of(1970, 1, 1), 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 int64 或 timestamp 时值为非 int64 或 timestamp 的异常
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(true, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(1, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(1.1F, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(1.1, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList("1", 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(true, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(1, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(1.1F, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(1.1, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList("1", 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 float 时值为非 float 的异常
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(true, 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(1, 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(1L, 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(1.1, 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList("1.1F", 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(new Binary("1.1F", StandardCharsets.UTF_8), 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(LocalDate.of(1970, 1, 1), 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 double 时值为非 double 的异常
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(true, 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(1, 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(1L, 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(1.1F, 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList("1.1F", 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(new Binary("1.1F", StandardCharsets.UTF_8), 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(LocalDate.of(1970, 1, 1), 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 text 或 string 时值为非 text 或 string 的异常
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(true, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(1, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(1L, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(1.1F, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(1.1, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        // Blob：String和Text类型支持binary数据
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "text2", "text3", "text4", "text5"));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(LocalDate.of(1970, 1, 1), "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(true, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(1, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(1L, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(1.1F, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(1.1, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        // Blob：String和Text类型支持binary数据
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "string2", "string3", "string4", "string5"));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(LocalDate.of(1970, 1, 1), "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Boolean
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(true, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Int32
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Int64、timestamp
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(1L, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Float
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(1.1F, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Double
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(1.1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Text、String
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList("blob1", new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Date
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(LocalDate.of(1970, 1, 1), new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Boolean
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(true, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Int32
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Int64、Timestamp
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(1L, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Float
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(1.1F, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Double
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(1.1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Text、String
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList("1970.1.1", LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Blob
        try {
            session.insertRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(new Binary("1970.1.1", StandardCharsets.UTF_8), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
    }

    /**
     * 测试 insertAlignedTablet 的错误情况：值和数据类型的类型不一致
     */
    @Test(priority = 48)
    public void testInsertAlignedRecordError3() {
        // 数据类型为 boolean 时值为非 boolean 的异常
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(1, true, true, true, true));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, 1L, true, true, true));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, 1.1F, true, true));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, true, 1.1, true));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, true, true, "true"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, true, true, new Binary("true", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
                    Arrays.asList(true, true, true, true, LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 int32 时值为非 int32 的异常
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(true, 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(1, 2L, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(1, 2, 1.1F, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(1.1, 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList("1", 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
                    Arrays.asList(LocalDate.of(1970, 1, 1), 2, 3, 4, 5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 int64 或 timestamp 时值为非 int64 或 timestamp 的异常
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(true, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(1, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(1.1F, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(1.1, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList("1", 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
                    Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(true, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(1, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(1.1F, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(1.1, 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList("1", 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP),
                    Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 float 时值为非 float 的异常
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(true, 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(1, 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(1L, 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(1.1, 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList("1.1F", 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(new Binary("1.1F", StandardCharsets.UTF_8), 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT),
                    Arrays.asList(LocalDate.of(1970, 1, 1), 2.2F, 3.3F, 4.4F, 5.5F));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 double 时值为非 double 的异常
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(true, 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(1, 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(1L, 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(1.1F, 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList("1.1F", 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(new Binary("1.1F", StandardCharsets.UTF_8), 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE),
                    Arrays.asList(LocalDate.of(1970, 1, 1), 2.2, 3.3, 4.4, 5.5));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // 数据类型为 text 或 string 时值为非 text 或 string 的异常
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(true, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(1, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(1L, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(1.1F, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(1.1, "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        // Blob：String和Text类型支持binary数据
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "text2", "text3", "text4", "text5"));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(LocalDate.of(1970, 1, 1), "text2", "text3", "text4", "text5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(true, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(1, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(1L, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(1.1F, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(1.1, "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }
        // Blob：String和Text类型支持binary数据
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT),
                    Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "string2", "string3", "string4", "string5"));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
                    Arrays.asList(LocalDate.of(1970, 1, 1), "string2", "string3", "string4", "string5"));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Boolean
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(true, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Int32
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Int64、timestamp
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(1L, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Float
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(1.1F, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Double
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(1.1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Text、String
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList("blob1", new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB),
                    Arrays.asList(LocalDate.of(1970, 1, 1), new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(true, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、Timestamp
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(1L, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(1.1F, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(1.1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList("1970.1.1", LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertAlignedRecord(
                    "root.db1.d1",
                    0,
                    Arrays.asList("s1", "s2", "s3", "s4", "s5"),
                    Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE),
                    Arrays.asList(new Binary("1970.1.1", StandardCharsets.UTF_8), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }
    }


    /**
     * 测试 insertTablet 的错误情况：值和数据类型的类型不一致
     */
    @Test(priority = 49)
    public void testInsertRecordsError3() {
        // int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(1, true, true, true, true), Arrays.asList(1, true, true, true, true), Arrays.asList(1, true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // int64、 timestamp
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(1L, true, true, true, true), Arrays.asList(1L, true, true, true, true), Arrays.asList(1L, true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(1.1F, true, true, true, true), Arrays.asList(1.1F, true, true, true, true), Arrays.asList(1.1F, true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(1.1, true, true, true, true), Arrays.asList(1.1, true, true, true, true), Arrays.asList(1.1, true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // text、 string
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList("true", true, true, true, true), Arrays.asList("true", true, true, true, true), Arrays.asList("true", true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // blob
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(new Binary("true", StandardCharsets.UTF_8), true, true, true, true), Arrays.asList(new Binary("true", StandardCharsets.UTF_8), true, true, true, true), Arrays.asList(new Binary("true", StandardCharsets.UTF_8), true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), true, true, true, true), Arrays.asList(LocalDate.of(1970, 1, 1), true, true, true, true), Arrays.asList(LocalDate.of(1970, 1, 1), true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(true, 2, 3, 4, 5), Arrays.asList(true, 2, 3, 4, 5), Arrays.asList(true, 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // long、timestamp
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(1L, 2, 3, 4, 5), Arrays.asList(1L, 2, 3, 4, 5), Arrays.asList(1L, 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(1.1F, 2, 3, 4, 5), Arrays.asList(1.1F, 2, 3, 4, 5), Arrays.asList(1.1F, 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(1.1, 2, 3, 4, 5), Arrays.asList(1.1, 2, 3, 4, 5), Arrays.asList(1.1, 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // text、string
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList("1", 2, 3, 4, 5), Arrays.asList("1", 2, 3, 4, 5), Arrays.asList("1", 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // blob
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2, 3, 4, 5), Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2, 3, 4, 5), Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), 2, 3, 4, 5), Arrays.asList(LocalDate.of(1970, 1, 1), 2, 3, 4, 5), Arrays.asList(LocalDate.of(1970, 1, 1), 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(true, 2L, 3L, 4L, 5L), Arrays.asList(true, 2L, 3L, 4L, 5L), Arrays.asList(true, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(1, 2L, 3L, 4L, 5L), Arrays.asList(1, 2L, 3L, 4L, 5L), Arrays.asList(1, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(1.1F, 2L, 3L, 4L, 5L), Arrays.asList(1.1F, 2L, 3L, 4L, 5L), Arrays.asList(1.1F, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(1.1, 2L, 3L, 4L, 5L), Arrays.asList(1.1, 2L, 3L, 4L, 5L), Arrays.asList(1.1, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList("1L", 2L, 3L, 4L, 5L), Arrays.asList("1L", 2L, 3L, 4L, 5L), Arrays.asList("1L", 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L), Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L), Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L), Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L), Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(true, 2L, 3L, 4L, 5L), Arrays.asList(true, 2L, 3L, 4L, 5L), Arrays.asList(true, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(1, 2L, 3L, 4L, 5L), Arrays.asList(1, 2L, 3L, 4L, 5L), Arrays.asList(1, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(1.1F, 2L, 3L, 4L, 5L), Arrays.asList(1.1F, 2L, 3L, 4L, 5L), Arrays.asList(1.1F, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(1.1, 2L, 3L, 4L, 5L), Arrays.asList(1.1, 2L, 3L, 4L, 5L), Arrays.asList(1.1, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList("1L", 2L, 3L, 4L, 5L), Arrays.asList("1L", 2L, 3L, 4L, 5L), Arrays.asList("1L", 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L), Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L), Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L), Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L), Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(true, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(true, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(true, 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(1, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1, 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(1L, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1L, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1L, 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(1.1, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1.1, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1.1, 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList("1F", 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList("1F", 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList("1F", 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(new Binary("1F", StandardCharsets.UTF_8), 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(new Binary("1F", StandardCharsets.UTF_8), 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(new Binary("1F", StandardCharsets.UTF_8), 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(LocalDate.of(1970, 1, 1), 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(LocalDate.of(1970, 1, 1), 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(true, 2.2, 3.3, 4.4, 5.5), Arrays.asList(true, 2.2, 3.3, 4.4, 5.5), Arrays.asList(true, 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(1, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1, 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(1L, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1L, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1L, 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(1F, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1F, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1F, 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList("1.1", 2.2, 3.3, 4.4, 5.5), Arrays.asList("1.1", 2.2, 3.3, 4.4, 5.5), Arrays.asList("1.1", 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(new Binary("1.1", StandardCharsets.UTF_8), 2.2, 3.3, 4.4, 5.5), Arrays.asList(new Binary("1.1", StandardCharsets.UTF_8), 2.2, 3.3, 4.4, 5.5), Arrays.asList(new Binary("1.1", StandardCharsets.UTF_8), 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), 2.2, 3.3, 4.4, 5.5), Arrays.asList(LocalDate.of(1970, 1, 1), 2.2, 3.3, 4.4, 5.5), Arrays.asList(LocalDate.of(1970, 1, 1), 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(true, "text2", "text3", "text4", "text5"), Arrays.asList(true, "text2", "text3", "text4", "text5"), Arrays.asList(true, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(1, "text2", "text3", "text4", "text5"), Arrays.asList(1, "text2", "text3", "text4", "text5"), Arrays.asList(1, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、Timestamp
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(1L, "text2", "text3", "text4", "text5"), Arrays.asList(1L, "text2", "text3", "text4", "text5"), Arrays.asList(1L, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(1.1F, "text2", "text3", "text4", "text5"), Arrays.asList(1.1F, "text2", "text3", "text4", "text5"), Arrays.asList(1.1F, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(1.1, "text2", "text3", "text4", "text5"), Arrays.asList(1.1, "text2", "text3", "text4", "text5"), Arrays.asList(1.1, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob：String和Text类型支持binary数据
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "text2", "text3", "text4", "text5"), Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "text2", "text3", "text4", "text5"), Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "text2", "text3", "text4", "text5")));
            session.executeNonQueryStatement("delete database root.**");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }

        // Date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), "text2", "text3", "text4", "text5"), Arrays.asList(LocalDate.of(1970, 1, 1), "text2", "text3", "text4", "text5"), Arrays.asList(LocalDate.of(1970, 1, 1), "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(true, "string2", "string3", "string4", "string5"), Arrays.asList(true, "string2", "string3", "string4", "string5"), Arrays.asList(true, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(1, "string2", "string3", "string4", "string5"), Arrays.asList(1, "string2", "string3", "string4", "string5"), Arrays.asList(1, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、Timestamp
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(1L, "string2", "string3", "string4", "string5"), Arrays.asList(1L, "string2", "string3", "string4", "string5"), Arrays.asList(1L, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(1.1F, "string2", "string3", "string4", "string5"), Arrays.asList(1.1F, "string2", "string3", "string4", "string5"), Arrays.asList(1.1F, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(1.1, "string2", "string3", "string4", "string5"), Arrays.asList(1.1, "string2", "string3", "string4", "string5"), Arrays.asList(1.1, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob：String和Text类型支持binary数据
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(new Binary("string1", StandardCharsets.UTF_8), "string2", "string3", "string4", "string5"), Arrays.asList(new Binary("string1", StandardCharsets.UTF_8), "string2", "string3", "string4", "string5"), Arrays.asList(new Binary("string1", StandardCharsets.UTF_8), "string2", "string3", "string4", "string5")));
            session.executeNonQueryStatement("delete database root.**");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }

        // Date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), "string2", "string3", "string4", "string5"), Arrays.asList(LocalDate.of(1970, 1, 1), "string2", "string3", "string4", "string5"), Arrays.asList(LocalDate.of(1970, 1, 1), "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(true, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(true, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(true, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、timestamp
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(1L, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1L, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1L, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(1.1F, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1.1F, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1.1F, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(1.1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1.1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1.1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList("blob1", new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList("blob1", new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList("blob1", new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(LocalDate.of(1970, 1, 1), new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(LocalDate.of(1970, 1, 1), new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(true, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(false, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(true, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(2, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(3, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、Timestamp
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(1L, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(2L, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(3L, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(1.1F, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(1.1F, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(1.1F, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(1.1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(1.1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(1.1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList("1970.1.1", LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList("1970.1.1", LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList("1970.1.1", LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(new Binary("1970.1.1", StandardCharsets.UTF_8), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(new Binary("1970.1.1", StandardCharsets.UTF_8), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(new Binary("1970.1.1", StandardCharsets.UTF_8), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }
    }

    /**
     * 测试 insertAlignedRecords 的错误情况：值和数据类型的类型不一致
     */
    @Test(priority = 50)
    public void testInsertAlignedRecordsError3() {
        // int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(1, true, true, true, true), Arrays.asList(1, true, true, true, true), Arrays.asList(1, true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // int64、 timestamp
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(1L, true, true, true, true), Arrays.asList(1L, true, true, true, true), Arrays.asList(1L, true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(1.1F, true, true, true, true), Arrays.asList(1.1F, true, true, true, true), Arrays.asList(1.1F, true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(1.1, true, true, true, true), Arrays.asList(1.1, true, true, true, true), Arrays.asList(1.1, true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // text、 string
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList("true", true, true, true, true), Arrays.asList("true", true, true, true, true), Arrays.asList("true", true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // blob
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(new Binary("true", StandardCharsets.UTF_8), true, true, true, true), Arrays.asList(new Binary("true", StandardCharsets.UTF_8), true, true, true, true), Arrays.asList(new Binary("true", StandardCharsets.UTF_8), true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN), Arrays.asList(TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN, TSDataType.BOOLEAN)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), true, true, true, true), Arrays.asList(LocalDate.of(1970, 1, 1), true, true, true, true), Arrays.asList(LocalDate.of(1970, 1, 1), true, true, true, true)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(true, 2, 3, 4, 5), Arrays.asList(true, 2, 3, 4, 5), Arrays.asList(true, 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // long、timestamp
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(1L, 2, 3, 4, 5), Arrays.asList(1L, 2, 3, 4, 5), Arrays.asList(1L, 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(1.1F, 2, 3, 4, 5), Arrays.asList(1.1F, 2, 3, 4, 5), Arrays.asList(1.1F, 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(1.1, 2, 3, 4, 5), Arrays.asList(1.1, 2, 3, 4, 5), Arrays.asList(1.1, 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // text、string
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList("1", 2, 3, 4, 5), Arrays.asList("1", 2, 3, 4, 5), Arrays.asList("1", 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // blob
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2, 3, 4, 5), Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2, 3, 4, 5), Arrays.asList(new Binary("1", StandardCharsets.UTF_8), 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32), Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), 2, 3, 4, 5), Arrays.asList(LocalDate.of(1970, 1, 1), 2, 3, 4, 5), Arrays.asList(LocalDate.of(1970, 1, 1), 2, 3, 4, 5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(true, 2L, 3L, 4L, 5L), Arrays.asList(true, 2L, 3L, 4L, 5L), Arrays.asList(true, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(1, 2L, 3L, 4L, 5L), Arrays.asList(1, 2L, 3L, 4L, 5L), Arrays.asList(1, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(1.1F, 2L, 3L, 4L, 5L), Arrays.asList(1.1F, 2L, 3L, 4L, 5L), Arrays.asList(1.1F, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(1.1, 2L, 3L, 4L, 5L), Arrays.asList(1.1, 2L, 3L, 4L, 5L), Arrays.asList(1.1, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList("1L", 2L, 3L, 4L, 5L), Arrays.asList("1L", 2L, 3L, 4L, 5L), Arrays.asList("1L", 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L), Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L), Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64), Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L), Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L), Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(true, 2L, 3L, 4L, 5L), Arrays.asList(true, 2L, 3L, 4L, 5L), Arrays.asList(true, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(1, 2L, 3L, 4L, 5L), Arrays.asList(1, 2L, 3L, 4L, 5L), Arrays.asList(1, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(1.1F, 2L, 3L, 4L, 5L), Arrays.asList(1.1F, 2L, 3L, 4L, 5L), Arrays.asList(1.1F, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(1.1, 2L, 3L, 4L, 5L), Arrays.asList(1.1, 2L, 3L, 4L, 5L), Arrays.asList(1.1, 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList("1L", 2L, 3L, 4L, 5L), Arrays.asList("1L", 2L, 3L, 4L, 5L), Arrays.asList("1L", 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L), Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L), Arrays.asList(new Binary("1L", StandardCharsets.UTF_8), 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP), Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP, TSDataType.TIMESTAMP)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L), Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L), Arrays.asList(LocalDate.of(1900, 1, 1), 2L, 3L, 4L, 5L)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(true, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(true, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(true, 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();
        }

        // Int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(1, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1, 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(1L, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1L, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1L, 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(1.1, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1.1, 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(1.1, 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList("1F", 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList("1F", 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList("1F", 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(new Binary("1F", StandardCharsets.UTF_8), 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(new Binary("1F", StandardCharsets.UTF_8), 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(new Binary("1F", StandardCharsets.UTF_8), 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT), Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(LocalDate.of(1970, 1, 1), 2.2F, 3.3F, 4.4F, 5.5F), Arrays.asList(LocalDate.of(1970, 1, 1), 2.2F, 3.3F, 4.4F, 5.5F)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(true, 2.2, 3.3, 4.4, 5.5), Arrays.asList(true, 2.2, 3.3, 4.4, 5.5), Arrays.asList(true, 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(1, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1, 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(1L, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1L, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1L, 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(1F, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1F, 2.2, 3.3, 4.4, 5.5), Arrays.asList(1F, 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList("1.1", 2.2, 3.3, 4.4, 5.5), Arrays.asList("1.1", 2.2, 3.3, 4.4, 5.5), Arrays.asList("1.1", 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(new Binary("1.1", StandardCharsets.UTF_8), 2.2, 3.3, 4.4, 5.5), Arrays.asList(new Binary("1.1", StandardCharsets.UTF_8), 2.2, 3.3, 4.4, 5.5), Arrays.asList(new Binary("1.1", StandardCharsets.UTF_8), 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE), Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), 2.2, 3.3, 4.4, 5.5), Arrays.asList(LocalDate.of(1970, 1, 1), 2.2, 3.3, 4.4, 5.5), Arrays.asList(LocalDate.of(1970, 1, 1), 2.2, 3.3, 4.4, 5.5)));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(true, "text2", "text3", "text4", "text5"), Arrays.asList(true, "text2", "text3", "text4", "text5"), Arrays.asList(true, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(1, "text2", "text3", "text4", "text5"), Arrays.asList(1, "text2", "text3", "text4", "text5"), Arrays.asList(1, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、Timestamp
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(1L, "text2", "text3", "text4", "text5"), Arrays.asList(1L, "text2", "text3", "text4", "text5"), Arrays.asList(1L, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(1.1F, "text2", "text3", "text4", "text5"), Arrays.asList(1.1F, "text2", "text3", "text4", "text5"), Arrays.asList(1.1F, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(1.1, "text2", "text3", "text4", "text5"), Arrays.asList(1.1, "text2", "text3", "text4", "text5"), Arrays.asList(1.1, "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob：String和Text类型支持binary数据
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "text2", "text3", "text4", "text5"), Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "text2", "text3", "text4", "text5"), Arrays.asList(new Binary("text1", StandardCharsets.UTF_8), "text2", "text3", "text4", "text5")));
            session.executeNonQueryStatement("delete database root.**");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }

        // Date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT), Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), "text2", "text3", "text4", "text5"), Arrays.asList(LocalDate.of(1970, 1, 1), "text2", "text3", "text4", "text5"), Arrays.asList(LocalDate.of(1970, 1, 1), "text2", "text3", "text4", "text5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(true, "string2", "string3", "string4", "string5"), Arrays.asList(true, "string2", "string3", "string4", "string5"), Arrays.asList(true, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(1, "string2", "string3", "string4", "string5"), Arrays.asList(1, "string2", "string3", "string4", "string5"), Arrays.asList(1, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、Timestamp
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(1L, "string2", "string3", "string4", "string5"), Arrays.asList(1L, "string2", "string3", "string4", "string5"), Arrays.asList(1L, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(1.1F, "string2", "string3", "string4", "string5"), Arrays.asList(1.1F, "string2", "string3", "string4", "string5"), Arrays.asList(1.1F, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(1.1, "string2", "string3", "string4", "string5"), Arrays.asList(1.1, "string2", "string3", "string4", "string5"), Arrays.asList(1.1, "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob：String和Text类型支持binary数据
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(new Binary("string1", StandardCharsets.UTF_8), "string2", "string3", "string4", "string5"), Arrays.asList(new Binary("string1", StandardCharsets.UTF_8), "string2", "string3", "string4", "string5"), Arrays.asList(new Binary("string1", StandardCharsets.UTF_8), "string2", "string3", "string4", "string5")));
            session.executeNonQueryStatement("delete database root.**");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }

        // Date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING), Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.STRING)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), "string2", "string3", "string4", "string5"), Arrays.asList(LocalDate.of(1970, 1, 1), "string2", "string3", "string4", "string5"), Arrays.asList(LocalDate.of(1970, 1, 1), "string2", "string3", "string4", "string5")));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(true, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(true, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(true, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、timestamp
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(1L, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1L, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1L, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(1.1F, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1.1F, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1.1F, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(1.1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1.1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(1.1, new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList("blob1", new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList("blob1", new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList("blob1", new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Date
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB), Arrays.asList(TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB, TSDataType.BLOB)),
                    Arrays.asList(Arrays.asList(LocalDate.of(1970, 1, 1), new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(LocalDate.of(1970, 1, 1), new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8)), Arrays.asList(LocalDate.of(1970, 1, 1), new Binary("blob2", StandardCharsets.UTF_8), new Binary("blob3", StandardCharsets.UTF_8), new Binary("blob4", StandardCharsets.UTF_8), new Binary("blob5", StandardCharsets.UTF_8))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Boolean
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(true, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(false, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(true, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int32
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(2, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(3, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Int64、Timestamp
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(1L, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(2L, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(3L, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Float
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(1.1F, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(1.1F, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(1.1F, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Double
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(1.1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(1.1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(1.1, LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Text、String
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList("1970.1.1", LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList("1970.1.1", LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList("1970.1.1", LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }

        // Blob
        try {
            session.insertAlignedRecords(
                    Arrays.asList("root.db1.d1", "root.db1.d2", "root.db1.d3"),
                    Arrays.asList(0L, 1L, 2L),
                    Arrays.asList(Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5"), Arrays.asList("s1", "s2", "s3", "s4", "s5")),
                    Arrays.asList(Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE), Arrays.asList(TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE, TSDataType.DATE)),
                    Arrays.asList(Arrays.asList(new Binary("1970.1.1", StandardCharsets.UTF_8), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(new Binary("1970.1.1", StandardCharsets.UTF_8), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1)), Arrays.asList(new Binary("1970.1.1", StandardCharsets.UTF_8), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1), LocalDate.of(1970, 1, 1))));
            assert false : "Expecting an error exception, but running normally";
        } catch (Exception e) {
            assert e instanceof ClassCastException : "Unexpected exception type, expect: ClassCastException, actual: " + e.getCause();

        }
    }
}
