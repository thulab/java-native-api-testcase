package org.apache.iotdb.api.test.tree.insert;

import org.apache.iotdb.api.test.BaseTestSuite_TreeModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.*;

/**
 * <p>Title：测试 InsertTablets 接口插入数据—正常情况<p/>
 * <p>Describe：测试使用 InsertTablets 接口插入各种数据类型的数据，包括BOOLEAN、INT32、INT64、FLOAT、DOUBLE、TEXT、STRING、BLOB、TIMESTAMP和DATE，包含空值情况<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2024/12/29<p/>
 */
public class TestInsertTabletsNormal extends BaseTestSuite_TreeModel {
    // 数据库名称
    private static final String database = "root.testInsertTables";
    // 设备名称
    private static final String deviceId = database + ".fdq";
    // 文件路径
    private static final String filepath1 = "data/tree/insert-tablet-success1.csv";
    private static final String filepath2 = "data/tree/insert-tablet-success2.csv";
    private static final String filepath3 = "data/tree/insert-tablet-success3.csv";

    // 存储路径
    private final List<String> paths = new ArrayList<>();
    // 存储数据类型
    private final List<TSDataType> dataTypes = new ArrayList<>();
    // 物理量的Schema
    private final List<IMeasurementSchema> schemaList = new ArrayList<>();
    // 用于存储多个Tablet
    private final Map<String, Tablet> tablets = new HashMap<>();

    // 物理量类型信息
    private final Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>();
    // 预期的记录条数
    private int expectCount = 0;

    /**
     * 在测试类之前准备好环境（数据库、时间序列）
     */
    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
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
            schemaList.add(new MeasurementSchema(key, value, TSEncoding.PLAIN, CompressionType.GZIP));
        });
        // 为时间序列创建编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 创建多个非对齐时间序列
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
    }

    /**
     * 测试 insertTables 方法接口（含空值）
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertTables() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 1、创建多个新的tablet实例
        Tablet tablet1 = new Tablet(deviceId, schemaList, 10);
        Tablet tablet2 = new Tablet(deviceId, schemaList, 10);
        Tablet tablet3 = new Tablet(deviceId, schemaList, 10);
        tablet1.initBitMaps();
        tablet2.initBitMaps();
        tablet3.initBitMaps();


        int rowIndex = 0;
        // 2、遍历获取的单行数据，进行数据处理
        // 第一份文件的数据
        for (Iterator<Object[]> it = getSingleNormal(filepath1); it.hasNext(); ) {
            // 统计期待数量
            expectCount++;
            // 获取每行数据
            Object[] line = it.next();
            // 向tablet添加时间戳
            tablet1.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 为每列添加数据
            for (int i = 0; i < schemaList.size(); i++) {
                if (line[i + 1] != null) {
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet1.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet1.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet1.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet1.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet1.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet1.addValue(schemaList.get(i).getMeasurementName(), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet1.addValue(schemaList.get(i).getMeasurementName(), rowIndex, new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet1.addValue(schemaList.get(i).getMeasurementName(), rowIndex, LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            rowIndex++;
        }
        rowIndex = 0;
        // 第二份文件的路径
        for (Iterator<Object[]> it = getSingleNormal(filepath2); it.hasNext(); ) {
            // 统计期待数量
            expectCount++;
            // 获取每行数据
            Object[] line = it.next();
            // 向tablet添加时间戳
            tablet2.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 为每列添加数据
            for (int i = 0; i < schemaList.size(); i++) {
                if (line[i + 1] != null) {
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet2.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet2.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet2.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet2.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet2.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet2.addValue(schemaList.get(i).getMeasurementName(), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet2.addValue(schemaList.get(i).getMeasurementName(), rowIndex, new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet2.addValue(schemaList.get(i).getMeasurementName(), rowIndex, LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            rowIndex++;
        }
        rowIndex = 0;
        // 第三份文件的数据
        for (Iterator<Object[]> it = getSingleNormal(filepath3); it.hasNext(); ) {
            // 统计期待数量
            expectCount++;
            // 获取每行数据
            Object[] line = it.next();
            // 向tablet添加时间戳
            tablet3.addTimestamp(rowIndex, Long.parseLong((String) line[0]));
            // 为每列添加数据
            for (int i = 0; i < schemaList.size(); i++) {
                if (line[i + 1] != null) {
                    // 根据数据类型添加值到tablet
                    switch (schemaList.get(i).getType()) {
                        case BOOLEAN:
                            tablet3.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Boolean.valueOf((String) line[i + 1]));
                            break;
                        case INT32:
                            tablet3.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Integer.valueOf((String) line[i + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            tablet3.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Long.valueOf((String) line[i + 1]));
                            break;
                        case FLOAT:
                            tablet3.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Float.valueOf((String) line[i + 1]));
                            break;
                        case DOUBLE:
                            tablet3.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Double.valueOf((String) line[i + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            tablet3.addValue(schemaList.get(i).getMeasurementName(), rowIndex, line[i + 1]);
                            break;
                        case BLOB:
                            tablet3.addValue(schemaList.get(i).getMeasurementName(), rowIndex, new Binary((String) line[i + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            tablet3.addValue(schemaList.get(i).getMeasurementName(), rowIndex, LocalDate.parse((CharSequence) line[i + 1]));
                            break;
                    }
                }
            }
            rowIndex++;
        }
        // 4、将生成的tablet添加的集合中
        tablets.put("1", tablet1);
        tablets.put("2", tablet2);
        tablets.put("3", tablet3);

        // 5、插入数据
        session.insertTablets(tablets);

        // 6、对比是否操作成功
        compare(expectCount);
    }

    /**
     * 用于查询比较插入条数看是否和预期相同
     */
    public void compare(int expectNonAligned) throws IoTDBConnectionException, StatementExecutionException {
        // 获取非对齐时间序列的实际记录条数
        int actualNonAligned = countLines("select * from " + deviceId, verbose);
        // 由于包含3条全为空值的记录需要减去3条
        expectNonAligned = expectNonAligned - 3;
        // 断言实际记录条数与预期相等
        assert actualNonAligned == expectNonAligned : "非对齐：实际记录条数与预期不一致，actual=" + actualNonAligned + " expect=" + expectNonAligned;
        // 清理非对齐时间序列的数据
        session.deleteData(deviceId, new Date().getTime());
    }

    /**
     * 获取正确的数据
     */
    @DataProvider(name = "insertSingleNormal")
    public Iterator<Object[]> getSingleNormal(String filepath) throws IOException {
        return new CustomDataProvider().load(filepath).getData();
    }

    /**
     * 在测试类之后执行的删除数据库
     */
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        // 删除数据库
        session.deleteDatabase(database);
    }
}
