package org.apache.iotdb.api.test.tree.insert;

import org.apache.iotdb.api.test.BaseTestSuite_TreeModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.*;

/**
 * <p>Title：测试 InsertRecords 接口插入数据—正常情况<p/>
 * <p>Describe：测试使用 InsertRecords 接口插入各种数据类型的数据，包括BOOLEAN、INT32、INT64、FLOAT、DOUBLE、TEXT、STRING、BLOB、TIMESTAMP和DATE<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2024/12/29<p/>
 */
public class TestInsertRecordsNormal extends BaseTestSuite_TreeModel {
    // 数据库名称
    private static final String database = "root.testInsertRecords";
    // 非对齐设备名称
    private static final String deviceId1 = database + ".fdq1";
    private static final String deviceId2 = database + ".fdq2";
    private static final String deviceId3 = database + ".fdq3";

    // 存储多个设备
    private final List<String> deviceIds = new ArrayList<>();
    // 存储多个times
    private final List<Long> times = new ArrayList<>();
    // 存储多个设备的多个物理量
    private final List<List<String>> measurementsList = new ArrayList<>();
    // 存储多个设备的多个时间序列数据类型
    private final List<List<TSDataType>> typesList = new ArrayList<>();
    // 存储多个设备的多个值
    private final List<List<Object>> valuesList = new ArrayList<>();
    // 存储单个设备多个物理量
    private List<String> measurements;
    // 存储单个设备多个数据类型
    private List<TSDataType> dataTypes;
    // 存储单个设备多个值
    private ArrayList<Object> values;
    // 存储路径
    private final List<String> paths = new ArrayList<>();

    // 存储物理量名称和数据类型
    private final Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>();
    // 预期的记录条数
    private int expectCount = 0;

    /**
     * 在测试类之前准备好环境（数据库、时间序列）
     */
    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        // 1、检查存储组是否存在，如果存在则删除
        if (checkStroageGroupExists(database)) {
            session.deleteDatabase(database);
        }

        // 2、创建数据库
        session.createDatabase(database);

        // 3、创建时间序列
        // 3.1、添加数据类型
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
        // 3.2、遍历measureTSTypeInfos，将路径、物理量和数据类型存入对应集合中
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId1 + "." + key);
        });
        for (int i = 0; i < 10; i++) {
            measurements = new ArrayList<>(10);
            dataTypes = new ArrayList<>(10);
            measureTSTypeInfos.forEach((key, value) -> {
                measurements.add(key);
                dataTypes.add(value);
            });
            // 将集合存入对应的集合中
            deviceIds.add(deviceId1);
            measurementsList.add(measurements);
            typesList.add(dataTypes);
        }
        // 3.3、创建并添加编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 3.4、为设备1创建非对齐时间序列
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
        // 清空容器
        paths.clear();

        // 3.5、重复操作为设备2创建非对齐时间序列
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId2 + "." + key);
        });
        for (int i = 0; i < 10; i++) {
            measurements = new ArrayList<>(10);
            dataTypes = new ArrayList<>(10);
            measureTSTypeInfos.forEach((key, value) -> {
                measurements.add(key);
                dataTypes.add(value);
            });
            // 将集合存入对应的集合中
            deviceIds.add(deviceId2);
            measurementsList.add(measurements);
            typesList.add(dataTypes);
        }
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
        paths.clear();

        // 3.6、重复操作为设备3创建非对齐时间序列
        measureTSTypeInfos.forEach((key, value) -> {
            paths.add(deviceId3 + "." + key);
        });
        for (int i = 0; i < 10; i++) {
            measurements = new ArrayList<>(10);
            dataTypes = new ArrayList<>(10);
            measureTSTypeInfos.forEach((key, value) -> {
                measurements.add(key);
                dataTypes.add(value);
            });
            // 将集合存入对应的集合中
            deviceIds.add(deviceId3);
            measurementsList.add(measurements);
            typesList.add(dataTypes);
        }
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);
    }

    /**
     * 测试 insertRecords 方法接口
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertRecords() throws IOException, IoTDBConnectionException, StatementExecutionException {
        for (int i = 0; i < 3; i++) {
            // 遍历获取的单行数据，为设备添加值
            for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
                // 统计行数
                expectCount++;
                values = new ArrayList<>();
                // 获取每行数据
                Object[] line = it.next();
                // 遍历每行逐个物理量的数据
                for (int j = 0; j < measurements.size(); j++) {
                    // 根据数据类型添加值到values中
                    switch (dataTypes.get(j)) {
                        case BOOLEAN:
                            values.add(line[j + 1] == null ? null : Boolean.valueOf((String) line[j + 1]));
                            break;
                        case INT32:
                            values.add(line[j + 1] == null ? null : Integer.valueOf((String) line[j + 1]));
                            break;
                        case INT64:
                        case TIMESTAMP:
                            values.add(line[j + 1] == null ? null : Long.valueOf((String) line[j + 1]));
                            break;
                        case FLOAT:
                            values.add(line[j + 1] == null ? null : Float.valueOf((String) line[j + 1]));
                            break;
                        case DOUBLE:
                            values.add(line[j + 1] == null ? null : Double.valueOf((String) line[j + 1]));
                            break;
                        case TEXT:
                        case STRING:
                            values.add(line[j + 1] == null ? null : line[j + 1]);
                            break;
                        case BLOB:
                            values.add(line[j + 1] == null ? null : new Binary((String) line[j + 1], Charset.defaultCharset()));
                            break;
                        case DATE:
                            values.add(line[j + 1] == null ? null : LocalDate.parse((CharSequence) line[j + 1]));
                            break;
                    }
                }
                // 添加值
                valuesList.add(values);
                // 添加时间戳
                times.add(Long.valueOf((String) line[0]));
            }
        }
        // 插入数据
        session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);

        // 对比是否操作成功
        compare(expectCount);
    }

    /**
     * 获取正确的数据
     */
    @DataProvider(name = "insertSingleNormal")
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/tree/insert-record-success.csv").getData();
    }

    /**
     * 用于查询比较插入条数看是否和预期相同
     */
    public void compare(int expectNonAligned) throws IoTDBConnectionException, StatementExecutionException {
        // 获取非对齐时间序列的实际记录条数
        int actualNonAligned = countLines("select * from " + deviceId1, verbose) + countLines("select * from " + deviceId2, verbose) + countLines("select * from " + deviceId3, verbose);
        // 断言实际记录条数与预期相等
        assert actualNonAligned == expectNonAligned : "非对齐：实际记录条数与预期不一致，actual=" + actualNonAligned + " expect=" + expectNonAligned;
        // 清理非对齐时间序列的数据
        session.deleteData(deviceId1, new Date().getTime());
        session.deleteData(deviceId2, new Date().getTime());
        session.deleteData(deviceId3, new Date().getTime());
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
