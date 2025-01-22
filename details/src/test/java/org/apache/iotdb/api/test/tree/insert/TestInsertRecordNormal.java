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
 * <p>Title：测试 InsertRecord 接口插入数据—正常情况<p/>
 * <p>Describe：测试使用 InsertRecord 接口插入各种数据类型的数据，包括BOOLEAN、INT32、INT64、FLOAT、DOUBLE、TEXT、STRING、BLOB、TIMESTAMP和DATE<p/>
 * <p>Author：肖林捷<p/>
 * <p>Date：2024/12/29<p/>
 */
public class TestInsertRecordNormal extends BaseTestSuite_TreeModel {
    // 数据库名称
    private static final String database = "root.testInsertRecord";
    // 非对齐设备名称
    private static final String deviceId = database + ".fdq";
    // 时间戳
    private Long time = null;

    // 存储多个时间序列路径
    private final List<String> paths = new ArrayList<>();
    // 用于存储物理量
    private final ArrayList<String> measurements = new ArrayList<>();
    // 用于存储数据类型
    private final ArrayList<TSDataType> dataTypes = new ArrayList<>();
    // 用于存储值
    private final ArrayList<Object> values = new ArrayList<>();

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
            paths.add(deviceId + "." + key);
            measurements.add(key);
            dataTypes.add(value);
        });
        // 3.3、创建并添加编码和压缩类型的列表
        List<TSEncoding> encodings = new ArrayList<>(10);
        List<CompressionType> compressionTypes = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            encodings.add(TSEncoding.PLAIN);
            compressionTypes.add(CompressionType.GZIP);
        }
        // 3.4、创建多条非对齐时间序列
        session.createMultiTimeseries(paths, dataTypes, encodings, compressionTypes,
                null, null, null, null);

    }

    /**
     * 测试 insertRecord 方法接口
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void insertRecord() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 遍历获取的单行数据，为设备添加值
        for (Iterator<Object[]> it = getSingleNormal(); it.hasNext(); ) {
            // 统计行数
            expectCount++;
            // 获取每行数据
            Object[] line = it.next();
            // 获取时间戳
            time = Long.valueOf((String) line[0]);
            // 遍历每行逐个物理量的数据
            for (int i = 0; i < measurements.size(); i++) {
                // 根据数据类型添加值到values中
                switch (dataTypes.get(i)) {
                    case BOOLEAN:
                        values.add(line[i + 1] == null ? null : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        values.add(line[i + 1] == null ? null : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        values.add(line[i + 1] == null ? null : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        values.add(line[i + 1] == null ? null : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        values.add(line[i + 1] == null ? null : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        values.add(line[i + 1] == null ? null : line[i + 1]);
                        break;
                    case BLOB:
                        values.add(line[i + 1] == null ? null : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        values.add(line[i + 1] == null ? null : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
            // 插入数据
            session.insertRecord(deviceId, time, measurements, dataTypes, values);
            // 清空容器
            values.clear();
        }
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
        int actualNonAligned = countLines("select * from " + deviceId, verbose);
        // 断言实际记录条数与预期相等
        assert actualNonAligned == expectNonAligned : "非对齐：实际记录条数与预期不一致，actual=" + actualNonAligned + " expect=" + expectNonAligned;
        // 清理非对齐时间序列的数据
        session.deleteData(deviceId, new Date().getTime());
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
