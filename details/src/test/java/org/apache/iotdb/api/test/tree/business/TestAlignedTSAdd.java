package org.apache.iotdb.api.test.tree.business;

import org.apache.iotdb.api.test.BaseTestSuite_TreeModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

/**
 * 增加TS aligned
 * 创建aligned Device(6 sensor), 插入数据，查询数据，删除数据，增加TS，插入数据，查询数据，删除Device
 * 2022-12-28
 */
public class TestAlignedTSAdd extends BaseTestSuite_TreeModel {
    private String device = "root.business.alignedTSAdd";
    private String database = device.substring(0, device.lastIndexOf('.'));
    private int expectCount = 17;
    private Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(6);

    private List<String> measurements = new ArrayList<>(7);
    private List<TSDataType> dataTypes = new ArrayList<>(7);
    private List<TSEncoding> encodings = new ArrayList<>(7);
    private List<CompressionType> compressors = new ArrayList<>(7);
    private List<String> alias = new ArrayList<>(7);

    private List<IMeasurementSchema> schemaList = new ArrayList<>(7);// tablet


    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        if (checkStroageGroupExists(database)) {
            session.deleteStorageGroup(database);
        }
        session.setStorageGroup(database);
        measureTSTypeInfos.put("s_boolean", TSDataType.BOOLEAN);
        measureTSTypeInfos.put("s_int", TSDataType.INT32);
        measureTSTypeInfos.put("s_long", TSDataType.INT64);
        measureTSTypeInfos.put("s_float", TSDataType.FLOAT);
        measureTSTypeInfos.put("s_double", TSDataType.DOUBLE);
        measureTSTypeInfos.put("s_text", TSDataType.TEXT);

        measureTSTypeInfos.forEach((key, value) -> {
            measurements.add(key);
            dataTypes.add(value);
            schemaList.add(new MeasurementSchema(key, value));
        });
    }

    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteStorageGroup(database);
    }

    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/tree/business-insert-records.csv").getData();
    }

    @Test(priority = 10)
    public void testCreateTS() throws IoTDBConnectionException, StatementExecutionException {
        for (int i = 0; i < 3; i++) {
            encodings.add(TSEncoding.RLE);
        }
        encodings.add(TSEncoding.GORILLA);
        encodings.add(TSEncoding.GORILLA);
        encodings.add(TSEncoding.DICTIONARY);

        for (int i = 0; i < 6; i++) {
            compressors.add(CompressionType.SNAPPY);
        }
        measureTSTypeInfos.forEach((key, value) -> {
            alias.add("aligned_" + key);
        });

        session.createAlignedTimeseries(device, measurements, dataTypes, encodings, compressors, alias);
        assert 6 == getTimeSeriesCount(device + ".*", false) : "创建TS数目";
    }

    @Test(priority = 20)
    public void testInsert() throws IOException, IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, 100);
        int rowIndex = 0;
        int col = 0;
        tablet.initBitMaps();
        Iterator<Object[]> it = getSingleNormal();
        while (it.hasNext()) {
            Object[] line = it.next();
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                col = i + 1;
                if (line[col] == null) {
                    continue;
                }
                switch (schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Boolean.valueOf((String) line[col]));
                        break;
                    case INT32:
                        tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Integer.valueOf((String) line[col]));
                        break;
                    case INT64:
                        tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Long.valueOf((String) line[col]));
                        break;
                    case FLOAT:
                        tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Float.valueOf((String) line[col]));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, Double.valueOf((String) line[col]));
                        break;
                    case TEXT:
                        tablet.addValue(schemaList.get(i).getMeasurementName(), rowIndex, line[col]);
                        break;
                }
            }
            rowIndex++;
        }
        session.insertAlignedTablet(tablet);
        assert expectCount - 1 == getRecordCount(device, verbose) : "插入record数目";

    }

    @Test(priority = 30)
    public void testQuery() throws IoTDBConnectionException, StatementExecutionException {
        checkQueryResult("select s_double from " + device + " where time=2022-11-22T17:29:58.754+08:00;", TSDataType.DOUBLE, 1899.21);
    }


    @Test(priority = 40)
    public void testUpdate() throws IoTDBConnectionException, StatementExecutionException {
        long timestamp = 1669109398772L;
        checkQueryResult("select s_text from " + device + " where time=" + timestamp + ";", TSDataType.TEXT, 0);

        List<Long> times = new ArrayList<>(1);
        List<List<String>> measurementsList = new ArrayList<>(1);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<List<TSDataType>> datatypeList = new ArrayList<>(1);
        times.add(timestamp);
        measurementsList.add(measurements);
        datatypeList.add(dataTypes);
        List<Object> values = new ArrayList<>(6);
        values.add(false);
        values.add(1);
        values.add(2L);
        values.add(3.0f);
        values.add(4.0);
        values.add("update_value");
        valuesList.add(values);
        session.insertAlignedRecordsOfOneDevice(device, times, measurementsList, datatypeList, valuesList);
        checkQueryResult("select s_text from " + device + " where time=" + timestamp + ";", TSDataType.TEXT, "update_value");
        checkQueryResult("select s_long from " + device + " where time=" + timestamp + ";", TSDataType.INT64, 2);
    }

    @Test(priority = 41)
    public void testAddTS() throws IoTDBConnectionException, StatementExecutionException {
        assert 6 == getTimeSeriesCount(device + ".*", false) : "增加TS前，TS数量";
        List<String> measurements_add = new ArrayList<>(1);
        List<TSDataType> dataTypes_add = new ArrayList<>(1);
        List<TSEncoding> encodings_add = new ArrayList<>(1);
        List<CompressionType> compressors_add = new ArrayList<>(1);
        List<String> alias_add = new ArrayList<>(1);

        encodings_add.add(TSEncoding.GORILLA);
        compressors_add.add(CompressionType.SNAPPY);
        measurements_add.add("appendFloat");
        dataTypes_add.add(TSDataType.FLOAT);
        alias_add.add("aligned_appendFloat");

        session.createAlignedTimeseries(device, measurements_add, dataTypes_add, encodings_add, compressors_add, alias_add);
        assert 7 == getTimeSeriesCount(device + ".*", false) : "增加TS后，TS数量";
        // for insert
        encodings.add(TSEncoding.GORILLA);
        compressors.add(CompressionType.SNAPPY);
        measurements.add("appendFloat");
        dataTypes.add(TSDataType.FLOAT);
        alias.add("aligned_appendFloat");
    }

    @Test(priority = 42)
    public void testInsertAfterUpdate() throws IoTDBConnectionException, StatementExecutionException {
        long timestamp1 = 1669109508000L;
        long timestamp2 = 1669109398772L;
        List<Long> times = new ArrayList<>(2);
        List<List<String>> valueList = new ArrayList<>(2);
        List<List<String>> measurementList = new ArrayList<>(2);
        times.add(timestamp1);
        times.add(timestamp2);
        measurementList.add(measurements);
        measurementList.add(measurements);

        for (int i = 0; i < 2; i++) {
            List<String> values = new ArrayList<>(7);
            values.add(String.valueOf(false));
            values.add(String.valueOf(i));
            values.add(String.valueOf(times.get(i)));
            values.add(String.valueOf((i + 1) * 13.33f));
            values.add(String.valueOf((i + 1) * 144.44));
            values.add("add/update after update TS:" + i);
            values.add(String.valueOf((i + 1) * 34567f));
            valueList.add(values);
        }
        session.insertAlignedStringRecordsOfOneDevice(device, times, measurementList, valueList);
        checkQueryResult("select s_float from " + device + " where time=" + timestamp1 + ";", TSDataType.FLOAT, 13.33);
        checkQueryResult("select appendFloat from " + device + " where time=" + timestamp1 + ";", TSDataType.FLOAT, 34567.0);
        checkQueryResult("select s_long from " + device + " where time=" + timestamp2 + ";", TSDataType.INT64, timestamp2);
        checkQueryResult("select appendFloat from " + device + " where time=" + timestamp2 + ";", TSDataType.FLOAT, 69134.0);
    }

    @Test(priority = 50)
    public void testDelete() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteData(device + ".*", 1669109404000L);
        assert 2 == getRecordCount(device, verbose) : "确认结果:删除后还剩2条数据";
    }

    @Test(priority = 60)
    public void testInsertAfterDelete() throws IoTDBConnectionException, StatementExecutionException {
        long timestamp = 1669109406000L;
        List<Object> values = new ArrayList<>(7);
        values.add(true);
        values.add(55);
        values.add(timestamp);
        values.add(13.33f);
        values.add(876.44);
        values.add("insert after delete");
        values.add(26.5f);
        session.insertAlignedRecord(device, timestamp, measurements, dataTypes, values);
        assert 3 == getRecordCount(device, verbose) : "确认结果:删除后插入成功";
        checkQueryResult("select s_long from " + device + " where time=" + timestamp + ";", TSDataType.INT64, timestamp);
        checkQueryResult("select appendFloat from " + device + " where time=" + timestamp + ";", TSDataType.FLOAT, 26.5);
    }

    @Test(priority = 70)
    public void testDropTimeseries() throws IoTDBConnectionException, StatementExecutionException {
        assert true == session.checkTimeseriesExists(device + ".s_boolean") : "TS boolean exists";
        session.deleteTimeseries(device + ".*");
        assert false == session.checkTimeseriesExists(device + ".s_boolean") : "TS boolean 已删除";
    }

}
