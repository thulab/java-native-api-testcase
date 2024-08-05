package org.apache.iotdb.api.test.business;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

/**
 * 减少TS aligned
 * 创建aligned Device(6 sensor), 插入数据，查询数据，删除数据，减少TS，插入数据，查询数据，删除Device
 * 2022-12-28
 */
public class TestAlignedTSMinus extends BaseTestSuite {
    private String device = "root.business.alignedTSMinus";
    private String database = device.substring(0,device.lastIndexOf('.'));
    private int expectCount = 17;
    private Map<String, TSDataType> measureTSTypeInfos = new LinkedHashMap<>(6);

    private List<String> measurements = new ArrayList<>(6);
    private List<TSDataType> dataTypes = new ArrayList<>(6);
    private List<MeasurementSchema> schemaList = new ArrayList<>(6);// tablet
    private List<TSEncoding> encodings = new ArrayList<>(6);
    private List<CompressionType> compressors = new ArrayList<>(6);
    private List<String> alias = new ArrayList<>(6);


    @BeforeClass(enabled = true)
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException, IOException {
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

        measureTSTypeInfos.forEach((key,value) -> {
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
        return new CustomDataProvider().load("data/business-insert-records.csv").getData();
    }

    @Test(priority = 10)
    public void testCreateTS() throws IoTDBConnectionException, StatementExecutionException {
        for (int i = 0; i <3 ; i++) {
            encodings.add(TSEncoding.RLE);
        }
        encodings.add(TSEncoding.GORILLA);
        encodings.add(TSEncoding.GORILLA);
        encodings.add(TSEncoding.DICTIONARY);

        for (int i = 0; i < 6; i++) {
            compressors.add(CompressionType.SNAPPY);
        }
        measureTSTypeInfos.forEach((key,value) -> {
            alias.add("aligned_"+key);
        });
        session.createAlignedTimeseries(device, measurements, dataTypes, encodings, compressors, alias);
        assert  6 == getTimeSeriesCount(device+".*", false) : "创建TS数目";
    }

    @Test(priority = 20)
    public void testInsert() throws IOException, IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList, 100);
        int rowIndex = 0;
        int col = 0;
        tablet.initBitMaps();
        Iterator<Object[]> it = getSingleNormal();
        while (it.hasNext()) {
            rowIndex = tablet.rowSize++;
            Object[] line = it.next();
            tablet.addTimestamp(rowIndex, Long.valueOf((String)line[0]));
            for (int i = 0; i < schemaList.size(); i++) {
                col = i+1;
                if (line[col] == null) {
                    tablet.bitMaps[i].mark(rowIndex);
                    tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, null);
                    continue;
                }
                switch(schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Boolean.valueOf((String)line[col]));
                        break;
                    case INT32:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Integer.valueOf((String)line[col]));
                        break;
                    case INT64:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Long.valueOf((String)line[col]));
                        break;
                    case FLOAT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Float.valueOf((String)line[col]));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, Double.valueOf((String)line[col]));
                        break;
                    case TEXT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, line[col]);
                        break;
                }
            }
        }
        session.insertAlignedTablet(tablet);
        assert expectCount-1 == getRecordCount(device, verbose) : "插入record数目";
//        Assert.assertThrows(StatementExecutionException.class, ()->session.insertTablet(tablet));
    }
    @Test(priority = 30)
    public void testQuery() throws IoTDBConnectionException, StatementExecutionException {
        checkQueryResult("select s_double from "+ device +" where time=2022-11-22T17:29:58.754+08:00;", 1899.21);
    }
    @Test(priority = 40)
    public void testUpdate() throws IoTDBConnectionException, StatementExecutionException {
        long timestamp = 1669109398772L;
        checkQueryResult("select s_text from "+device +" where time="+timestamp+";",  0);

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
        session.insertAlignedRecordsOfOneDevice(device, times, measurementsList,datatypeList,valuesList);
        checkQueryResult("select s_text from "+device +" where time="+timestamp+";", "update_value");
        checkQueryResult("select s_long from "+device +" where time="+timestamp+";", 2);
    }
    @Test(priority = 41)
    public void testMinusTS() throws IoTDBConnectionException, StatementExecutionException {
        assert 6 == getTimeSeriesCount(device+".*", false) : "删除TS前，TS数量";
        session.deleteTimeseries(device+".s_boolean");
        assert 5 == getTimeSeriesCount(device+".*", false) : "删除TS后，TS数量";
        measurements.remove(0);
        dataTypes.remove(0);
        encodings.remove(0);
        compressors.remove(0);
        alias.remove(0);
    }

    @Test(priority = 42)
    public void testInsertAfterUpdate() throws IoTDBConnectionException, StatementExecutionException {
        long timestamp = 1669109398772L;
        checkQueryResult("select s_text from "+device +" where time="+timestamp+";",  "update_value");

        List<Long> times = new ArrayList<>(1);
        List<List<String>> measurementsList = new ArrayList<>(1);
        List<List<Object>> valuesList = new ArrayList<>(1);
        List<List<TSDataType>> datatypeList = new ArrayList<>(1);
        times.add(timestamp);
        measurementsList.add(measurements);
        datatypeList.add(dataTypes);
        List<Object> values = new ArrayList<>(6);
        values.add(1);
        values.add(timestamp);
        values.add(3.0f);
        values.add(4.0);
        values.add("testInsertAfterUpdate");
        valuesList.add(values);
        session.insertAlignedRecordsOfOneDevice(device, times, measurementsList,datatypeList,valuesList);
        checkQueryResult("select s_text from "+device +" where time="+timestamp+";", "testInsertAfterUpdate");
        checkQueryResult("select s_long from "+device +" where time="+timestamp+";", timestamp);

    }
    @Test(priority = 50)
    public void testDelete() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteData(device+".*", 1669109404000L);
        assert 1 == getRecordCount(device, verbose) : "确认结果:删除后还剩一条数据";
    }
    @Test(priority = 60)
    public void testInsertAfterDelete() throws IoTDBConnectionException, StatementExecutionException {
        List<Object> values = new ArrayList<>(6);
        values.add(55);
        values.add(1669109406000L);
        values.add(13.33f);
        values.add(876.44);
        values.add("insert after delete");
        session.insertAlignedRecord(device, 1669109406000L, measurements, dataTypes, values);
        assert 2 == getRecordCount(device, false) : "确认结果:删除后插入成功";
    }

    @Test(priority = 70)
    public void testDropTimeseries() throws IoTDBConnectionException, StatementExecutionException {
        assert true == session.checkTimeseriesExists(device+".s_int") :"TS int exists";
        session.deleteTimeseries(device+".*");
        assert false == session.checkTimeseriesExists(device+".s_int") :"TS int 已删除";
    }

}
