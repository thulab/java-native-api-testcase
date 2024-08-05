package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.read.common.RowRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

public class TriggerNameTest extends BaseTestSuite {

    @BeforeClass
    public void beforeClass() throws IoTDBConnectionException, StatementExecutionException {
        clean_triggers();
    }
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        clean_triggers();
    }
    @DataProvider(name = "normalNames", parallel = true)
    private Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/names-normal.csv").getData();
    }
    @DataProvider(name = "errorNames", parallel = true)
    private Iterator<Object[]> getErrorNames() throws IOException {
        return new CustomDataProvider().load("data/names-error.csv").getData();
    }
    @DataProvider(name = "sameNames", parallel = true)
    private Iterator<Object[]> getSameNames() throws IOException {
        return new CustomDataProvider().load("data/same-name-concurrent.csv").getData();
    }


    private void check_trigger_status(String name, String status) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement("show triggers;");
        while(records.hasNext()) {
            RowRecord row = records.next();
            assert status.equals(row.getFields().get(2).toString()) : "pipe状态检查:expect ["+status+"], actual ["+row.getFields().get(2).toString()+"]";
//            assert name.equals(row.getFields().get(0).toString()) : "show pipe中的名称检查:expect ["+name+"], actual ["+row.getFields().get(0).toString()+"]";
        }
    }
    private void clean_triggers() throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet records = session.executeQueryStatement("show pipes;");
        int index = 0;
        while(records.hasNext()) {
            index++;
            RowRecord row = records.next();
            if (verbose) {
                System.out.println("drop pipe " + row.getFields().get(0));
            }
            session.executeNonQueryStatement("drop pipe " + row.getFields().get(0));
        }
        if (verbose) {
            System.out.println("drop pipes :"+index);
        }
    }
    @Test(priority = 10, dataProvider = "normalNames")
    public void testTriggerName_normal(String name, String comment, String Index) throws IoTDBConnectionException, StatementExecutionException, IOException {
        String sql = "CREATE STATELESS TRIGGER " +name+
                " AFTER INSERT ON root.triggertest.d1.* AS 'org.example.DoubleValueMonitor' WITH ( 'remote_ip'='127.0.0.1',  'lo' = '10',  'hi' = '15.5');";
        Session s = PrepareConnection.getSession();
        s.executeNonQueryStatement(sql);
        s.executeNonQueryStatement("drop trigger "+name);
        s.close();
    }
    @Test(priority = 20, dataProvider = "errorNames", expectedExceptions = StatementExecutionException.class)
    public void testTriggerName_error(String name, String comment, String Index) throws IoTDBConnectionException, StatementExecutionException, IOException {
        String sql = "CREATE STATELESS TRIGGER " +name+
                " AFTER INSERT ON root.triggertest.d1.* AS 'org.example.DoubleValueMonitor' WITH ( 'remote_ip'='127.0.0.1',  'lo' = '10',  'hi' = '15.5');";
//        System.out.println(sql);
        Session s = PrepareConnection.getSession();
        s.executeNonQueryStatement(sql);
        s.close();
    }

}
