package org.apache.iotdb.api.test.tree.data.query;

import org.testng.annotations.Test;

public class TestQueryNormal {

    /**
     * 测试 ExecuteRawDataQuery
     */
//    @Test(priority = 100)
    public void testExecuteRawDataQuery() {
        // executeRawDataQuery(List<String> paths, long startTime, long endTime, long timeOut)
        // executeRawDataQuery(List<String> paths, long startTime, long endTime)
        // executeLastDataQuery(List<String> paths, long lastTime)
    }

    /**
     * 测试 ExecuteFastLastDataQueryForOnePrefixPath
     */
//    @Test
    public void testExecuteFastLastDataQueryForOnePrefixPath() {
        // executeFastLastDataQueryForOnePrefixPath(final List<String> prefixes)
    }

    /**
     * 测试 ExecuteLastDataQueryForOneDevice
     */
//    @Test
    public void testExecuteLastDataQueryForOneDevice() {
        // executeLastDataQueryForOneDevice(String db, String device, List<String> sensors, boolean isLegalPathNodes)
    }

    /**
     * 测试 ExecuteAggregationQuery
     */
//    @Test
    public void testExecuteAggregationQuery() {
        // executeAggregationQuery(List<String> paths, List<TAggregationType> aggregations)
        // executeAggregationQuery(List<String> paths, List<TAggregationType> aggregations, long startTime, long endTime)
        // executeAggregationQuery(
        //      List<String> paths,
        //      List<TAggregationType> aggregations,
        //      long startTime,
        //      long endTime,
        //      long interval)
        // executeAggregationQuery(
        //      List<String> paths,
        //      List<TAggregationType> aggregations,
        //      long startTime,
        //      long endTime,
        //      long interval,
        //      long slidingStep)
    }
}

