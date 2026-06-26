package org.apache.iotdb.api.test.rpc;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Covers RpcUtils branches that do not require a running IoTDB instance.
 */
public class TestRpcUtils {

    @Test(priority = 10)
    public void testBuildStatusAndResponseWrappers() {
        ByteBuffer responseData = ByteBuffer.wrap(new byte[]{1, 2, 3});
        TSStatus status = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "failed", responseData);

        Assert.assertEquals(status.getCode(), TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
        Assert.assertEquals(status.getMessage(), "failed");
        Assert.assertTrue(status.isSetResponseData());
        Assert.assertEquals(status.getResponseData(), new byte[]{1, 2, 3});

        TSStatus codeStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
        Assert.assertEquals(codeStatus.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());

        TSExecuteStatementResp executeResp = RpcUtils.getTSExecuteStatementResp(status);
        Assert.assertEquals(executeResp.getStatus().getCode(), status.getCode());
        Assert.assertEquals(executeResp.getStatus().getMessage(), "failed");

        TSFetchResultsResp fetchResp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
        Assert.assertEquals(fetchResp.getStatus().getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    @Test(priority = 20)
    public void testVerifySuccessAndFailureStatus() throws StatementExecutionException {
        RpcUtils.verifySuccess(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
        RpcUtils.verifySuccess(RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND));

        TSStatus failure = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "execute failed");
        Assert.expectThrows(StatementExecutionException.class, () -> RpcUtils.verifySuccess(failure));

        TSStatus multipleSuccess = RpcUtils.getStatus(Arrays.asList(
                RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
                RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)));
        RpcUtils.verifySuccess(multipleSuccess);

        TSStatus multipleFailure = RpcUtils.getStatus(Arrays.asList(
                RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
                RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "sub failed")));
        Assert.expectThrows(StatementExecutionException.class, () -> RpcUtils.verifySuccess(multipleFailure));
    }

    @Test(priority = 30)
    public void testVerifySuccessWithRedirection() {
        TSStatus directRedirect = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
                .setRedirectNode(new TEndPoint("127.0.0.1", 6668));
        Assert.expectThrows(RedirectException.class, () -> RpcUtils.verifySuccessWithRedirection(directRedirect));

        TSStatus subStatusRedirect = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
                .setSubStatus(Collections.singletonList(
                        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
                                .setRedirectNode(new TEndPoint("127.0.0.1", 6669))));
        Assert.expectThrows(RedirectException.class, () -> RpcUtils.verifySuccessWithRedirection(subStatusRedirect));
    }

    @Test(priority = 35)
    public void testVerifySuccessWithRedirectionForMultiDevices() {
        TSStatus status = RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
                .setSubStatus(Arrays.asList(
                        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
                                .setRedirectNode(new TEndPoint("127.0.0.1", 6670)),
                        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)));

        Assert.expectThrows(
                RedirectException.class,
                () -> RpcUtils.verifySuccessWithRedirectionForMultiDevices(
                        status, Arrays.asList("root.sg.d1", "root.sg.d2")));
    }

    @Test(priority = 40)
    public void testFailureStatusExtractionAndSquash() {
        TSStatus success = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
        TSStatus failure = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "failed");
        TSStatus multiple = RpcUtils.getStatus(Arrays.asList(success, failure));

        TSStatus extracted = RpcUtils.extractFailureStatues(multiple);
        Assert.assertEquals(extracted.getCode(), TSStatusCode.MULTIPLE_ERROR.getStatusCode());
        Assert.assertEquals(extracted.getSubStatusSize(), 1);
        Assert.assertEquals(extracted.getSubStatus().get(0).getMessage(), "failed");

        TSStatus successSquash = RpcUtils.squashResponseStatusList(Collections.singletonList(success));
        Assert.assertEquals(successSquash.getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());

        TSStatus failureSquash = RpcUtils.squashResponseStatusList(Arrays.asList(success, failure));
        Assert.assertEquals(failureSquash.getCode(), TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
        Assert.assertTrue(failureSquash.getMessage().contains("failed"));
    }

    @Test(priority = 50)
    public void testSqlDetection() {
        Assert.assertTrue(RpcUtils.isUseDatabase("use root.test"));
        Assert.assertTrue(RpcUtils.isUseDatabase("USE root.test"));
        Assert.assertFalse(RpcUtils.isUseDatabase(" show databases"));
        Assert.assertFalse(RpcUtils.isUseDatabase("use"));

        Assert.assertTrue(RpcUtils.isSetSqlDialect("set sql_dialect=table"));
        Assert.assertTrue(RpcUtils.isSetSqlDialect("SET   sql_dialect = tree"));
        Assert.assertFalse(RpcUtils.isSetSqlDialect("set timezone=UTC"));
        Assert.assertFalse(RpcUtils.isSetSqlDialect("set sql"));
    }

    @Test(priority = 60)
    public void testTimeFormatAndPrecision() {
        Assert.assertEquals(RpcUtils.setTimeFormat(" Long "), "long");
        Assert.assertEquals(RpcUtils.setTimeFormat("number"), "number");
        Assert.assertEquals(RpcUtils.setTimeFormat("yyyy-MM-dd"), "yyyy-MM-dd");
        Assert.expectThrows(IllegalArgumentException.class, () -> RpcUtils.setTimeFormat("bad-pattern["));

        ZoneId utc = ZoneId.of("UTC");
        Assert.assertEquals(RpcUtils.formatDatetime("long", RpcUtils.MILLISECOND, 123L, utc), "123");
        Assert.assertEquals(
                RpcUtils.formatDatetime("yyyy-MM-dd HH:mm:ss", RpcUtils.MILLISECOND, 0L, utc),
                "1970-01-01 00:00:00");
        Assert.assertTrue(RpcUtils.formatDatetime("default", RpcUtils.MICROSECOND, 1L, utc).contains(".000001"));
        Assert.assertTrue(RpcUtils.formatDatetime("iso8601", RpcUtils.NANOSECOND, -1L, utc).contains(".999999999"));
    }

    @Test(priority = 70)
    public void testTimeFactorAndTimestampConversion() {
        TSOpenSessionResp defaultResp = new TSOpenSessionResp();
        Assert.assertEquals(RpcUtils.getTimeFactor(defaultResp), 1_000);

        Map<String, String> configuration = new HashMap<>();
        configuration.put(RpcUtils.TIME_PRECISION, RpcUtils.MILLISECOND);
        Assert.assertEquals(RpcUtils.getTimeFactor(new TSOpenSessionResp().setConfiguration(configuration)), 1_000);

        configuration.put(RpcUtils.TIME_PRECISION, RpcUtils.MICROSECOND);
        Assert.assertEquals(RpcUtils.getTimeFactor(new TSOpenSessionResp().setConfiguration(configuration)), 1_000_000);

        configuration.put(RpcUtils.TIME_PRECISION, RpcUtils.NANOSECOND);
        Assert.assertEquals(RpcUtils.getTimeFactor(new TSOpenSessionResp().setConfiguration(configuration)), 1_000_000_000);

        configuration.put(RpcUtils.TIME_PRECISION, "second");
        Assert.expectThrows(
                IllegalArgumentException.class,
                () -> RpcUtils.getTimeFactor(new TSOpenSessionResp().setConfiguration(configuration)));

        Assert.assertEquals(RpcUtils.getTimePrecision(1_000), RpcUtils.MILLISECOND);
        Assert.assertEquals(RpcUtils.getTimePrecision(1_000_000), RpcUtils.MICROSECOND);
        Assert.assertEquals(RpcUtils.getTimePrecision(1_000_000_000), RpcUtils.NANOSECOND);
        Assert.expectThrows(IllegalArgumentException.class, () -> RpcUtils.getTimePrecision(1));

        Timestamp timestamp = RpcUtils.convertToTimestamp(1_234_567L, 1_000_000);
        Assert.assertEquals(timestamp.getTime(), 1_234L);
        Assert.assertEquals(timestamp.getNanos(), 234_567_000);
    }

    @Test(priority = 80)
    public void testVerifyStatusListFailure() {
        List<TSStatus> statuses = Arrays.asList(
                RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
                RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "batch failed"));

        Assert.expectThrows(BatchExecutionException.class, () -> RpcUtils.verifySuccess(statuses));
    }
}
