package org.apache.iotdb.api.test.tree.other;

import org.apache.iotdb.api.test.BaseTestSuiteTreeModel;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TimeSeriesBaseTestSuite extends BaseTestSuiteTreeModel {

    /**
     * 时间序列类共用的库名（来自 timeseries-*.csv 与显式 createDatabase）。
     * run 级首类全局清理后，后续时间序列类只删这些库，不再 drop 全实例。
     */
    private static final Set<String> MANAGED_DATABASES = Set.of(
            "root.t1", "root.testDuplicate", "root.sg.d1", "root.sgt", "root.error",
            "root.l", "root.sg", "root.t", "root.key",
            "root.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234",
            "root.caseSensitive", "root.sgm", "root.sgm02", "root.sgm06",
            "root.non", "root.root", "root.params.ts");

    @Override
    protected Set<String> managedDatabases() {
        return MANAGED_DATABASES;
    }

    public List<Object> translateString2Type(String datatypeStr, String encodingStr, String compressStr) {
        datatypeStr = datatypeStr.toLowerCase();
        encodingStr = encodingStr.toUpperCase();
        compressStr = compressStr.toUpperCase();
        List<Object> result = new ArrayList<>(3);
        switch (datatypeStr) {
            case "boolean":
                result.add(TSDataType.BOOLEAN);
                break;
            case "int":
                result.add(TSDataType.INT32);
                break;
            case "long":
                result.add(TSDataType.INT64);
                break;
            case "float":
                result.add(TSDataType.FLOAT);
                break;
            case "double":
                result.add(TSDataType.DOUBLE);
                break;
            case "vector":
                result.add(TSDataType.VECTOR);
                break;
            case "text":
                result.add(TSDataType.TEXT);
                break;
            default:
                result.add(null);
                break;
        }
        switch (encodingStr) {
            case "PLAIN":
                result.add(TSEncoding.PLAIN);
                break;
            case "DICTIONARY":
                result.add(TSEncoding.DICTIONARY);
                break;
            case "RLE":
                result.add(TSEncoding.RLE);
                break;
            case "DIFF":
                result.add(TSEncoding.DIFF);
                break;
            case "TS_2DIFF":
                result.add(TSEncoding.TS_2DIFF);
                break;
            case "BITMAP":
                result.add(TSEncoding.BITMAP);
                break;
            case "GORILLA_V1":
                result.add(TSEncoding.GORILLA_V1);
                break;
            case "REGULAR":
                result.add(TSEncoding.REGULAR);
                break;
            case "GORILLA":
                result.add(TSEncoding.GORILLA);
                break;
            case "ZIGZAG":
                result.add(TSEncoding.ZIGZAG);
                break;
            case "FREQ":
                result.add(TSEncoding.FREQ);
                break;
            case "SPRINTZ":
                result.add(TSEncoding.SPRINTZ);
                break;
            case "RLBE":
                result.add(TSEncoding.RLBE);
                break;
            default:
                result.add(null);
                break;
        }
        switch (compressStr) {
            case "UNCOMPRESSED":
                result.add(CompressionType.UNCOMPRESSED);
                break;
            case "SNAPPY":
                result.add(CompressionType.SNAPPY);
                break;
            case "GZIP":
                result.add(CompressionType.GZIP);
                break;
            case "LZ4":
                result.add(CompressionType.LZ4);
                break;
            case "ZSTD":
                result.add(CompressionType.ZSTD);
                break;
            case "LZMA2":
                result.add(CompressionType.LZMA2);
                break;
            default:
                result.add(null);
                break;
        }
        return result;
    }

}
