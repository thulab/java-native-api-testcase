package org.apache.iotdb.api.test.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.jetbrains.annotations.NotNull;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * 读取csv文件，返回除第一行header外的数据，供给给测试方法使用
 */
public class CustomDataProvider {
    private final Logger logger = Logger.getLogger(CustomDataProvider.class);
    private final List<Object[]> testCases = new ArrayList<>();
    private Reader reader;

    /**
     * 读取并解析CSV文件（树模型）
     */
    public Iterable<CSVRecord> readCSV_tree(String filepath, char delimiter) throws IOException {
        // 在Windows中使用
//        String path = Objects.requireNonNull(CustomDataProvider.class.getClassLoader().getResource(filepath)).getPath();
//        if (path.charAt(0) == '/') {
//            path = path.substring(1);
//        }
//        logger.info("read csv:" + path);
//        this.reader = Files.newBufferedReader(Paths.get(path));
        // 在Linux中使用
        logger.info("read csv:" + CustomDataProvider.class.getClassLoader().getResource(filepath).getPath());
        this.reader = Files.newBufferedReader(Paths.get(CustomDataProvider.class.getClassLoader().getResource(filepath).getPath()));

        CSVFormat csvformat = CSVFormat.DEFAULT.withDelimiter(delimiter).withEscape('\\').withQuote('"').withIgnoreEmptyLines(true);
        Iterable<CSVRecord> records = csvformat.parse(reader);
        // 去除header
        records.iterator().next();
        return records;
    }

    /**
     * 读取并解析CSV文件（表模型的）
     */
    public Iterable<CSVRecord> readCSV_table(String filepath) throws IOException {
        // 在Windows中使用
//        String path = Objects.requireNonNull(CustomDataProvider.class.getClassLoader().getResource(filepath)).getPath();
//        if (path.charAt(0) == '/') {
//            path = path.substring(1);
//        }
//        logger.info("read csv:" + path);
//        this.reader = Files.newBufferedReader(Paths.get(path));
        // 在Linux中使用
        logger.info("read csv:" + CustomDataProvider.class.getClassLoader().getResource(filepath).getPath());
        this.reader = Files.newBufferedReader(Paths.get(CustomDataProvider.class.getClassLoader().getResource(filepath).getPath()));

        // 设置CSV文件的格式
        CSVFormat csvformat = CSVFormat.MYSQL;
        // 解析CSV文件，返回包含CSV记录的Iterable对象
        Iterable<CSVRecord> records = csvformat.parse(reader);
        // 去除header
        records.iterator().next();
        return records;
    }

    /**
     * 处理map
     */
    private Map<String, String> processMapField(@NotNull String cols) {
        if (cols.equals("null")) {
            return null;
        }
        Map<String, String> cols_map = new HashMap<>();
        if (cols.equals("empty")) {
            return cols_map;
        }
        for (String item : cols.split("[|]")) {
            if (item.isEmpty()) {
                continue;
            }
            String[] values = item.split(":");
            if (values.length == 1) {
                cols_map.put(values[0], "");
            } else {
                cols_map.put(values[0], values[1]);
            }
        }
        return cols_map;
    }

    /**
     * 处理list
     */
    private @NotNull List processListField(@NotNull String cols) {
        if (cols.equals("null")) {
            return null;
        } else if (cols.startsWith("m:")) {
            List<Map<String, String>> result = new ArrayList<Map<String, String>>();
            for (String item_l : cols.split(",")) {
                item_l = item_l.substring(2);
                Map<String, String> cols_map = new HashMap<>();
                if (item_l.equals("empty")) {
                    result.add(cols_map);
                } else {
                    for (String item_m : item_l.split("[|]")) {
                        item_m = item_m.substring(2);
                        if (item_m.isEmpty()) {
                            continue;
                        }
                        String[] values = item_m.split(":");
                        cols_map.put(values[0], values[1]);
                    }
                }
            }
            return result;
        } else {
            List<String> result = new ArrayList<>();
            if (!cols.equals("empty")) {
                result.addAll(Arrays.asList(cols.split(",")));
            }
            return result;
        }
    }

    /**
     * 解析包括map,list在内的自定义格式
     */
    public CustomDataProvider load(String filepath, char delimiter) throws IOException {
        Iterable<CSVRecord> records = this.readCSV_tree(filepath, delimiter);
        for (CSVRecord record : records) {
            // 解析每一行
            List<Object> columns_arr = new ArrayList<>();
            Iterator<String> record_iter = record.iterator();
            // 如果以#开头，那么跳过
            String cols = record_iter.next();
            boolean breakFlag = false;
            if (!cols.startsWith("#")) {
                do {
                    if (cols.startsWith("m:")) {
                        cols = cols.substring(2);
                        columns_arr.add(processMapField(cols));
                    } else if (cols.startsWith("l:")) {
                        cols = cols.substring(2);
                        columns_arr.add(processListField(cols));
                    } else if (cols.equals("null")) {
                        columns_arr.add(null);
                    } else {
                        columns_arr.add(cols);
                    }
                    if (record_iter.hasNext()) {
                        cols = record_iter.next();
                    } else {
                        breakFlag = true;
                    }
                } while (!breakFlag);
                this.testCases.add(columns_arr.toArray());
            }
        }
        return this;
    }

    /**
     * 解析sql语句的csv文件
     */
    private CustomDataProvider load_sql(String filepath) throws IOException {
        Iterable<CSVRecord> records;
        // 读取并解析csv文件
        records = this.readCSV_table(filepath);
        // 获取每条记录
        for (CSVRecord record : records) {
            // 创建一个列表，用于存储解析后的列数据
            List<Object> columns_arr = new ArrayList<>();
            // 创建一个迭代器，用于遍历CSV记录的每一列
            Iterator<String> record_iter = record.iterator();
            // 读取第一列数据
            String cols = record_iter.next();
            // 标记是否需要跳出循环
            boolean breakFlag = false;
            // 判断是否以"#"开头
            if (!cols.startsWith("#")) {
                do {
                    // 不是则直接添加到列表
                    columns_arr.add(cols);
                    // 检查是否还有更多的列数据
                    if (record_iter.hasNext()) {
                        cols = record_iter.next(); // 读取下一列数据
                    } else {
                        breakFlag = true; // 如果没有更多的列数据，设置标记为true
                    }
                    // 如果标记为true，则跳出循环
                } while (!breakFlag);
                // 将解析后的列数据转换为数组，并添加到testCases列表中
                this.testCases.add(columns_arr.toArray());
            }
        }
        return this;
    }

    /**
     * 加载csv,直接返回String类型
     */
    public List<List<String>> loadString(String filepath, char delimiter) throws IOException {
        Iterable<CSVRecord> records = this.readCSV_tree(filepath, delimiter);
        List<List<String>> result = new ArrayList<>();
        for (CSVRecord record : records) {
            // 解析每一行
            List<String> eachLine = new ArrayList<>();
            Iterator<String> recordIter = record.iterator();
            // 如果以#开头，那么跳过
            String firstCols = recordIter.next();
            if (!firstCols.startsWith("#")) {
                eachLine.add(firstCols);
                while (recordIter.hasNext()) {
                    eachLine.add(recordIter.next());
                }
                result.add(eachLine);
            }
        }
        this.reader.close();
        return result;
    }

    /**
     * 固定解析 ts-structures.csv
     * 第一列 TSDataType
     * 第二列 TSEncoding
     * 第三列 CompressionType
     */
    public List<List<Object>> parseTSStructure(String filepath) throws IOException {
        List<List<Object>> result = new ArrayList<>();
        Iterable<CSVRecord> records = this.readCSV_tree(filepath, ',');
        for (CSVRecord record : records) {
            Iterator<String> recordIter = record.iterator();
            // 如果以#开头，那么跳过
            String firstCols = recordIter.next();
            if (!firstCols.startsWith("#")) {
                // 解析每一行
                List<Object> eachLine = new ArrayList<>();
                eachLine.add(parseDataType(firstCols));
                eachLine.add(parseEncoding(recordIter.next()));
                eachLine.add(parseCompressionType(recordIter.next()));
                eachLine.add(recordIter.next());
                eachLine.add(recordIter.next());
                result.add(eachLine);
                testCases.add(eachLine.toArray());
            }
        }
        this.reader.close();
        return result;
    }

    /**
     * 解析数据类型
     */
    public TSDataType parseDataType(String datatypeStr) {
        switch (datatypeStr.toLowerCase()) {
            case "boolean":
                return TSDataType.BOOLEAN;
            case "int":
                return TSDataType.INT32;
            case "long":
                return TSDataType.INT64;
            case "float":
                return TSDataType.FLOAT;
            case "double":
                return TSDataType.DOUBLE;
            case "vector":
                return TSDataType.VECTOR;
            case "text":
                return TSDataType.TEXT;
            case "string":
                return TSDataType.STRING;
            case "time":
                return TSDataType.TIMESTAMP;
            case "blob":
                return TSDataType.BLOB;
            case "date":
                return TSDataType.DATE;
            case "null":
                return null;
            default:
                throw new RuntimeException("bad input DataType: " + datatypeStr);
        }
    }

    /**
     * 解析编码方式
     */
    public TSEncoding parseEncoding(String encodingStr) {
        switch (encodingStr.toUpperCase()) {
            case "PLAIN":
                return TSEncoding.PLAIN;
            case "DICTIONARY":
                return TSEncoding.DICTIONARY;
            case "RLE":
                return TSEncoding.RLE;
            case "DIFF":
                return TSEncoding.DIFF;
            case "TS_2DIFF":
                return TSEncoding.TS_2DIFF;
            case "BITMAP":
                return TSEncoding.BITMAP;
            case "GORILLA_V1":
                return TSEncoding.GORILLA_V1;
            case "REGULAR":
                return TSEncoding.REGULAR;
            case "GORILLA":
                return TSEncoding.GORILLA;
            case "ZIGZAG":
                return TSEncoding.ZIGZAG;
            case "FREQ":
                return TSEncoding.FREQ;
            case "SPRINTZ":
                return TSEncoding.SPRINTZ;
            case "RLBE":
                return TSEncoding.RLBE;
            default:
                throw new RuntimeException("bad input Encoding: " + encodingStr);
        }
    }

    /**
     * 解析压缩方式
     */
    public CompressionType parseCompressionType(String compressStr) {
        compressStr.toUpperCase();
        switch (compressStr) {
            case "UNCOMPRESSED":
                return CompressionType.UNCOMPRESSED;
            case "SNAPPY":
                return CompressionType.SNAPPY;
            case "GZIP":
                return CompressionType.GZIP;
            case "LZ4":
                return CompressionType.LZ4;
            case "ZSTD":
                return CompressionType.ZSTD;
            case "LZMA2":
                return CompressionType.LZMA2;
            default:
                throw new RuntimeException("bad input CompressionType：" + compressStr);
        }
    }

    /**
     * 获取第一列
     */
    public List<String> getFirstColumns(String filepath, char delimiter) throws IOException {
        Iterable<CSVRecord> records = this.readCSV_tree(filepath, delimiter);
        List<String> columns_arr = new ArrayList<>();
        for (CSVRecord record : records) {
            // 解析每一行
            Iterator<String> record_iter = record.iterator();
            // 如果以#开头，那么跳过
            String first_cols = record_iter.next();
            if (!first_cols.startsWith("#")) {
                columns_arr.add(first_cols);
            }
        }
        this.reader.close();
        return columns_arr;
    }

    /**
     * 解析csv文件，将第一列device和第二列tsName拼接为完整的path,返回path list
     */
    public List<String> getDeviceAndTs(String filepath) throws IOException {
        Iterable<CSVRecord> records = this.readCSV_tree(filepath, ',');
        List<String> columns_arr = new ArrayList<>();
        for (CSVRecord record : records) {
            // 解析每一行
            Iterator<String> record_iter = record.iterator();
            // 如果以#开头，那么跳过
            String first_cols = record_iter.next();
            if (!first_cols.startsWith("#")) {
                columns_arr.add(first_cols + "." + record_iter.next());
            }
        }
        this.reader.close();
        return columns_arr;
    }

    public List<String> getFirstColumns(String filepath) throws IOException {
        return getFirstColumns(filepath, ',');
    }

    /**
     * 加载 props, attributes and tags 格式csv
     */
    public List<Map<String, String>> loadProps(String filepath) throws IOException {
        Iterable<CSVRecord> records = this.readCSV_tree(filepath, ',');
        List<Map<String, String>> result = new ArrayList<>();
        for (CSVRecord record : records) {
            // 解析每一行
            Iterator<String> recordIter = record.iterator();
            // 如果以#开头，那么跳过
            String first_cols = recordIter.next();
            if (!first_cols.startsWith("#")) {
                if (first_cols.startsWith("m:")) {
                    first_cols = first_cols.substring(2);
                }
                result.add(processMapField(first_cols));
            }
        }
        return result;
    }

    public CustomDataProvider load(String filepath) throws IOException {
        return load(filepath, ',');
    }

    /**
     * 加载csv文件（表模型）
     */
    public CustomDataProvider load_table(String filepath, boolean isSQL) throws IOException {
        // 判断是否是sql语句
        if (isSQL) {
            // 是则调用解析sql的
            return load_sql(filepath);
        } else {
            // 不是则正常解析
            return load(filepath, ',');
        }
    }

    public Iterator<Object[]> getData() throws IOException {
        return testCases.iterator();
    }
}
