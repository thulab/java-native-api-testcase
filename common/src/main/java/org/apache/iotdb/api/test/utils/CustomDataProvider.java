package org.apache.iotdb.api.test.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.testng.log4testng.Logger;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * 读取csv文件，返回除第一行header外的数据，供给给测试方法使用
 */
public class CustomDataProvider {
    private final Logger logger = Logger.getLogger(CustomDataProvider.class);
    private final List<Object[]> testCases = new ArrayList<>();
    private Reader reader;

    // 统一按资源URL读取，避免针对 Windows/Linux 分别维护路径处理逻辑。
    private Reader openResourceReader(String filepath) throws IOException {
        URL resource = CustomDataProvider.class.getClassLoader().getResource(filepath);
        if (resource == null) {
            throw new FileNotFoundException("Resource not found: " + filepath);
        }

        if ("file".equalsIgnoreCase(resource.getProtocol())) {
            try {
                // 文件协议可以安全地转换为 Path，兼容空格和中文路径。
                Path path = Paths.get(resource.toURI());
                logger.info("read csv:" + path.toAbsolutePath());
                return Files.newBufferedReader(path);
            } catch (Exception e) {
                throw new IOException("Failed to open resource file: " + filepath, e);
            }
        }

        // 协议不能直接转 Path，这里退回到 classpath 流读取。
        logger.info("read csv from classpath:" + resource);
        return new BufferedReader(new InputStreamReader(resource.openStream(), StandardCharsets.UTF_8));
    }

    // 读取、解析并跳过首行 header，树模型和表模型共用这段流程。
    private Iterable<CSVRecord> parseCsv(String filepath, CSVFormat csvFormat) throws IOException {
        this.reader = openResourceReader(filepath);
        Iterable<CSVRecord> records = csvFormat.parse(reader);
        Iterator<CSVRecord> iterator = records.iterator();
        if (iterator.hasNext()) {
            iterator.next();
        }
        return records;
    }

    /**
     * 读取并解析CSV文件（树模型）
     */
    public Iterable<CSVRecord> readCSV_tree(String filepath, char delimiter) throws IOException {
        CSVFormat csvformat = CSVFormat.DEFAULT.withDelimiter(delimiter).withEscape('\\').withQuote('"').withIgnoreEmptyLines(true);
        return parseCsv(filepath, csvformat);
    }

    /**
     * 读取并解析CSV文件（表模型的）
     */
    public Iterable<CSVRecord> readCSV_table(String filepath) throws IOException {
        return parseCsv(filepath, CSVFormat.MYSQL);
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
    // list 字段可能显式表达 null，同时兼容字符串列表和 map 列表两种结构。
    private @Nullable List<?> processListField(@NotNull String cols) {
        if (cols.equals("null")) {
            return null;
        } else if (cols.startsWith("m:")) {
            List<Map<String, String>> result = new ArrayList<>();
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
     * 固定解析 ts-structures-plain.csv
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
        return switch (datatypeStr.toLowerCase(Locale.ROOT)) {
            case "boolean" -> TSDataType.BOOLEAN;
            case "int" -> TSDataType.INT32;
            case "long" -> TSDataType.INT64;
            case "float" -> TSDataType.FLOAT;
            case "double" -> TSDataType.DOUBLE;
            case "vector" -> TSDataType.VECTOR;
            case "text" -> TSDataType.TEXT;
            case "string" -> TSDataType.STRING;
            case "timestamp" -> TSDataType.TIMESTAMP;
            case "blob" -> TSDataType.BLOB;
            case "date" -> TSDataType.DATE;
            case "null" -> null;
            default -> throw new RuntimeException("bad input DataType: " + datatypeStr);
        };
    }

    /**
     * 解析编码方式
     */
    // 保留 FREQ 兼容，避免旧 csv 数据集在升级后失效。
    @SuppressWarnings("deprecation")
    public TSEncoding parseEncoding(String encodingStr) {
        return switch (encodingStr.toUpperCase(Locale.ROOT)) {
            case "PLAIN" -> TSEncoding.PLAIN;
            case "DICTIONARY" -> TSEncoding.DICTIONARY;
            case "RLE" -> TSEncoding.RLE;
            case "DIFF" -> TSEncoding.DIFF;
            case "TS_2DIFF" -> TSEncoding.TS_2DIFF;
            case "BITMAP" -> TSEncoding.BITMAP;
            case "GORILLA_V1" -> TSEncoding.GORILLA_V1;
            case "REGULAR" -> TSEncoding.REGULAR;
            case "GORILLA" -> TSEncoding.GORILLA;
            case "ZIGZAG" -> TSEncoding.ZIGZAG;
            case "FREQ" -> TSEncoding.FREQ;
            case "SPRINTZ" -> TSEncoding.SPRINTZ;
            case "RLBE" -> TSEncoding.RLBE;
            default -> throw new RuntimeException("bad input Encoding: " + encodingStr);
        };
    }

    /**
     * 解析压缩方式
     */
    public CompressionType parseCompressionType(String compressStr) {
        // 统一转大写后再分支，避免忽略 toUpperCase() 返回值导致大小写解析不一致。
        String normalizedCompress = compressStr.toUpperCase(Locale.ROOT);
        return switch (normalizedCompress) {
            case "UNCOMPRESSED" -> CompressionType.UNCOMPRESSED;
            case "SNAPPY" -> CompressionType.SNAPPY;
            case "GZIP" -> CompressionType.GZIP;
            case "LZ4" -> CompressionType.LZ4;
            case "ZSTD" -> CompressionType.ZSTD;
            case "LZMA2" -> CompressionType.LZMA2;
            default -> throw new RuntimeException("bad input CompressionType：" + compressStr);
        };
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
