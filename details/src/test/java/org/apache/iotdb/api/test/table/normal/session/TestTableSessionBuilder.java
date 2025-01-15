package org.apache.iotdb.api.test.table.normal.session;

import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

/**
 * Title：测试 TableSessionBuilder 接口
 * Describe：测试抽象出现新接口 TableSessionBuilder
 * Author：肖林捷
 * Date：2024/12/29
 */
public class TestTableSessionBuilder  {
    // 用于获取配置文件中的配置
    private static ReadConfig config;
    // url
    private static String LOCAL_URLS;
    private static String LOCAL_URL;

    // 初始化
    static {
        try {
            config = ReadConfig.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 判断是否是集群
        if (config.getValue("is_cluster").equals("true")) {
            LOCAL_URLS = config.getValue("host_nodes");
        } else {
            LOCAL_URL = config.getValue("url");
        }
    }


    /**
     * 测试 nodeUrls 方法
     */
    @Test(priority = 10)
    public void testNodeUrls() {
        if (config.getValue("is_cluster").equals("false")) {
            // 一个节点的url单机
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .nodeUrls(Collections.singletonList(LOCAL_URL))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 测试 username 和 password 方法
     */
    @Test(priority = 20)
    public void testUserNameAndPassword() {
        if (config.getValue("is_cluster").equals("false")) {
            // 单机的root用户
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .username("root")
                                 .password("root")
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("SHOW CURRENT_USER").next().getFields().get(0).getStringValue();
                String expect = "root";
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 集群的root用户
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .username("root")
                                 .password("root")
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("SHOW CURRENT_USER").next().getFields().get(0).getStringValue();
                String expect = "root";
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * 测试 database 方法
     */
    @Test(priority = 30)
    public void testDataBase() {
        if (config.getValue("is_cluster").equals("false")) {
            // 目标 database 不存在
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .database("testDataBase1")
                                 .build()) {
                // 判断是否存在数据库
                try (SessionDataSet dataSet = session.executeQueryStatement("show databases")) {
                    while (dataSet.hasNext()) {
                        String dbName = dataSet.next().getFields().get(0).getStringValue();
                        if (!dbName.equals("information_schema")) {
                            // 删除数据库
                            session.executeNonQueryStatement("drop database " + dbName);
                        }
                    }
                }
                session.executeNonQueryStatement("create database testDataBase1");
                session.executeNonQueryStatement("create table table1 (" +
                        "device_id string TAG, " +
                        "ATTRIBUTE STRING ATTRIBUTE, " +
                        "string string FIELD, " +
                        "text text FIELD," +
                        " DOUBLE DOUBLE FIELD," +
                        " FLOAT FLOAT FIELD, " +
                        "INT64 INT64 FIELD, " +
                        "blob blob FIELD, " +
                        "BOOLEAN BOOLEAN FIELD, " +
                        "INT32 INT32 FIELD," +
                        "timestamp timestamp FIELD," +
                        "date date FIELD)");
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show tables").next().getFields().get(0).toString();
                String expect = "table1";
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
                session.executeNonQueryStatement("drop database testDataBase1");
                session.executeNonQueryStatement("create database testDataBase2");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 目标 database 存在
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .database("testDataBase2")
                                 .build()) {
                // 判断是否和预期符号
                String result = null;
                SessionDataSet dataSet = session.executeQueryStatement("show databases");
                while (dataSet.hasNext()) {
                    String dbName = dataSet.next().getFields().get(0).getStringValue();
                    if (!dbName.equals("information_schema")) {
                        result = dbName;
                    }
                }
                String expect = "testdatabase2";
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
                session.executeNonQueryStatement("drop database testDataBase2");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 3、各种合法的数据库名称
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .database("没问题testDataBase1234__")
                                 .build()) {
                session.executeNonQueryStatement("create database \"没问题testDataBase1234__\"");
                session.executeNonQueryStatement("create table table1 (" +
                        "device_id string TAG, " +
                        "ATTRIBUTE STRING ATTRIBUTE, " +
                        "string string FIELD, " +
                        "text text FIELD," +
                        " DOUBLE DOUBLE FIELD," +
                        " FLOAT FLOAT FIELD, " +
                        "INT64 INT64 FIELD, " +
                        "blob blob FIELD, " +
                        "BOOLEAN BOOLEAN FIELD, " +
                        "INT32 INT32 FIELD," +
                        "timestamp timestamp FIELD," +
                        "date date FIELD)");
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show tables").next().getFields().get(0).toString();
                String expect = "table1";
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
                session.executeNonQueryStatement("drop database \"没问题testDataBase1234__\"");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 4、集群
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .database("没问题testDataBase1234__")
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                session.executeNonQueryStatement("create database \"没问题testDataBase1234__\"");
                session.executeNonQueryStatement("create table table1 (" +
                        "device_id string TAG, " +
                        "ATTRIBUTE STRING ATTRIBUTE, " +
                        "string string FIELD, " +
                        "text text FIELD," +
                        " DOUBLE DOUBLE FIELD," +
                        " FLOAT FLOAT FIELD, " +
                        "INT64 INT64 FIELD, " +
                        "blob blob FIELD, " +
                        "BOOLEAN BOOLEAN FIELD, " +
                        "INT32 INT32 FIELD," +
                        "timestamp timestamp FIELD," +
                        "date date FIELD)");
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show tables").next().getFields().get(0).toString();
                String expect = "table1";
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
                session.executeNonQueryStatement("drop database \"没问题testDataBase1234__\"");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 测试 queryTimeoutInMs 方法
     */
    @Test(priority = 30)
    public void testQueryTimeoutInMs() {
        if (config.getValue("is_cluster").equals("false")) {
            // 设置查询超时为9223372036854775807
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .queryTimeoutInMs(9223372036854775807L)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .queryTimeoutInMs(9223372036854775807L)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 测试 fetchSize 方法
     */
    @Test(priority = 40)
    public void testFetchSize() {
        if (config.getValue("is_cluster").equals("false")) {
            // 设置查询结果的提取大小为2147483647
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .fetchSize(2147483647)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 设置查询结果的提取大小为0
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .fetchSize(0)
                                 .build()) {
                session.executeNonQueryStatement("create database testFetchSize2");
                session.executeNonQueryStatement("use testFetchSize2");
                int expect = 0;
                for (int i = 0; i < 10; i++) {
                    session.executeNonQueryStatement("create table table" + i + " (TAG STRING TAG, attr STRING ATTRIBUTE, m INT32 FIELD)");
                    expect++;
                }
                int result = 0;
                SessionDataSet dataSet = session.executeQueryStatement("show tables");
                while (dataSet.hasNext()) {
                    dataSet.next();
                    result++;
                }
                // 判断是否和预期符号
                assert result == expect : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
                session.executeNonQueryStatement("drop database testFetchSize2");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 3、设置查询结果的提取大小小于0
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .fetchSize(-100)
                                 .build()) {
                session.executeNonQueryStatement("create database testFetchSize3");
                session.executeNonQueryStatement("use testFetchSize3");
                int expect = 0;
                for (int i = 0; i < 10; i++) {
                    session.executeNonQueryStatement("create table table" + i + " (TAG STRING TAG, attr STRING ATTRIBUTE, m INT32 FIELD)");
                    expect++;
                }
                int result = 0;
                SessionDataSet dataSet = session.executeQueryStatement("show tables");
                while (dataSet.hasNext()) {
                    dataSet.next();
                    result++;
                }
                // 判断是否和预期符号
                assert result == expect : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
                session.executeNonQueryStatement("drop database testFetchSize3");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 设置查询结果的提取大小为2147483647
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .fetchSize(2147483647)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                session.executeQueryStatement("show cluster");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 设置查询结果的提取大小为0
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .fetchSize(0)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                session.executeNonQueryStatement("create database testFetchSize2");
                session.executeNonQueryStatement("use testFetchSize2");
                int expect = 0;
                for (int i = 0; i < 10; i++) {
                    session.executeNonQueryStatement("create table table" + i + " (TAG STRING TAG, attr STRING ATTRIBUTE, m INT32 FIELD)");
                    expect++;
                }
                int result = 0;
                SessionDataSet dataSet = session.executeQueryStatement("show tables");
                while (dataSet.hasNext()) {
                    dataSet.next();
                    result++;
                }
                // 判断是否和预期符号
                assert result == expect : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
                session.executeNonQueryStatement("drop database testFetchSize2");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 设置查询结果的提取大小小于0
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .fetchSize(-100)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                session.executeNonQueryStatement("create database testFetchSize3");
                session.executeNonQueryStatement("use testFetchSize3");
                int expect = 0;
                for (int i = 0; i < 10; i++) {
                    session.executeNonQueryStatement("create table table" + i + " (TAG STRING TAG, attr STRING ATTRIBUTE, m INT32 FIELD)");
                    expect++;
                }
                int result = 0;
                SessionDataSet dataSet = session.executeQueryStatement("show tables");
                while (dataSet.hasNext()) {
                    dataSet.next();
                    result++;
                }
                // 判断是否和预期符号
                assert result == expect : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
                session.executeNonQueryStatement("drop database testFetchSize3");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * 测试 zoneId 方法
     */
    @Test(priority = 50)
    public void testZoneId() {
        if (config.getValue("is_cluster").equals("false")) {
            // 设置时区为 America/New_York
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .zoneId(ZoneId.of("America/New_York"))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 设置时区为 UTC+8
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .zoneId(ZoneId.of("UTC+8"))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群  America/New_York
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .zoneId(ZoneId.of("America/New_York"))
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 多个节点的url集群 UTC+8
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .zoneId(ZoneId.of("UTC+8"))
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }


    }

    /**
     * 测试 thriftDefaultBufferSize 方法
     * TODO：目前可以设置无效值，和开发讨论，说之后会确定
     */
    @Test(priority = 60)
    public void testThriftDefaultBufferSize() {
        if (config.getValue("is_cluster").equals("false")) {
            // 设置缓冲区大小为 2147483647
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .thriftDefaultBufferSize(1024)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群 2147483647
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .thriftDefaultBufferSize(1024)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }


    }

    /**
     * 测试 thriftMaxFrameSize 方法
     * TODO：目前可以设置无效值，和开发讨论，说之后会确定
     */
    @Test(priority = 70)
    public void testThriftMaxFrameSize() {
        if (config.getValue("is_cluster").equals("false")) {
            // 设置最大帧大小为 2147483647
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .thriftMaxFrameSize(2147483647)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群 2147483647
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .thriftMaxFrameSize(2147483647)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 测试 enableRedirection 方法
     */
    @Test(priority = 80)
    public void testEnableRedirection() {
        if (config.getValue("is_cluster").equals("false")) {
            // 单机启用重定向
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .enableRedirection(true)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .enableRedirection(true)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // TODO:集群启动重定向，关闭其他节点，往leader写入数据，观察其他节点连接数
        }


    }

    /**
     * 测试 enableAutoFetch 方法
     */
    @Test(priority = 90)
    public void testEnableAutoFetch() {
        if (config.getValue("is_cluster").equals("false")) {
            // 启用自动获取可用数据节点
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .enableAutoFetch(true)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .enableAutoFetch(true)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 集群开启自动获取可用数据节点，重启非leader节点，判断是否能自动获取
            // 集群开启自动获取可用数据节点，只启动leader节点，创建session后，启动其他节点判断是否能自动获取
        }


    }

    /**
     * 测试 maxRetryCount 方法
     */
    @Test(priority = 100)
    public void testMaxRetryCount() {
        if (config.getValue("is_cluster").equals("false")) {
            // 设置连接尝试的最大重试次数为2147483647
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .maxRetryCount(2147483647)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .maxRetryCount(2147483647)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 集群启动重定向，但是设置最大重试次数为0，关闭其他节点，往leader写入数据，观察其他节点连接数
        }


    }

    /**
     * 测试 retryIntervalInMs 方法
     */
    @Test(priority = 110)
    public void testRetryIntervalInMs() {
        if (config.getValue("is_cluster").equals("false")) {
            // 设置重试之间的间隔为2147483647
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .retryIntervalInMs(2147483647)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 多个节点的url集群
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .retryIntervalInMs(2147483647)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 集群启动重定向，但是设置最大重试次数为默认，设置重试之间的间隔为不同时间，关闭其他节点，往leader写入数据，观察其他节点连接数
        }

    }

    /**
     * 测试 useSSL、trustStore 和 trustStorePwd 方法
     * TODO：此测试点需要单独测试，需要在配置文件中更改配置（如下），具体参照SSL 传输数据用户手册文档：https://timechor.feishu.cn/docx/KEkEdh6WBooLXtxjz8BcAJRgnDe
     * enable_thrift_ssl=true
     * key_store_path=私钥位置  # 此处为 C:\\projects\\iotdb\\ssl\\aa.keystore
     * key_store_pwd=私钥密码   # 此次为 123456
     */
    //@Test(priority = 120)
    public void testUseSSLAndTrustStoreAndTrustStorePwd() {
        // 启用安全连接的SSL
        try (ITableSession session =
                     new TableSessionBuilder()
                             .useSSL(true)
                             .trustStore("C:\\projects\\iotdb\\ssl\\aa.truststore")
                             .trustStorePwd("123456")
                             .build()) {
            // 判断是否和预期符号
            String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
            String expect = config.getValue("host");
            assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
            ;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试 enableCompression 方法
     * TODO：此测试点需要单独测试，需要在配置文件中启动配置 dn_rpc_thrift_compression_enable=true
     */
    //@Test(priority = 130)
    public void testEnableCompression() {
        // 启用连接的rpc压缩
        try (ITableSession session =
                     new TableSessionBuilder()
                             .enableCompression(true)
                             .build()) {
            // 判断是否和预期符号
            String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
            String expect = config.getValue("host");
            assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
            ;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试 connectionTimeoutInMs 方法
     * TODO：测试点1，感觉方法没有效果？
     */
    @Test(priority = 140)
    public void testConnectionTimeoutInMs() {
        if (config.getValue("is_cluster").equals("false")) {
            // 设置连接超时时间（毫秒）为最大取值
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .connectionTimeoutInMs(Integer.MAX_VALUE)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 设置连接超时时间（毫秒）为0
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .connectionTimeoutInMs(0)
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 设置连接超时时间（毫秒）为最大取值
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .connectionTimeoutInMs(Integer.MAX_VALUE)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }

            // 设置连接超时时间（毫秒）为0
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .connectionTimeoutInMs(0)
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * 测试接口中方法的默认值
     */
    @Test(priority = 150)
    public void testDefault() {
        if (config.getValue("is_cluster").equals("false")) {
            // 单机下所有的默认情况
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .build()) {
                // 判断是否和预期符号
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("host");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;

            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 集群下所有默认情况
            try (ITableSession session =
                         new TableSessionBuilder()
                                 .nodeUrls(Arrays.asList(LOCAL_URLS.split(",")))
                                 .build()) {
                // 判断是否符合预期
                String result = session.executeQueryStatement("show cluster").next().getFields().get(3).getStringValue();
                String expect = config.getValue("hosts");
                assert result.equals(expect) : result + " != " + expect + " 结果和预期不符合，结果为：" + result + " 预期为：" + expect;
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }


    }

}
