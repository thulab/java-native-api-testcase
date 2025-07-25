package org.apache.iotdb.api.test.sql;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.testng.annotations.Test;

import java.sql.*;


public class TestSQL extends BaseTestSuite_TableModel {

    private static final String[] SQLs =
            new String[]{
                    // 准备数据
                    "create database ainode_table_value_function_test",
                    "use ainode_table_value_function_test",
                    "create table t1(tag1 TAG,tag2 TAG,attr1 ATTRIBUTE,attr2 ATTRIBUTE,int32 INT32,int64 INT64,float FLOAT,double DOUBLE)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(1,'tag1','tag2','attr1','attr2',1,10,100.0,1000.0)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(2,'tag1','tag2','attr1','attr2',1,10,100.0,1000.0)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(3,'tag1','tag2','attr1','attr2',1,10,100.0,1000.0)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(4,'tag1','tag2','attr1','attr2',1,10,100.0,1000.0)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(5,'tag1','tag2','attr1','attr2',1,10,100.0,1000.0)",
                    "create table t2(tag1 TAG,tag2 TAG,attr1 ATTRIBUTE,attr2 ATTRIBUTE,int32 INT32,int64 INT64,float FLOAT,double DOUBLE)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(1,'tag1','tag2','attr1','attr2',-1,-10,-100.0,-1000.0)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(2,'tag1','tag2','attr1','attr2',-1,-10,-100.0,-1000.0)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(3,'tag1','tag2','attr1','attr2',-1,-10,-100.0,-1000.0)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(4,'tag1','tag2','attr1','attr2',-1,-10,-100.0,-1000.0)",
                    "insert into t1(time,tag1,tag2,attr1,attr2,int32,int64,float,double) values(5,'tag1','tag2','attr1','attr2',-1,-10,-100.0,-1000.0)",
                    "create table t3(tag1 TAG,tag2 TAG,attr1 ATTRIBUTE,attr2 ATTRIBUTE,int32 INT32,int64 INT64,float FLOAT,double DOUBLE)",
                    "insert into t3(time,tag1,tag2,attr1,attr2,int64,float,double) values(1,'tag1','tag2','attr1','attr2',10,100.0,1000.0)",
                    "insert into t3(time,tag2,attr1,attr2,int32,float) values(2,'tag2','attr1','attr2',2,200.0)",
                    "insert into t3(time,tag1,tag2,attr1,attr2,int64) values(3,'tag1','tag2','attr1','attr2',30)",
                    "insert into t3(time,tag1,tag2,attr1,attr2,double) values(4,'tag1','tag2','attr1','attr2',4000.0)",
                    "insert into t3(time,tag2,attr1,attr2,int32) values(5,'tag2','attr1','attr2',1)",
                    "create table t10(tag1 TAG,tag2 TAG,attr1 ATTRIBUTE,attr2 ATTRIBUTE,int32 INT32,int64 INT64,float FLOAT,double DOUBLE,timstamp TIMESTAMP);",

                    // 正常情况
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial',preserve_input => true)",
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,int64 from t1) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,float from t1) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,double from t1) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t3) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,int64 from t3) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,float from t3) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,double from t3) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial', output_length => 10, output_start_time => 6, output_interval => 10, timecol => 'time', preserve_input => true, model_options => '')",
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial', model_options => '')",

                    // 错误情况
                    "SELECT * FROM forecast (model_id => '_sundial')",
                    "SELECT * FROM forecast (input => t1)",
                    "SELECT * FROM forecast (input => t1, model_id => '')",
                    "SELECT * FROM forecast (input => t1, model_id => 'no')",
                    "SELECT * FROM forecast (input => (select time, tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial',output_length => 0)",
                    "SELECT * FROM forecast (input => (select time, tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial',timecol => '')",
                    "SELECT * FROM forecast (input => (select time, tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial',timecol => 'tag1')",
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t10) PARTITION BY tag1, model_id => '_sundial',timecol => 'timstamp')",
                    "SELECT * FROM forecast (input => t1 PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time,tag1 from t1) PARTITION BY tag1, model_id => '_sundial')",
                    "SELECT * FROM forecast (input => (select time, tag1,int32,int64,float,double from t1) PARTITION BY tag1, model_id => '_sundial', output_length => 1)",
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t1) PARTITION BY tag1, model_id => '_sundial', model_options => '123')",
                    "SELECT * FROM forecast (input => (select time,tag1,int64 from t1) PARTITION BY tag1, model_id => '_sundial', PREDICATED_COLUMNS => 'tag1')",
                    "SELECT * FROM forecast (input => (select time,tag1,int64 from t1) PARTITION BY tag1, model_id => '_sundial', PREDICATED_COLUMNS => 'no')",
                    "SELECT * FROM forecast (input => (select time,tag1,int32 from t1) PARTITION BY tag1 ORDER BY time DESC, model_id \n" +
                            "=> '_sundial')",
                    // 清理环境
                    "drop DATABASE ainode_table_value_function_test",
            };

    /**
     * 测试SQL
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void runSQL() throws ClassNotFoundException {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        try (Connection connection =
                     DriverManager.getConnection(
                             "jdbc:iotdb://172.20.31.63:6667?sql_dialect=table", "root", "root");
             Statement statement = connection.createStatement()) {

            for (String sql : SQLs) {
                try {
                    statement.execute(sql);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}