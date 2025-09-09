package org.apache.iotdb.api.test.tree.template;

import org.apache.iotdb.api.test.BaseTestSuite_TreeModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.api.test.utils.Tools;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

/**
 * Title：测试批量激活模板
 * Describe：测试批量激活模板各种方式
 * Author：肖林捷
 * LastDate：2025/1/14
 */
public class ActiveInBatch extends BaseTestSuite_TreeModel {
    private List<String> paths = new ArrayList<>();
    private List<List<Object>> structures;
    private List<List<Object>> errStructures;
    private static final String databasePrefix = "root.template";
    private static final String templateNamePrefix = "tempGroup";
    private String[] databases = new String[]{"root.templateSingle1", "root.templateSingle2"};
    private String[] loadNodePaths = new String[]{"root.templateSingle1.single", "root.templateSingle2.sub"};
    private static final String tName = "tempGroupSingle";
    private static final String tsName = "s0";
    private static int maxDatabaseLength;

    /**
     * 测试前准备工作
     */
    @BeforeClass
    public void BeforeClass() throws IOException, IoTDBConnectionException, StatementExecutionException {
        // 获取最大数据库名长度
        maxDatabaseLength = Integer.parseInt(ReadConfig.getInstance().getValue("max_database_length"));
        // 获取模板数据结构
        structures = new CustomDataProvider().parseTSStructure("data/tree/ts-structures.csv");
        // 获取错误的模板数据结构
        errStructures = new CustomDataProvider().parseTSStructure("data/tree/ts-structures-error.csv");
        if (verbose) {
            logger.info("######## ActiveInBatch #######");
        }
    }

    /**
     * 获取错误模板名
     */
    @DataProvider(name = "getErrorNames", parallel = false)
    public Iterator<Object[]> getErrorNames() throws IOException {
        return new CustomDataProvider().load("data/tree/names-error.csv").getData();
    }

    /**
     * 获取正常模板名
     */
    @DataProvider(name = "getNormalNames", parallel = false)
    public Iterator<Object[]> getNormalNames() throws IOException {
        return new CustomDataProvider().load("data/tree/names-normal.csv").getData();
    }

    /**
     * 测试激活模板
     */
    @Test(priority = 1, dataProvider = "getNormalNames")
    public void testNormalOne(String name, String comment, String index) throws StatementExecutionException, IoTDBConnectionException, IOException {
        // 获取session
        Session session = PrepareConnection.getSession();
        // 设置数据库和模板名
        String database = databasePrefix + index;
        String templateName = templateNamePrefix + index;
        // 判断数据库是否存在
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        // 用于存储时间序列
        List<String> devicePathList = new ArrayList<>(1);
        devicePathList.add(database + "." + name);
        // 声明一个模板对象
        Template template = new Template(templateName, isAligned);
        // 准备创建模板需要的数据
        List<Object> struct = Tools.getRandom(structures);
        TSDataType tsDataType = (TSDataType) struct.get(0);
        MeasurementNode mNode = new MeasurementNode(name, tsDataType, (TSEncoding) struct.get(1), (CompressionType) struct.get(2));
        template.addToTemplate(mNode);
        // 创建模板
        session.createSchemaTemplate(template);
        // 查看模板是否存在
        assert checkTemplateExists(templateName) : templateName + " 创建模版失败";
        // 挂载模板
        session.setSchemaTemplate(templateName, database);
        // 查看模板是否挂载成功
        assert checkTemplateContainPath(templateName, database) : templateName + " 挂载模版失败";
        // 根据模板创建时间序列（相当于激活模板）
        session.createTimeseriesUsingSchemaTemplate(devicePathList);
        // 判断设备模板的路径（即模板在该路径上已激活，序列已创建）是否存在
        assert devicePathList.size() == getActivePathsCount(templateName, verbose) : templateName + "激活失败";
        // 验证指定路径使用了模板
        assert checkUsingTemplate(devicePathList.get(0), verbose) : devicePathList.get(0) + "没有使用了模版";
        // 写入数据
        insertRecordSingle(database + "." + name + "." + name, tsDataType, isAligned, null);
        // 解除模板
        deactivateTemplate(templateName, devicePathList);
        // 卸载模板
        session.unsetSchemaTemplate(database, templateName);
        // 删除数据库（也相当于解除并卸载设备模板）
        session.deleteStorageGroup(database);
        // 删除模板
        session.dropSchemaTemplate(templateName);
        session.close();
    }

    /**
     * 测试 TIMECHODB-82 问题：测试传入空情况
     */
    @Test(priority = 10)
    public void testNullError() {
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.createTimeseriesUsingSchemaTemplate(null);
        });
        paths.add(null);
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        paths.add(databases[0]);
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
    }

    /**
     * 测试 createTimeseriesUsingSchemaTemplate 方法传入空集合情况
     */
//    @Test(priority = 20, expectedExceptions = StatementExecutionException.class)
    public void testEmptyError() throws IoTDBConnectionException, StatementExecutionException {
        // TODO：【需优化】createTimeseriesUsingSchemaTemplate 方法处理空集合情况不会报错
        session.createTimeseriesUsingSchemaTemplate(new ArrayList<>());
    }

    /**
     * 用于测试 TIMECHODB-76 问题：测试路径为null情况
     */
    @Test(priority = 30, expectedExceptions = StatementExecutionException.class)
    public void testSingleError_databaseNotExist() throws IoTDBConnectionException, StatementExecutionException {
        paths.clear();
        paths.add(loadNodePaths[0]);
        // TODO：之后可能会优化为空情况，优化为不报错
        session.createTimeseriesUsingSchemaTemplate(paths);
    }

    /**
     * 测试错误情况：该路径无挂载模板时激活模板
     */
    @Test(priority = 40, expectedExceptions = StatementExecutionException.class)
    public void testSingleError_normalPath() throws IoTDBConnectionException, StatementExecutionException {
        if (!checkStroageGroupExists(databases[0])) {
            session.setStorageGroup(databases[0]);
        }
        paths.clear();
        paths.add(loadNodePaths[0]);
        // TODO：之后可能会优化为空情况，优化为不报错
        session.createTimeseriesUsingSchemaTemplate(paths);
    }

    /**
     * 测试创建模板
     */
    @Test(priority = 50)
    public void createTemplate() throws IoTDBConnectionException, StatementExecutionException, IOException {
        if (!checkTemplateExists(tName)) {
            int templateCount = getTemplateCount(verbose);
            Template template = new Template(tName, isAligned);
            MeasurementNode mNode = new MeasurementNode(tsName, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
            template.addToTemplate(mNode);
            session.createSchemaTemplate(template);
            assert 1 + templateCount == getTemplateCount(verbose) : "创建模版成功:" + tName;
        }
        for (String database : databases) {
            if (!checkStroageGroupExists(database)) {
                session.createDatabase(database);
            }
            session.setSchemaTemplate(tName, database);
            assert checkTemplateContainPath(tName, database) : "挂载模版成功:" + tName + " - " + database;
        }
    }

    /**
     * 测试通配符命名
     */
    @Test(priority = 51)
    public void testWildcard() {
        paths.clear();
        paths.add(databases[0] + ".d.*");
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        paths.clear();
        paths.add(databases[0] + ".**");
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
    }

    /**
     * 测试先激活父路径然后激活子路径
     */
    @Test(priority = 80)
    public void testActiveDatabaseAfterTemplate() throws IoTDBConnectionException, StatementExecutionException {
        String loadNode = databases[0];
        int expectCount = getCount("count devices " + loadNode, verbose) + 1;
        paths.clear();
        paths.add(loadNode);
        assert checkStroageGroupExists(loadNode) : "database已经存在:" + loadNode;
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices " + loadNode, verbose) : "激活和挂载都是database:" + loadNode;
        insertRecordSingle(loadNode + "." + tsName, TSDataType.INT32, isAligned, null);
        expectCount = getCount("count devices " + loadNode + ".**", verbose) + 1;
        paths.clear();
        paths.add(loadNode + ".sub");
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices " + loadNode + ".**", verbose) : "激活database子path:" + loadNode + ".sub";
        insertRecordSingle(loadNode + ".sub." + tsName, TSDataType.INT32, isAligned, null);
    }

    /**
     * 测试同时在父路径和子路径激活
     */
    @Test(priority = 80)
    public void testActiveParentChild() throws IoTDBConnectionException, StatementExecutionException {
        String database = databases[1];
        String parentPath = database + ".parent";
        String childPath = parentPath + ".child";
        int expectCount = getCount("count devices " + database + ".**", verbose) + 2;
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        paths.clear();
        paths.add(parentPath);
        paths.add(childPath);
        assert checkStroageGroupExists(database) : database + "不存在";
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices " + database + ".**", verbose) : "同时在parent和child路径进行激活";
        insertRecordSingle(parentPath + "." + tsName, TSDataType.INT32, isAligned, null);
        insertRecordSingle(childPath + "." + tsName, TSDataType.INT32, isAligned, null);
    }

    /**
     * 测试重复路径
     */
    @Test(priority = 90)
    public void testDuplicatePath() throws IoTDBConnectionException, StatementExecutionException {
        String doublePath1 = databases[1] + ".dDouble";
        String doublePath2 = databases[1] + ".dDouble2";
        int expectCount = getCount("count devices " + databases[1] + ".**", verbose) + 1;
        paths.clear();
        paths.add(doublePath1);
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getCount("count devices " + databases[1] + ".**", verbose) : "激活：" + databases[1] + "0.dDouble";
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        assert expectCount == getCount("count devices " + databases[1] + ".**", verbose) : "再次激活：" + doublePath2;
        insertRecordSingle(doublePath1 + "." + tsName, TSDataType.INT32, isAligned, null);
        paths.clear();
        paths.add(doublePath2);
        paths.add(doublePath2);
        session.createTimeseriesUsingSchemaTemplate(paths);
        expectCount++;
        assert expectCount == getCount("count devices " + databases[1] + ".**", verbose) : "paths中有重复路径，激活：" + doublePath2;
        insertRecordSingle(doublePath2 + "." + tsName, TSDataType.INT32, isAligned, null);
    }

    /**
     * 测试同时激活包含两个路径的模板
     */
    @Test(priority = 100)
    public void testContain2loadedPath() throws IoTDBConnectionException, StatementExecutionException {
        paths.clear();
        for (int i = 0; i < loadNodePaths.length; i++) {
            paths.add(loadNodePaths[i] + ".db");
        }
        session.createTimeseriesUsingSchemaTemplate(paths);
        for (int i = 0; i < loadNodePaths.length; i++) {
            checkTemplateContainPath(tName, paths.get(i));
        }
    }

    /**
     * 测试包错误路径的模板激活
     */
    @Test(priority = 101)
    public void testContainErrorPath() throws IoTDBConnectionException, StatementExecutionException {
        String database = databasePrefix + "_ab";
        int expectCount = getCount("count devices root.**", verbose);
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        paths.clear();
        paths.add(databases[0] + ".thisIsNormal");
        paths.add(database + ".notLoad");
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        assert expectCount == getCount("count devices root.**", verbose) : "含有未挂载路径";
        paths.clear();
        paths.add(null);
        paths.add(databases[0] + ".thisIsNormal");
        // TIMECHODB-75
        Assert.assertThrows(StatementExecutionException.class, () -> {
            session.createTimeseriesUsingSchemaTemplate(paths);
        });
        assert expectCount == getCount("count devices root.**", verbose) : "含有null";
    }

    /**
     * 测试 TIMECHODB-79 问题
     */
    @Test(priority = 110, dataProvider = "getErrorNames", expectedExceptions = StatementExecutionException.class)
    public void testErrorPath(String name, String comment, String index) throws IoTDBConnectionException, StatementExecutionException {
        paths.clear();
//        paths.add(databases[1]+".dNormal");
        paths.add(databases[1] + "." + name);
        session.createTimeseriesUsingSchemaTemplate(paths);
    }

    /**
     * 测试有效路径
     */
    @Test(priority = 111)
    public void testValidPath() throws IOException, IoTDBConnectionException, StatementExecutionException {
        paths.clear();
        List<String> names = new CustomDataProvider().getFirstColumns("data/tree/names-normal.csv");
        for (int i = 0; i < names.size(); i++) {
            paths.add(databases[1] + "." + names.get(i));
        }
        int expectCount = getDeviceCount(databases[1] + ".**", verbose) + paths.size();
        session.createTimeseriesUsingSchemaTemplate(paths);
        assert expectCount == getDeviceCount(databases[1] + ".**", verbose) : "激活 validPath";
        for (int i = 0; i < paths.size(); i++) {
            insertRecordSingle(paths.get(i) + "." + tsName, TSDataType.INT32, isAligned, null);
        }
    }

    /**
     * 测试创建大型模板
     */
    //    @Test(priority = 130)
    public void createTemplateBig() throws IoTDBConnectionException, StatementExecutionException, IOException {
        String templateName = templateNamePrefix;
        String database = databasePrefix;
        List<IMeasurementSchema> schemaList = new ArrayList<>(structures.size());
        Template template = new Template(templateName, isAligned);
        if (!checkStroageGroupExists(database)) {
            session.createDatabase(database);
        }
        for (int i = 0; i < structures.size(); i++) {
            MeasurementNode mNode = new MeasurementNode("s_" + i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2));
            template.addToTemplate(mNode);
            schemaList.add(new MeasurementSchema("s_" + i, (TSDataType) structures.get(i).get(0),
                    (TSEncoding) structures.get(i).get(1), (CompressionType) structures.get(i).get(2)));
        }
        session.createSchemaTemplate(template);
        assert checkStroageGroupExists(templateName) : "创建模版成功:" + templateName;
        session.setSchemaTemplate(templateName, database);
        assert checkTemplateContainPath(templateName, database) : "挂载模版成功:" + templateName + " - " + database;
        int i = 0;
        StringJoiner sj = new StringJoiner(".");
        sj.add(database);
        while (true) {
            sj.add("level_" + i);
            i++;
            paths.add(sj.toString());
            if (i > 547) {
                int expectCount = getCount("count devices " + database + ".**", verbose) + paths.size();
                session.createTimeseriesUsingSchemaTemplate(paths);
                assert expectCount == getCount("count devices " + database + ".**", verbose) : "激活" + i;
                for (int j = 0; j < paths.size(); j++) {
                    insertTabletMulti(paths.get(j), schemaList, 10, isAligned);
                }
                session.deleteDatabase(database);
                session.createDatabase(database);
            }
        }
    }

}
