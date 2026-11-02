#!/usr/bin/env python3
"""把 java-native-api-testcase 已导入 PLM 的用例名称改为中文优先。

名称格式沿用 TsFile 用例的风格：``模块｜场景``。模块中文、场景中文，必要的
API / 类型名保留英文。脚本同时维护仓库内三份台账：

- ``.plm-case-defs.json``：用例定义（name / description）
- ``.plm-case-info.json``：用例元信息（name / plmDescription）
- ``.plm-cn-names.json``：PLM 用例 ID -> 中文名称

用法（PAT 只从环境变量读取，绝不写入文件或命令行参数）::

  export TIMECHO_PLM_BASE_URL=https://plm.infra.timecho.com
  export TIMECHO_PLM_TOKEN=plm_pat_xxx
  python3 scripts/plm/zh_case_names.py --check          # 只校验映射完整性
  python3 scripts/plm/zh_case_names.py --dry-run        # 打印将要写入的名称
  python3 scripts/plm/zh_case_names.py --write-local    # 只更新仓库台账
  python3 scripts/plm/zh_case_names.py --apply          # 更新 PLM + 仓库台账
  python3 scripts/plm/zh_case_names.py --verify         # 回读 PLM 校验
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFS = ROOT / ".plm-case-defs.json"
INFO = ROOT / ".plm-case-info.json"
CN_NAMES = ROOT / ".plm-cn-names.json"

# 每个测试类的模块前缀与「方法 -> 场景」映射。
# 场景名只写场景本身，最终名称为「模块｜场景」。
SPECS = {
    "TestRpcUtils": ("RPC 工具", {
        "testBuildStatusAndResponseWrappers": "构建 Status 与响应包装",
        "testFailureStatusExtractionAndSquash": "失败状态提取与压缩",
        "testSqlDetection": "SQL 类型识别",
        "testTimeFactorAndTimestampConversion": "时间因子与时间戳换算",
        "testTimeFormatAndPrecision": "时间格式与精度",
        "testVerifyStatusListFailure": "校验状态列表失败",
        "testVerifySuccessAndFailureStatus": "校验成功与失败状态",
        "testVerifySuccessWithRedirection": "校验带重定向的成功状态",
        "testVerifySuccessWithRedirectionForMultiDevices": "校验多设备带重定向的成功状态",
    }),
    "org.apache.iotdb.api.test.table.data.TestInsertError": ("表模型-写入异常", {
        "insertError1": "写入数据类型不一致",
        "insertError2": "写入不含 FIELD",
    }),
    "TestInsertNormal": ("表模型-写入", {
        "insertAllNull": "插入全空值",
        "insertAutoCreateColumn": "自动创建列",
        "insertAutoCreateTable": "自动创建表",
        "insertNoNull": "插入无空值",
        "insertNull": "插入含空值",
        "insertOnlyAttr": "仅写属性列",
        "insertOnlyField": "仅写测点列",
        "insertOnlyTag": "仅写标签列",
        "insertOnlyTagAndAttr": "仅写标签与属性列",
        "insertSQL": "使用 SQL 语句插入",
    }),
    "TestSelectNormal": ("表模型-查询", {
        "testDataIteratorException": "数据迭代器异常场景",
        "testDataIteratorNormal": "数据迭代器正常读取",
        "testExtractTimeFunction": "extract 时间函数结果",
        "testFieldNormal": "按 Field 读取结果",
        "testOther": "其他查询接口",
        "testSQL": "执行多种查询 SQL",
    }),
    "TestDataBaseNormal": ("表模型-数据库", {
        "createDataBase": "创建数据库",
        "dropDataBase": "删除数据库",
        "showDataBase": "查看数据库",
        "useDataBase": "切换数据库",
    }),
    "TestTableSessionBuilder": ("表模型-会话构建", {
        "testAll": "全参数组合",
        "testConnectionTimeoutInMs": "连接超时参数",
        "testDataBase": "指定数据库参数",
        "testDefault": "默认参数",
        "testEnableAutoFetch": "自动拉取参数",
        "testEnableRedirection": "重定向参数",
        "testFetchSize": "拉取批大小参数",
        "testMaxRetryCount": "最大重试次数参数",
        "testNodeUrls": "节点地址列表参数",
        "testQueryTimeoutInMs": "查询超时参数",
        "testRetryIntervalInMs": "重试间隔参数",
        "testThriftDefaultBufferSize": "Thrift 默认缓冲区参数",
        "testThriftMaxFrameSize": "Thrift 最大帧大小参数",
        "testUserNameAndPassword": "用户名与密码参数",
        "testZoneId": "时区参数",
    }),
    "TestTableSessionBuilderError": ("表模型-会话构建异常", {
        "testErrorNodeUrlsHost": "节点地址主机错误",
        "testErrorNodeUrlsPort": "节点地址端口错误",
        "testErrorUserNameAndPassword": "用户名或密码错误",
    }),
    "TestTableSessionPoolBuilder": ("表模型-会话池构建", {
        "testAll": "全参数组合",
        "testConnectionTimeoutInMs": "连接超时参数",
        "testDataBase": "指定数据库参数",
        "testDefault": "默认参数",
        "testEnableAutoFetch": "自动拉取参数",
        "testEnableRedirection": "重定向参数",
        "testFetchSize": "拉取批大小参数",
        "testMaxRetryCount": "最大重试次数参数",
        "testMaxSize": "连接池最大连接数参数",
        "testNodeUrls": "节点地址列表参数",
        "testQueryTimeoutInMs": "查询超时参数",
        "testRetryIntervalInMs": "重试间隔参数",
        "testThriftDefaultBufferSize": "Thrift 默认缓冲区参数",
        "testThriftMaxFrameSize": "Thrift 最大帧大小参数",
        "testUserNameAndPassword": "用户名与密码参数",
        "testZoneId": "时区参数",
    }),
    "TestTableNormal": ("表模型-表管理", {
        "createTable": "创建表",
        "describe": "查看表结构",
        "showTables": "查看表列表",
    }),
    "TestInsertAlignedRecordNormal": ("树模型-写入", {
        "insertAlignedRecord": "写入单条对齐记录（构造一）",
        "insertAlignedRecord2": "写入单条对齐记录（构造二）",
    }),
    "TestInsertAlignedRecordsNormal": ("树模型-写入", {
        "insertAlignedRecords": "写入多条对齐记录",
    }),
    "TestInsertAlignedRecordsOfOneDeviceNormal": ("树模型-写入", {
        "insertAlignedRecordsOfOneDevice": "写入同设备多条对齐记录",
    }),
    "TestInsertAlignedTabletNormal": ("树模型-写入", {
        "insertAlignedTablet": "写入单个对齐 Tablet",
    }),
    "TestInsertAlignedTabletsNormal": ("树模型-写入", {
        "insertAlignedTables": "写入多个对齐 Tablet（构造一）",
        "insertAlignedTables2": "写入多个对齐 Tablet（构造二）",
        "insertAlignedTablets3": "写入多个对齐 Tablet（构造三）",
    }),
    "TestInsertRecordNormal": ("树模型-写入", {
        "insertRecord1": "写入单条记录（构造一）",
        "insertRecord2": "写入单条记录（构造二）",
    }),
    "TestInsertRecordsNormal": ("树模型-写入", {
        "insertRecords": "写入多条记录（构造一）",
        "insertRecords2": "写入多条记录（构造二）",
    }),
    "TestInsertRecordsOfOneDeviceNormal": ("树模型-写入", {
        "insertRecordsOfOneDevice": "写入同设备多条记录",
    }),
    "TestInsertTabletNormal": ("树模型-写入", {
        "testSessionInsertTable": "Session 写入 Tablet",
        "testSessionPoolInsertTable": "SessionPool 写入 Tablet",
    }),
    "TestInsertTabletsNormal": ("树模型-写入", {
        "insertTables": "写入多个 Tablet（含空值）",
    }),
    "org.apache.iotdb.api.test.tree.data.insert.TestInsertError": ("树模型-写入异常", {
        "testInsertAlignedRecordsOfOneDeviceError1": "同设备对齐多条记录_元数据与值类型不一致",
        "testInsertAlignedRecordError1": "对齐单条记录_元数据与值类型不一致",
        "testInsertAlignedRecordError3": "对齐单条记录_值与声明类型不一致",
        "testInsertAlignedRecordInferenceError1": "对齐单条记录推断类型_序列类型不一致",
        "testInsertAlignedRecordsError1": "对齐多条记录_元数据与值类型不一致",
        "testInsertAlignedRecordsError3": "对齐多条记录_值与声明类型不一致",
        "testInsertAlignedRecordsInferenceError1": "对齐多条记录推断类型_序列类型不一致",
        "testInsertAlignedStringRecordsOfOneDeviceInferenceError1": "同设备对齐字符串记录推断类型_序列类型不一致",
        "testInsertAlignedTabletError1": "对齐 Tablet_元数据与值类型不一致",
        "testInsertAlignedTabletError2": "对齐 Tablet_数值越界或格式非法",
        "testInsertAlignedTabletsError1": "多个对齐 Tablet_元数据与值类型不一致",
        "testInsertAlignedTabletsError2": "多个对齐 Tablet_数值越界或格式非法",
        "testInsertRecordError1": "单条记录_元数据与值类型不一致",
        "testInsertRecordError3": "单条记录_值与声明类型不一致",
        "testInsertRecordInferenceError1": "单条记录推断类型_序列类型不一致",
        "testInsertRecordsError1": "多条记录_元数据与值类型不一致",
        "testInsertRecordsError3": "多条记录_值与声明类型不一致",
        "testInsertRecordsInferenceError1": "多条记录推断类型_序列类型不一致",
        "testInsertRecordsOfOneDeviceError1": "同设备多条记录_元数据与值类型不一致",
        "testInsertStringRecordsOfOneDeviceInferenceError1": "同设备字符串记录推断类型_序列类型不一致",
        "testInsertTabletError1": "Tablet_元数据与值类型不一致",
        "testInsertTabletError2": "Tablet_数值越界或格式非法",
        "testInsertTabletsError1": "多个 Tablet_元数据与值类型不一致",
        "testInsertTabletsError2": "多个 Tablet_数值越界或格式非法",
        "testTSError": "创建序列_不支持的编码或压缩类型",
    }),
    "TestInsertParams": ("树模型-写入参数", {
        "testInsertRecord_datatypeEmpty": "单条记录_数据类型列表为空",
        "testInsertRecord_sameTS": "单条记录_重复测点",
        "testInsertRecord_sizedatatypeDup": "单条记录_数据类型多于测点",
        "testInsertRecord_sizeTSNameDup": "单条记录_测点名称重复",
        "testInsertRecord_sizeTSNameOver": "单条记录_测点数量多于值",
        "testInsertRecord_sizeValuesDup": "单条记录_值数量多于测点",
        "testInsertRecord_timestampFuture": "单条记录_未来时间戳",
        "testInsertRecord_timestampNegative": "单条记录_负时间戳",
        "testInsertRecord_tsNameEmpty": "单条记录_测点名为空",
        "testInsertRecord_valuesEmpty": "单条记录_值为空列表",
        "testInsertRecord_valuesErrorType": "单条记录_值类型错误",
        "testInsertRecord_valuesNull": "单条记录_值为 null",
        "testInsertRecord_valuesNullIn": "单条记录_值列表含 null",
        "testInsertRecords_sameTS": "多条记录_重复测点",
        "testInsertTablet_NoValue": "Tablet_无值",
        "testInsertTablet_rowIndex0": "Tablet_行索引 0 写入并回读",
        "testInsertTablet_rowIndex2": "Tablet_单行索引写入并回读",
        "testInsertTablet_rowIndexNegative": "Tablet_行索引为负",
        "testInsertTablet_sameTS": "Tablet_重复测点",
        "testInsertTablet_schemaListNull": "Tablet_列结构列表为 null",
        "testInsertTablet_schemaListNullIn": "Tablet_列结构列表含 null",
        "testInsertTablet_schemaListNullIn2": "Tablet_列结构含空测点名",
        "testInsertTablet_schemaOutOfIndex": "Tablet_列索引越界",
        "testInsertTablet_timestampFuture": "Tablet_未来时间戳",
        "testInsertTablet_timestampNegative": "Tablet_负时间戳",
        "testInsertTablet_typeError": "Tablet_值类型错误（Double）",
        "testInsertTablet_typeError2": "Tablet_值类型错误（String）",
    }),
    "TestDataBase": ("树模型-数据库", {
        "test1": "批量删除大量数据库",
        "testDeleteStorageGroup_error": "删除单个存储组_非法路径",
        "testDeleteStorageGroup_exact_normal": "删除单个存储组_精确路径",
        "testDeleteStorageGroup_match_normal": "删除单个存储组_通配路径",
        "testDeleteStorageGroups_empty": "批量删除存储组_空列表",
        "testDeleteStorageGroups_error": "批量删除存储组_非法路径",
        "testDeleteStorageGroups_min": "批量删除存储组_单条",
        "testSetStorageGroup_error": "创建存储组_非法名称",
        "testSetStorageGroup_keyword_normal": "创建存储组_关键字名称",
        "testSetStorageGroup_normal": "创建存储组_合法名称",
    }),
    "TestDatabaseParams": ("树模型-数据库参数", {
        "testCreateChild": "在已有库下创建子库",
        "testCreateDatabaseEmpty": "创建数据库_空名称",
        "testCreateDatabaseNull": "创建数据库_null 名称",
        "testDeleteDatabaseEmpty": "删除单个数据库_空名称",
        "testDeleteDatabaseNonExist": "删除单个数据库_不存在",
        "testDeleteDatabasesEmpty": "批量删除数据库_空列表",
        "testDeleteDatabasesNonExist": "批量删除数据库_不存在",
        "testDeleteDatabasesNull": "批量删除数据库_null 列表",
        "testDeleteSgEmpty": "删除存储组_空名称",
        "testDeleteSgsEmpty": "批量删除存储组_空列表",
        "testDeleteSgsNonExist": "批量删除存储组_不存在",
        "testDeleteSgsNull": "批量删除存储组_null 列表",
        "testDuplicateCreate": "重复创建数据库",
        "testDuplicateDelete": "重复删除数据库",
        "testSetStorageGroupNull": "创建存储组_null 名称",
        "testSetStroageGroupEmpty": "创建存储组_空名称",
        "testSgNonExist": "删除不存在的存储组",
    }),
    "TestSessionBuilderException": ("树模型-会话构建异常", {
        "testSessionBuilderParamException1": "同时指定主机端口与节点地址",
        "testSessionBuilderParamException2": "节点地址为空集合",
    }),
    "TestSessionBuilderNormal": ("树模型-会话构建", {
        "testSessionBuilderAllParam": "全参数构建 Session",
        "testSessionBuilderPartParam": "部分参数构建 Session",
    }),
    "TestSessionNormal": ("树模型-会话", {
        "testSessionGenerate": "多种参数构建 Session",
        "testSessionOtherMethod": "Session 其他方法",
    }),
    "TestSessionPoolBuilderNormal": ("树模型-会话池构建", {
        "testSessionBuilderPartParam": "部分参数构建 SessionPool",
        "testSessionPoolBuilderAllParam": "全参数构建 SessionPool",
    }),
    "TestSessionPoolNormal": ("树模型-会话池", {
        "testSessionPoolGenerate": "全参数构建 SessionPool",
        "testSessionPoolOther": "SessionPool 其他方法",
        "testSessionPoolRejectsEmptyNodeUrls": "节点地址为空集合被拒绝",
        "testSessionPoolResultSetLifecycle": "结果集生命周期",
    }),
    "ActiveInBatch": ("树模型-模板激活", {
        "createTemplate": "创建模板",
        "testActiveDatabaseAfterTemplate": "先激活父路径再激活子路径",
        "testActiveParentChild": "父子路径同时激活",
        "testContain2loadedPath": "激活含两个已挂载路径的模板",
        "testContainErrorPath": "激活含非法路径的模板",
        "testDuplicatePath": "重复路径激活",
        "testErrorPath": "非法路径激活",
        "testNormalOne": "单路径正常激活",
        "testNullError": "传入 null 激活",
        "testSingleError_databaseNotExist": "数据库不存在时激活",
        "testSingleError_normalPath": "无模板挂载路径激活",
        "testValidPath": "有效路径激活",
        "testWildcard": "通配符路径激活",
    }),
    "TestDynamicTemplateMin": ("树模型-动态模板", {
        "testStructure_addAllType": "添加全部类型测点",
        "testStructureNormal_addOne": "添加单个测点",
    }),
    "TestDynamicTemplateMulti": ("树模型-动态模板", {
        "testMultipleTemplates": "多个模板共存",
    }),
    "TestOrdinary": ("树模型-模板动态扩展", {
        "createTemplate": "创建模板",
        "testAdd0": "添加 0 个测点",
        "testAddTSToTemplate_normalNames": "向模板添加合法名称测点",
        "testCreateSingle_struct": "创建多种合法结构模板",
        "testInsert_noTemp": "未挂载模板时写入",
        "testReAdd": "重复添加测点",
        "testReAdd_auto": "自动创建时重复添加测点",
        "testReAdd_inBatchDuplicate": "批量添加重复测点",
        "testReAdd_otherType": "重复添加不同类型测点",
        "testTemplateAndTS": "先建模板后建序列",
        "testTSAndTemplate": "先建序列后挂模板",
    }),
    "TestTemplate": ("树模型-模板", {
        "createTemplate_aligned": "创建对齐模板",
        "testAddDuplicateNodes": "添加重复节点",
        "testCreateSingle_errorStruct": "创建非法结构模板（多组数据）",
        "testCreateSingle_struct": "创建合法结构模板（多组数据）",
        "testCreateTemplate_0TS": "创建 0 个测点的模板",
        "testCreateTemplate_1TS": "创建 1 个测点的模板",
        "testCreateTemplate_structError": "创建非法结构模板（固定用例）",
        "testCreateTemplate_structNormal": "创建合法结构模板（固定用例）",
        "testSet_2template": "同一路径挂载两个模板",
        "testSet_childPath": "挂载子路径",
        "testSet_database": "挂载数据库",
        "testSet_parentPath": "挂载父路径",
    }),
    "TestTemplateAligned": ("树模型-模板（对齐）", {
        "testCreateTemplate": "创建模板",
        "testDelete": "删除模板序列数据",
        "testDropTemplate": "模板卸载校验",
        "testInsert": "写入模板序列数据",
        "testInsertAfterDelete": "删除数据后再次写入",
        "testQuery": "查询模板序列数据",
        "testUpdate": "更新模板序列数据",
    }),
    "TestTemplateAlignedAddTS": ("树模型-模板追加测点（对齐）", {
        "testAddTS": "追加测点",
        "testCreateAlignedTemplate": "创建模板",
        "testInsertAlignedTemplateTS": "写入模板测点数据",
        "testInsertWithoutAdd": "未追加测点直接写入",
        "testUpdateAfterAddTS_alignedTemp": "追加测点后更新数据",
        "testUpdateAlignedTS": "更新模板测点数据",
    }),
    "TestTemplateNonAligned": ("树模型-模板（非对齐）", {
        "testCreateTemplate": "创建模板",
        "testDelete": "删除模板序列数据",
        "testDropTemplate": "模板卸载校验",
        "testInsert": "写入模板序列数据",
        "testInsertAfterDelete": "删除数据后再次写入",
        "testQuery": "查询模板序列数据",
        "testUpdate": "更新模板序列数据",
    }),
    "TestTemplateNonAlignedAddTS": ("树模型-模板追加测点（非对齐）", {
        "testCreateAlignedTemplate": "创建模板",
        "testInsertAlignedTemplateTS": "写入模板测点数据",
        "testInsertWithoutAdd": "未追加测点直接写入",
        "testUpdateAfterAddTS_alignedTemp": "追加测点后更新数据",
        "testUpdateAlignedTS": "更新模板测点数据",
    }),
    "TestTemplateParams": ("树模型-模板参数", {
        "createTemplate": "创建参数测试用模板",
        "testContainsNullTsName": "测点名称列表含 null",
        "testCreateTemplate_empty": "创建空名称模板",
        "testDeactive_emptyPath": "解除激活_空路径",
        "testDeactive_emptyTemplate": "解除激活_空模板名",
        "testDeactive_nonExistPath": "解除激活_路径不存在",
        "testDeactive_nonExistTemplate": "解除激活_模板不存在",
        "testDeactive_nullPath": "解除激活_null 路径",
        "testDeactive_nullTemplate": "解除激活_null 模板名",
        "testDeactive_wildcardPath": "解除激活_通配路径",
        "testDeactive_wildcardPath_noActive": "解除未激活的通配路径",
        "testDropTemplate_empty": "删除模板_空名称",
        "testDropTemplate_invalid": "删除模板_名称非法",
        "testNormal": "模板挂载与解除激活正常流程",
        "testNullTsName": "测点名称为 null",
        "testSet_emptyPath": "挂载模板_空路径",
        "testSet_noPath": "挂载模板_路径不存在",
        "testSet_noTemp": "挂载模板_模板不存在",
        "testSet_noTempEmptyPath": "挂载模板_模板与路径均为空",
        "testSet_noTempRootPath": "挂载模板_模板不存在且路径为根",
        "testUnset_activePath": "解除挂载_已激活路径",
        "testUnSet_emptyPath": "解除挂载_空路径",
        "testUnSet_noPath": "解除挂载_路径不存在",
        "testUnset_wildcardPath": "解除挂载_通配路径",
    }),
    "TestAlignedTS": ("树模型-对齐序列", {
        "testCreateTS": "创建序列",
        "testDelete": "删除数据",
        "testDropTimeseries": "删除序列",
        "testInsert": "写入数据",
        "testInsertAfterDelete": "删除后再次写入",
        "testQuery": "查询数据",
        "testUpdate": "更新数据",
    }),
    "TestAlignedTSAdd": ("树模型-对齐序列追加测点", {
        "testAddTS": "追加测点",
        "testCreateTS": "创建序列",
        "testDelete": "删除数据",
        "testDropTimeseries": "删除序列",
        "testInsert": "写入数据",
        "testInsertAfterDelete": "删除后再次写入",
        "testInsertAfterUpdate": "更新后再次写入",
        "testQuery": "查询数据",
        "testUpdate": "更新数据",
    }),
    "TestAlignedTSMinus": ("树模型-对齐序列删除测点", {
        "testCreateTS": "创建序列",
        "testDelete": "删除数据",
        "testDropTimeseries": "删除序列",
        "testInsert": "写入数据",
        "testInsertAfterDelete": "删除后再次写入",
        "testInsertAfterUpdate": "更新后再次写入",
        "testMinusTS": "删除测点",
        "testQuery": "查询数据",
        "testUpdate": "更新数据",
    }),
    "TestBlendScenario": ("树模型-混布场景", {
        "testCreateAlignedTemplate": "创建对齐模板",
        "testCreateNonAlignedTemplate": "创建非对齐模板",
        "testDropTemplate": "删除模板",
        "testDropTS": "删除时间序列",
        "testInsertAlignedTemplateTS": "写入对齐模板测点数据",
        "testInsertAlignedTS": "写入对齐序列数据",
        "testInsertNonAlignedTemplateTS": "写入非对齐模板测点数据",
        "testInsertNonAlignedTS": "写入非对齐序列数据",
        "testUpdateAlignedTemplateTS": "更新对齐模板测点数据",
        "testUpdateAlignedTS": "更新对齐序列数据",
        "testUpdateNonAlignedTemplateTS": "更新非对齐模板测点数据",
        "testUpdateNonAlignedTS": "更新非对齐序列数据",
    }),
    "TestBlendScenarioAddTS": ("树模型-混布场景追加测点", {
        "testCreateAlignedTemplate": "创建对齐模板",
        "testCreateNonAlignedTemplate": "创建非对齐模板",
        "testDropTemplate": "删除模板",
        "testDropTS": "删除时间序列",
        "testInsertAlignedTemplateTS": "写入对齐模板测点数据",
        "testInsertAlignedTS": "写入对齐序列数据",
        "testInsertNonAlignedTemplateTS": "写入非对齐模板测点数据",
        "testInsertNonAlignedTS": "写入非对齐序列数据",
        "testUpdateAfterAddTS_aligned": "追加测点后更新对齐序列",
        "testUpdateAfterAddTS_nonAligned": "追加测点后更新非对齐序列",
        "testUpdateAfterAddTS_alignedTemp": "追加测点后更新对齐模板数据",
        "testUpdateAfterAddTS_nonAlignedTemp": "追加测点后更新非对齐模板数据",
        "testUpdateAlignedTemplateTS": "更新对齐模板测点数据",
        "testUpdateAlignedTS": "更新对齐序列数据",
        "testUpdateNonAlignedTemplateTS": "更新非对齐模板测点数据",
        "testUpdateNonAlignedTS": "更新非对齐序列数据",
    }),
    "TestCleanUp": ("树模型-清理", {
        "createAlignedTS": "创建对齐时间序列",
        "createNonAlignedTS": "创建非对齐时间序列",
        "createTemplate": "创建对齐模板",
        "createTemplateNonAligned": "创建非对齐模板",
    }),
    "TestNonAlignedTS": ("树模型-非对齐序列", {
        "testCreateTS": "创建序列",
        "testDelete": "删除数据",
        "testDropTimeseries": "删除序列",
        "testInsert": "写入数据",
        "testInsertAfterDelete": "删除后再次写入",
        "testQuery": "查询数据",
        "testUpdate": "更新数据",
    }),
    "TestNonAlignedTSAdd": ("树模型-非对齐序列追加测点", {
        "testAddTS": "追加测点",
        "testCreateTS": "创建序列",
        "testDelete": "删除数据",
        "testDropTimeseries": "删除序列",
        "testInsert": "写入数据",
        "testInsertAfterDelete": "删除后再次写入",
        "testInsertAfterUpdate": "更新后再次写入",
        "testQuery": "查询数据",
        "testUpdate": "更新数据",
    }),
    "TestNonAlignedTSMinus": ("树模型-非对齐序列删除测点", {
        "testCreateTS": "创建序列",
        "testDelete": "删除数据",
        "testDropTimeseries": "删除序列",
        "testInsert": "写入数据",
        "testInsertAfterDelete": "删除后再次写入",
        "testInsertAfterUpdate": "更新后再次写入",
        "testMinusTS": "删除测点",
        "testQuery": "查询数据",
        "testUpdate": "更新数据",
    }),
    "TestTimeSeries": ("树模型-时间序列", {
        "testCreateSingleTimeSeries_error": "创建单条序列_非法参数",
        "testCreateSingleTimeSeries_normal": "创建单条序列_合法参数",
        "testCreateSingleTimeSeriesAligned_normal": "创建单条对齐序列_合法参数",
        "testCreateTimeSeries_10props": "创建序列_10 个属性",
        "testCreateTimeSeries_1props": "创建序列_1 个属性",
        "testCreateTimeSeries_GTprops_error": "创建序列_属性数量超限",
        "testCreateTimeSeries_max_props": "创建序列_最大属性数量",
        "testDeleteError": "删除单条序列_非法路径",
        "testDeleteMultiTimeSeries_error": "批量删除序列_非法路径",
        "testDeleteMultiTS": "批量删除序列_通配路径",
        "testDeleteSingleTimeSeries_error": "删除单条序列_参数非法",
        "testDeleteSingleTimeSeries_normal": "删除单条序列_合法路径",
        "testDeviceAsTS": "设备路径作为序列路径",
        "testDuplicateCreateTS": "重复创建序列",
        "testDuplicateTS": "重复序列路径",
        "testSingle": "单条序列基础流程",
    }),
    "TestTimeSeriesMulti": ("树模型-批量时间序列", {
        "testCreateTimeSeriesMulti_single_error": "批量创建_单条非法参数",
        "testCreateTimeSeriesMultiAligned_inBatch": "批量创建对齐序列",
        "testCreateTimeSeriesMulti_error": "批量创建_非法参数",
        "testCreateTimeSeriesMulti_normal": "批量创建_合法参数",
        "testCreateTimeSeriesMulti_single": "批量创建_单条合法参数",
    }),
    "TestTimeSeriesParams": ("树模型-时间序列参数", {
        "testCreateAligned_nullAlias": "创建对齐序列_别名为 null",
        "testCreateAligned_nullAttrs": "创建对齐序列_属性为 null",
        "testCreateAligned_nullProps": "创建对齐序列_属性列为 null",
        "testCreateAligned_nullTags": "创建对齐序列_标签为 null",
        "testCreateAligned_PropsNullInList": "创建对齐序列_属性列表含 null",
        "testCreateMulti_nullAlias": "批量创建_别名为 null",
        "testCreateMulti_nullAttrs": "批量创建_属性为 null",
        "testCreateMulti_nullProps": "批量创建_属性列为 null",
        "testCreateMulti_nullTags": "批量创建_标签为 null",
        "testCreateTimeSeries_nullAlias_error": "创建单条序列_别名为 null",
        "testCreateTimeSeries_nullAttrs_error": "创建单条序列_属性为 null",
        "testCreateTimeSeries_nullProps_error": "创建单条序列_属性列为 null",
        "testCreateTimeSeries_nullTags_error": "创建单条序列_标签为 null",
    }),
}


def short_class(target: str) -> str:
    return target.split("::", 1)[0].rsplit(".", 1)[-1]


def build_mapping(defs: dict) -> dict:
    """返回 {plmCaseId: {name, description, target}}，任何未覆盖的方法都报错。"""
    mapping = {}
    missing = []
    for case in defs["cases"]:
        target = case["target"]
        fqcn = target.split("::", 1)[0]
        cls = short_class(target)
        method = target.split("::", 1)[1]
        spec = SPECS.get(fqcn) or SPECS.get(cls)
        if not spec or method not in spec[1]:
            missing.append(target)
            continue
        module, scenario = spec[0], spec[1][method]
        name = f"{module}｜{scenario}"
        mapping[case["plmCaseId"]] = {
            "name": name,
            "description": f"验证{scenario}；对应测试程序用例 {target}。",
            "target": target,
        }
    if missing:
        raise SystemExit("以下测试方法缺少中文名称映射：\n" + "\n".join(missing))
    names = [v["name"] for v in mapping.values()]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise SystemExit("中文用例名称重复：\n" + "\n".join(duplicates))
    if len(mapping) != len(defs["cases"]):
        raise SystemExit(f"映射数量 {len(mapping)} 与用例数量 {len(defs['cases'])} 不一致")
    return mapping


def api_request(method: str, path: str, body=None) -> dict:
    base = os.environ.get("TIMECHO_PLM_BASE_URL", "").rstrip("/")
    token = os.environ.get("TIMECHO_PLM_TOKEN", "")
    if not base or not token:
        raise SystemExit("必须设置 TIMECHO_PLM_BASE_URL 与 TIMECHO_PLM_TOKEN")
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json; charset=utf-8"},
    )
    last = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < 3:
                last = exc
                time.sleep(2 ** attempt)
                continue
            raise
        except urllib.error.URLError as exc:
            if attempt < 3:
                last = exc
                time.sleep(2 ** attempt)
                continue
            raise
    raise RuntimeError(str(last))


def write_json_atomic(path: Path, data) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, path)


def apply_local(defs: dict, info: dict, cn_names: dict, mapping: dict) -> None:
    for case in defs["cases"]:
        item = mapping.get(case["plmCaseId"])
        if not item:
            continue
        case["name"] = item["name"]
        case["description"] = item["description"]
    for cid, item in mapping.items():
        cn_names[cid] = item["name"]
        meta = info.get(cid)
        if meta is not None:
            meta["name"] = item["name"]
            meta["plmDescription"] = item["description"]
    write_json_atomic(DEFS, defs)
    write_json_atomic(INFO, info)
    write_json_atomic(CN_NAMES, cn_names)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="只校验映射完整性")
    parser.add_argument("--dry-run", action="store_true", help="打印将写入的名称")
    parser.add_argument("--write-local", action="store_true", help="更新仓库内三份台账")
    parser.add_argument("--apply", action="store_true", help="更新 PLM 并更新仓库台账")
    parser.add_argument("--verify", action="store_true", help="回读 PLM 校验名称")
    args = parser.parse_args()

    defs = json.loads(DEFS.read_text(encoding="utf-8"))
    info = json.loads(INFO.read_text(encoding="utf-8"))
    cn_names = json.loads(CN_NAMES.read_text(encoding="utf-8"))
    mapping = build_mapping(defs)
    print(f"映射完整：{len(mapping)} 条，名称无重复。")

    if args.dry_run:
        for cid, item in mapping.items():
            print(f"{cid}\t{item['name']}")
        return 0

    if args.write_local:
        apply_local(defs, info, cn_names, mapping)
        print("仓库台账已更新。")
        return 0

    if args.apply:
        # plmId 以 .plm-case-defs.json 为准；逐条按需更新，已一致则跳过。
        changed = 0
        for idx, case in enumerate(defs["cases"], 1):
            cid = case["plmCaseId"]
            item = mapping[cid]
            plm_id = case.get("plmId")
            if not plm_id:
                raise SystemExit(f"{cid} 缺少 plmId，无法更新")
            current = api_request("GET", f"/api/open/test-cases/{plm_id}")
            if current.get("code") != 200:
                raise SystemExit(f"{cid} 查询失败：{current}")
            old = current["data"] or {}
            if old.get("name") == item["name"] and old.get("description") == item["description"]:
                continue
            result = api_request(
                "PUT",
                f"/api/open/test-cases/{plm_id}",
                {"name": item["name"], "description": item["description"]},
            )
            if result.get("code") != 200:
                raise SystemExit(f"{cid} 更新失败：{result}")
            changed += 1
            if idx % 25 == 0:
                print(f"已处理 {idx}/{len(defs['cases'])}，实际更新 {changed} 条。", flush=True)
        apply_local(defs, info, cn_names, mapping)
        print(f"PLM 更新完成：实际更新 {changed} 条，仓库台账已同步。")
        return 0

    if args.verify:
        mismatches = []
        for case in defs["cases"]:
            cid = case["plmCaseId"]
            item = mapping[cid]
            current = api_request("GET", f"/api/open/test-cases/{case['plmId']}")
            if current.get("code") != 200:
                mismatches.append(f"{cid} 查询失败：{current}")
                continue
            data = current["data"] or {}
            if data.get("name") != item["name"]:
                mismatches.append(f"{cid} 名称不一致：{data.get('name')!r} != {item['name']!r}")
        if mismatches:
            print("校验失败：")
            print("\n".join(mismatches))
            return 1
        print(f"校验通过：{len(defs['cases'])} 条名称均与映射一致。")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
