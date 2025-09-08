#!/bin/bash

# 脚本名称: run_coverage.sh
# 功能: 编译项目，运行JaCoCo覆盖率分析并生成报告

# 设置脚本在遇到错误时停止执行
set -e

# 配置变量
JACOCO_AGENT="jacoco/lib/jacocoagent.jar"        # JaCoCo Java代理jar包路径，用于代码覆盖率收集
JACOCO_CLI="jacoco/lib/jacococli.jar"            # JaCoCo命令行工具jar包路径，用于生成报告
APP_JAR="details/target/details-master.jar"      # 应用程序主jar包路径
EXEC_FILE="jacoco.exec"                          # JaCoCo执行数据文件名，存储覆盖率数据
CLASS_FILES="code/classes"                       # 编译后的class文件目录路径，用于生成报告
SOURCE_FILES="code/src"                          # 源代码文件目录路径，用于生成带源码的报告
REPORT_DIR="jacoco/report"                       # 覆盖率报告输出目录

# 函数：检查文件是否存在
check_file_exists() {
    if [ ! -f "$1" ]; then
        print_message "error: file $1 not found"
        exit 1
    fi
}

# 函数：检查目录是否存在
check_dir_exists() {
    if [ ! -d "$1" ]; then
        print_message "error: dir $1 not found"
        exit 1
    fi
}

# 1. 检查必要文件和目录
check_dir_exists "jacoco/lib"
check_file_exists "$JACOCO_AGENT"
check_file_exists "$JACOCO_CLI"

# 2. 清理并打包项目（跳过测试）
mvn clean package -DskipTests

# 3. 运行应用程序并收集覆盖率数据
java -javaagent:$JACOCO_AGENT=includes=*,output=file,destfile=$EXEC_FILE,append=false -jar $APP_JAR

# 5. 生成覆盖率报告
java -jar $JACOCO_CLI report $EXEC_FILE --classfiles $CLASS_FILES --sourcefiles $SOURCE_FILES --html $REPORT_DIR

print_message "Coverage analysis completed!"
print_message "eport location: $REPORT_DIR/index.html"
