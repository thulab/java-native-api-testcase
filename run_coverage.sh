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

# 函数：打印消息
print_message() {
    echo "[INFO] $1"
}

# 函数：打印错误消息
print_error() {
    echo "[ERROR] $1"
}

# 函数：打印警告消息
print_warning() {
    echo "[WARN] $1"
}

# 函数：检查文件是否存在
check_file_exists() {
    if [ ! -f "$1" ]; then
        print_error "file $1 not found"
        exit 1
    fi
}

# 函数：检查目录是否存在
check_dir_exists() {
    if [ ! -d "$1" ]; then
        print_error "dir $1 not found"
        exit 1
    fi
}

# 1. 检查必要文件和目录
check_dir_exists "jacoco/lib"
check_file_exists "$JACOCO_AGENT"
check_file_exists "$JACOCO_CLI"
check_dir_exists "$CLASS_FILES"
check_dir_exists "$SOURCE_FILES"

# 2. 清理并打包项目
mvn clean package -DskipTests

# 3. 从jar文件中提取类文件用于报告生成
print_message "Extracting classes from JAR: $APP_JAR"
jar -xf "$APP_JAR" org/

if [ -d "org/apache/iotdb" ]; then
    cp -r org/apache/iotdb "$CLASS_FILES/org/apache/"
fi

if [ -d "org/apache/tsfile" ]; then
    cp -r org/apache/tsfile "$CLASS_FILES/org/apache/"
fi

# 清理临时目录
if [ -d "org" ]; then
    rm -rf org
fi

# 检查类文件目录中是否有IoTDB和TsFile类目录
if [ ! -d "$CLASS_FILES/org/apache/iotdb" ]; then
    print_warning "  - IoTDB classes NOT found"
    exit 1
fi

if [ ! -d "$CLASS_FILES/org/apache/tsfile" ]; then
    print_warning "  - TsFile classes NOT found"
    exit 1
fi

print_message "------------------------------------------------------------------------"
print_message "Running application and collecting coverage data"
print_message "------------------------------------------------------------------------"

# 4. 运行应用程序并收集覆盖率数据
java -javaagent:$JACOCO_AGENT=includes=org.apache.*,output=file,destfile=$EXEC_FILE,append=false -jar $APP_JAR

sleep 5

# 5. 检查执行数据文件是否存在
if [ ! -f "$EXEC_FILE" ]; then
    print_error "Execution data file $EXEC_FILE was not generated"
    exit 1
fi

print_message "------------------------------------------------------------------------"
print_message "Generating coverage report"
print_message "------------------------------------------------------------------------"

# 6. 检查是否有类文件可用于报告生成
if [ ! -d "$CLASS_FILES" ] || [ -z "$(ls -A "$CLASS_FILES")" ]; then
    print_error "No class files found in $CLASS_FILES for report generation"
    exit 1
fi

# 7. 生成覆盖率报告
java -jar $JACOCO_CLI report $EXEC_FILE --classfiles $CLASS_FILES --sourcefiles $SOURCE_FILES --html $REPORT_DIR

# 检查报告生成是否成功
if [ $? -ne 0 ]; then
    print_error "Failed to generate coverage report"
    exit 1
fi

# 清理临时目录
if [ -d "$CLASS_FILES" ]; then
    rm -rf "$CLASS_FILES"
fi

print_message "------------------------------------------------------------------------"
print_message "Coverage analysis completed!"
print_message "Report location: $REPORT_DIR/index.html"
print_message "------------------------------------------------------------------------"