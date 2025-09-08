@echo off
setlocal enabledelayedexpansion

REM 脚本名称: run_coverage.bat
REM 功能: 编译项目，运行JaCoCo覆盖率分析并生成报告

REM 配置变量
set JACOCO_AGENT=jacoco\lib\jacocoagent.jar
set JACOCO_CLI=jacoco\lib\jacococli.jar
set APP_JAR=details\target\details-master.jar
set EXEC_FILE=jacoco.exec
set CLASS_FILES=code\classes
set SOURCE_FILES=code\src
set REPORT_DIR=jacoco\report

REM 1. 检查必要文件和目录
if not exist "jacoco\lib" (
    echo Error: Directory jacoco\lib does not exist
    exit /b 1
)
if not exist "%JACOCO_AGENT%" (
    echo Error: File %JACOCO_AGENT% does not exist
    exit /b 1
)
if not exist "%JACOCO_CLI%" (
    echo Error: File %JACOCO_CLI% does not exist
    exit /b 1
)

REM 2. 清理并打包项目（跳过测试）
call mvn clean package -DskipTests

REM 3. 运行应用程序并收集覆盖率数据
java -javaagent:%JACOCO_AGENT%=includes=*,output=file,destfile=%EXEC_FILE%,append=false -jar %APP_JAR%

REM 4. 生成覆盖率报告
java -jar %JACOCO_CLI% report %EXEC_FILE% --classfiles %CLASS_FILES% --sourcefiles %SOURCE_FILES% --html %REPORT_DIR%

echo Coverage analysis completed!
echo Report location: %REPORT_DIR%\index.html

pause
