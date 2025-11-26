@echo off
setlocal enabledelayedexpansion

REM 脚本名称: run_coverage.bat
REM 功能: 编译项目，运行JaCoCo覆盖率分析并生成报告

echo [INFO] ------------------------------------------------------------------------
echo [INFO] Running coverage analysis
echo [INFO] ------------------------------------------------------------------------

REM 配置变量
set JACOCO_AGENT=jacoco\lib\jacocoagent.jar
set JACOCO_CLI=jacoco\lib\jacococli.jar
set APP_JAR=details\target\details-master.jar
set EXEC_FILE=jacoco.exec
set CLASS_FILES=code\classes
set SOURCE_FILES=code\src
set REPORT_DIR=jacoco\report

REM 检查必要文件和目录
if not exist "jacoco\lib" (
    echo [ERROR] Directory jacoco\lib does not exist
    exit /b 1
)
if not exist "%JACOCO_AGENT%" (
    echo [ERROR] File %JACOCO_AGENT% does not exist
    exit /b 1
)
if not exist "%JACOCO_CLI%" (
    echo [ERROR] File %JACOCO_CLI% does not exist
    exit /b 1
)
if not exist "%CLASS_FILES%" (
    echo [ERROR] Class files directory %CLASS_FILES% does not exist
    exit /b 1
)
if not exist "%SOURCE_FILES%" (
    echo [ERROR] Source files directory %SOURCE_FILES% does not exist
    exit /b 1
)

REM 清理并打包项目
call mvn clean package -DskipTests
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Maven build failed
    exit /b 1
)

REM 添加延迟等待，确保编译完成
timeout /t 2 /nobreak >nul

REM 提取jar中的org.apache类文件并移动到类文件目录
echo [INFO] Extracting classes from JAR: %APP_JAR%
jar -xf "%APP_JAR%" org/

if exist org\apache\iotdb (
    xcopy org\apache\iotdb "%CLASS_FILES%\org\apache\iotdb\" /E /I /H /Y >nul
)

if exist org\apache\tsfile (
    xcopy org\apache\tsfile "%CLASS_FILES%\org\apache\tsfile\" /E /I /H /Y >nul
)

REM 清理临时目录
if exist org (
    rd /s /q org
)

REM 检查类文件目录中是否有IoTDB和TsFile类目录
if not exist "%CLASS_FILES%\org\apache\iotdb" (
    echo [WARN]   - IoTDB classes NOT found
    exit /b 1
)
if not exist "%CLASS_FILES%\org\apache\tsfile" (
    echo [WARN]   - TsFile classes NOT found
    exit /b 1
)

echo [INFO] ------------------------------------------------------------------------
echo [INFO] Running application and collecting coverage data
echo [INFO] ------------------------------------------------------------------------

REM 运行应用程序并收集覆盖率数据
echo [INFO] Starting application with JaCoCo agent
java -javaagent:%JACOCO_AGENT%=includes=org.apache.*,output=file,destfile=%EXEC_FILE%,append=false -jar %APP_JAR%

REM 添加延迟等待，确保执行完成
timeout /t 5 /nobreak >nul

REM 检查执行数据文件是否存在
if not exist "%EXEC_FILE%" (
    echo [ERROR] Execution data file %EXEC_FILE% was not generated
    exit /b 1
)

echo [INFO] ------------------------------------------------------------------------
echo [INFO] Generating coverage report
echo [INFO] ------------------------------------------------------------------------

REM 检查是否有类文件可用于报告生成
if not exist "%CLASS_FILES%\*" (
    echo [ERROR] No class files found in %CLASS_FILES% for report generation
    exit /b 1
)

REM 生成覆盖率报告
java -jar %JACOCO_CLI% report %EXEC_FILE% --classfiles %CLASS_FILES% --sourcefiles %SOURCE_FILES% --html %REPORT_DIR%

REM 检查报告生成是否成功
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to generate coverage report
    exit /b 1
)

REM 清空类目录内容
if exist "%CLASS_FILES%" (
    del /q "%CLASS_FILES%"\*.*
    for /d %%i in ("%CLASS_FILES%"\*) do rmdir /s /q "%%i"
)

echo [INFO] ------------------------------------------------------------------------
echo [INFO] Coverage analysis completed!
echo [INFO] Report location: %REPORT_DIR%\index.html
echo [INFO] ------------------------------------------------------------------------