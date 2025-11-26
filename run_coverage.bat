@echo off
setlocal enabledelayedexpansion

REM Function: Compile project, run JaCoCo coverage analysis and generate report

echo [INFO] ------------------------------------------------------------------------
echo [INFO] Running coverage analysis
echo [INFO] ------------------------------------------------------------------------

REM Configure variables
set JACOCO_AGENT=jacoco\lib\jacocoagent.jar
set JACOCO_CLI=jacoco\lib\jacococli.jar
set APP_JAR=details\target\details-master.jar
set EXEC_FILE=jacoco.exec
set CLASS_FILES=code\classes
set SOURCE_FILES=code\src
set REPORT_DIR=jacoco\report

REM Check necessary files and directories
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
if exist "%CLASS_FILES%" (
   del /q "%CLASS_FILES%"\*.*
   for /d %%i in ("%CLASS_FILES%"\*) do rmdir /s /q "%%i"
) else (
    mkdir "%CLASS_FILES%"
)
if not exist "%SOURCE_FILES%" (
    echo [ERROR] Source directory %SOURCE_FILES% does not exist
    exit /b 1
)
if not exist "%SOURCE_FILES%\org\apache\iotdb" (
    echo [ERROR] Source code does not exist, please supplement it before execution
    exit /b 1
)

call mvn clean package -DskipTests
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Maven build failed
    exit /b 1
)
timeout /t 2 /nobreak >nul

REM Extract org.apache class files from jar and move to class files directory
echo [INFO] ------------------------------------------------------------------------
echo [INFO] Extracting classes from JAR: %APP_JAR%
echo [INFO] ------------------------------------------------------------------------
jar -xf "%APP_JAR%" org/

if exist org\apache\iotdb (
    xcopy org\apache\iotdb "%CLASS_FILES%\org\apache\iotdb\" /E /I /H /Y >nul
)

if exist org\apache\tsfile (
    xcopy org\apache\tsfile "%CLASS_FILES%\org\apache\tsfile\" /E /I /H /Y >nul
)

REM Clean up temporary directory
if exist org (
    rd /s /q org
)

REM Check if IoTDB and TsFile class directories exist in the class files directory
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

REM Run application and collect coverage data
java -javaagent:%JACOCO_AGENT%=includes=org.apache.*,output=file,destfile=%EXEC_FILE%,append=false -jar %APP_JAR%

REM Add delay waiting to ensure execution completion
timeout /t 5 /nobreak >nul

REM Check if execution data file exists
if not exist "%EXEC_FILE%" (
    echo [ERROR] Execution data file %EXEC_FILE% was not generated
    exit /b 1
)

echo [INFO] ------------------------------------------------------------------------
echo [INFO] Generating coverage report
echo [INFO] ------------------------------------------------------------------------

REM Check if class files are available for report generation
if not exist "%CLASS_FILES%\*" (
    echo [ERROR] No class files found in %CLASS_FILES% for report generation
    exit /b 1
)

REM Generate coverage report
java -jar %JACOCO_CLI% report %EXEC_FILE% --classfiles %CLASS_FILES% --sourcefiles %SOURCE_FILES% --html %REPORT_DIR%

REM Check if the inspection report was generated successfully
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to generate coverage report
    exit /b 1
)

echo [INFO] ------------------------------------------------------------------------
echo [INFO] Coverage analysis completed!
echo [INFO] Report location: %REPORT_DIR%\index.html
echo [INFO] ------------------------------------------------------------------------