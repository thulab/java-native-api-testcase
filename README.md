# java-native-api-test

Test the java native api of iotdb.
This project is developed by java, maven and TestNG.

## required environment

jdk: >= 1.8
maven: >= 3.8.1
mvn clean install -DskipTests 

## configuration

1. The iotdb connection(default is localhost):
```shell
common/src/resources/config.properties
```
2. The iotdb-session dependency: the pom.xml of project
3. Configure running test cases: 

```shell
details/src/resources/testng.xml 
```

（Note: Running in Linux and Windows environments requires modifying the corresponding configuration in org/Apache/iotdb/API/test/utls/Customs Data Provider. java）

## run

```shell
# compile jar
mvn clean package -DskipTests
# run tests and generate html report
mvn surefire-report:report
```

Running time is approximately 20 minutes

## report

After running, here is the reports:
```shell
details/target/site/surefire-report.html
```
![](assets/16843000786395.jpg)



