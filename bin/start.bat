@ECHO OFF
set CLASSPATH=.
set CLASSPATH=%CLASSPATH%;"..\lib\*"
set REDIS_CONNECT_INTEGRATION_TEST_CONFIG=..\config
set LOGBACK_CONFIG=..\config\logback.xml
echo -------------------------------


java -XX:+HeapDumpOnOutOfMemoryError -Xms256m -Xmx1g -classpath "../lib/*" -Dlogback.configurationFile=$LOGBACK_CONFIG -Dredisconnect.integration.test.configLocation=$REDIS_CONNECT_INTEGRATION_TEST_CONFIG com.redis.connect.integration.test.IntegrationMain %1
