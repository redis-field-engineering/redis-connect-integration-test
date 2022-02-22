export CLASSPATH="../lib/*"
export REDIS_CONNECT_INTEGRATION_TEST_CONFIG=../config
export LOGBACK_CONFIG=../config/logback.xml
echo -------------------------------


java -XX:+HeapDumpOnOutOfMemoryError -Xms256m -Xmx1g -classpath "../lib/*" -Dlogback.configurationFile=$LOGBACK_CONFIG -Dredisconnect.integration.test.configLocation=$REDIS_CONNECT_INTEGRATION_TEST_CONFIG com.redis.connect.integration.test.IntegrationMain $1