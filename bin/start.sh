export CLASSPATH="../lib/*"
export REDIS_CDC_INTEGRATION_TEST_CONFIG=../config
export LOGBACK_CONFIG=../config/logback.xml
echo -------------------------------


java -XX:+HeapDumpOnOutOfMemoryError -Xms256m -Xmx1g -classpath "../lib/*" -Dlogback.configurationFile=$LOGBACK_CONFIG -Dredislabs.integration.test.configLocation=$REDIS_CDC_INTEGRATION_TEST_CONFIG com.redislabs.cdc.integration.test.IntegrationMain $1