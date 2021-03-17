package com.redislabs.cdc.integration.test.target.redis;

import brave.Tracing;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.redislabs.cdc.integration.test.config.IntegrationConfig;
import com.redislabs.cdc.integration.test.connections.JDBCConnectionProvider;
import com.redislabs.cdc.integration.test.core.CSVLoader;
import com.redislabs.cdc.integration.test.core.CoreConfig;
import com.redislabs.cdc.integration.test.core.IntegrationUtil;
import com.redislabs.cdc.integration.test.core.ReadFile;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.tracing.BraveTracing;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONArray;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */

@Slf4j
public class QueryAndCompare implements Runnable {
    private static final ReadFile readFile = new ReadFile();
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private static final Map<String, Object> sourceConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String sourceJsonFile = (String) sourceConfig.get("sourceJsonFile");
    private static final String sourceQueryFile = (String) sourceConfig.get("sourceQueryFile");
    private static final String sourceSqlString = (String) sourceConfig.get("sourceSqlString");
    private static final String csvFile = (String) sourceConfig.get("csvFile");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private static final String primaryKey = (String) sourceConfig.get("pkey");
    private static final int batchSize = (int) sourceConfig.get("batchSize");
    private static String query = null;

    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");
    private static final String redisURI = (String) targetConfig.get("redisUrl");
    private static final String keysInFile = (String) targetConfig.get("keys");

    private final CoreConfig coreConfig = new CoreConfig();

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        loadCSV();
        compareSourceAndTargetJson();
    }

    private void compareSourceAndTargetJson() {
        try {
            Thread.sleep(10000L);
            String keysFromFile = readFile.readFileAsString(keysInFile);
            // read redis.keys and store all of the keys
            String[] keys = keysFromFile.split(";");
            log.info("Got {} Redis keys to process from {}.", keys.length, keysInFile);

            /*
            Prepare source JSON object
             */
            // execute the query and create a json output of the source DB
            createSourceJson();
            // read the source json output as JSONArray
            JSONArray sourceList = readFile.readFileAsJson(sourceJsonFile);
            // parse JSONArray as JSONString
            JsonElement sourceJson= JsonParser.parseString(sourceList.toJSONString());


            // Prepare target JSON object
            JsonElement targetJson = JsonParser.parseString(hgetAll(keys).toJSONString());

            if(log.isDebugEnabled()) {
                log.debug("Source:\n {}", sourceJson);
                log.debug("Target:\n {}", targetJson);
            }

            log.info("Going to compare {} source records to {} Redis target records.", keys.length, sourceList.size());

            if (!IntegrationUtil.compareJson(sourceJson, targetJson)) {
                log.info("Comparison result: {}", IntegrationUtil.compareJson(sourceJson, targetJson));
                log.info("Source and Target records did not match.");
                log.info("Source:\n {}", sourceJson);
                log.info("Target:\n {}", targetJson);
            }
            else {
                log.info("Source and Target records matched. Hooray!");
                log.info("Comparison result: {}", IntegrationUtil.compareJson(sourceJson, targetJson));
            }

            //assertEquals(sourceJson, targetJson);

            } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }

    /**
     * @param keys <keys>List of Redis keys.</keys>
     * @return output as org.json.simple.JSONArray
     */
    private JSONArray hgetAll(String[] keys) {
        //RedisClient redisClient = RedisClient.create(redisURI);
        //StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        //log.info("Connected to target Redis at {}", redisURI);
        //RedisCommands<String, String> syncCommands = redisConnection.sync();
        RedisClient redisClient =
                RedisClient.create(
                        DefaultClientResources.builder()
                                .tracing(BraveTracing.create(Tracing.newBuilder().build()))
                                .build(),
                        redisURI);

        StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        log.info("Connected to target Redis at {}", redisURI);
        RedisCommands<String, String> syncCommands = redisConnection.sync();

        JSONArray value = new JSONArray();
        for (String key : keys) {
            value.add(syncCommands.hgetall(key));
        }

        redisClient.shutdown();

        return value;
    }

    private void createSourceJson() {
        try {
            // Prepare Source data
            if(sourceSqlString == null) {
                query = readFile.readFileAsString(sourceQueryFile);
            }
            ResultSet rs = getSqlData(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // only declare and create a new instance inside the rs processing
            Map<String, Object> sourceMap;
            List<Object> sourceList = new ArrayList<>();

            while (rs.next()) {
                sourceMap = new HashMap<>();
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    String columnTypeName = rsmd.getColumnTypeName(columnIndex);
                    //log.info(columnTypeName);
                    if (columnTypeName == "money") {
                        sourceMap.put(rsmd.getColumnName(columnIndex), IntegrationUtil.removeTrailingZeroes(rs.getString(columnIndex)));
                    } else if (columnTypeName == "datetime") {
                        String input = rs.getString(columnIndex).replace( " " , "T" );
                        LocalDateTime ldt = LocalDateTime.parse(input);
                        sourceMap.put(rsmd.getColumnName(columnIndex), ldt.toString());
                    } else {
                        sourceMap.put(rsmd.getColumnName(columnIndex), rs.getString(columnIndex));
                    }
                }
                sourceList.add(sourceMap);
            }
            IntegrationUtil.writeToFileAsJson(sourceJsonFile,sourceList);
            log.info("Got {} rows to process from {}.", sourceList.size(), sourceJsonFile);

        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private ResultSet getSqlData(String query) {
        ResultSet resultSet = null;
        try {
            Connection sqlConnection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            log.info("Connected to Provider at {}", sqlConnection.toString());

            PreparedStatement preparedStatement = sqlConnection.prepareStatement(query);
            preparedStatement.setFetchSize(batchSize);
            resultSet = preparedStatement.executeQuery();

        } catch(SQLException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        return resultSet;
    }

    private void loadCSV() {
        Connection sqlConnection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
        log.info("Connected to Provider at {}", sqlConnection.toString());
        CSVLoader csvLoader = new CSVLoader(sqlConnection, ',');
        try {
            csvLoader.loadCSVToTable(csvFile, tableName, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}