package com.redislabs.connect.integration.test.target.redis;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.redislabs.connect.integration.test.config.IntegrationConfig;
import com.redislabs.connect.integration.test.connections.JDBCConnectionProvider;
import com.redislabs.connect.integration.test.core.CoreConfig;
import com.redislabs.connect.integration.test.core.IntegrationUtil;
import com.redislabs.connect.integration.test.core.ReadFile;
import com.redislabs.connect.integration.test.source.rdb.LoadRDB;
import io.lettuce.core.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONArray;
import picocli.CommandLine;

import java.io.File;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

/**
 *
 * @author Virag Tripathi
 *
 */

@Slf4j
@CommandLine.Command(name = "loadsqlandcompare",
        description = "Load source table with sql inserts and compare them with target JSON objects.")
public class QueryAndCompare implements Runnable {
    private static final ReadFile readFile = new ReadFile();
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private static final Map<String, Object> sourceConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String sourceJsonFile = (String) sourceConfig.get("sourceJsonFile");
    private static final String loadQuery = (String) sourceConfig.get("loadQuery");
    private static final String select = (String) sourceConfig.get("select");
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
        compareSourceAndTargetJson();
    }

    private void compareSourceAndTargetJson() {
        IntegrationUtil integrationUtil = new IntegrationUtil();
        try {
            LoadRDB loadRDB = new LoadRDB();
            loadRDB.run();
            Thread.sleep(1000L);
            File keysInFilePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(keysInFile));

            String keysFromFile = readFile.readFileAsString(keysInFilePath.getAbsolutePath());
            // populate the keysInFile instead of user
            //String query_pkeys = "SELECT " + pKeys + " FROM " + tableName + " ORDER BY " + pKeys + ";";
            String[] keys = keysFromFile.split(";");

            log.info("Got {} Redis keys to process from {}.", keys.length, keysInFilePath.getAbsolutePath());

            /*
            Prepare source JSON object
             */
            // execute the query and create a json output of the source DB
            createSourceJson();
            // read the source json output as JSONArray
            File sourceJsonFilePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(sourceJsonFile));

            JSONArray sourceList = readFile.readFileAsJson(sourceJsonFilePath.getAbsolutePath());
            // parse JSONArray as JSONString
            JsonElement sourceJson = JsonParser.parseString(sourceList.toJSONString());


            // Prepare target JSON object
            RedisClient redisClient = RedisClient.create(redisURI);
            JsonElement targetJson = JsonParser.parseString(integrationUtil.hgetAllAsJsonArray(redisClient, keys).toJSONString());

            if(log.isDebugEnabled()) {
                log.debug("Source:\n {}", sourceJson);
                log.debug("Target:\n {}", targetJson);
            }

            log.info("Going to compare {} source records to {} Redis target records.", keys.length, sourceList.size());

            if (!integrationUtil.compareJson(sourceJson, targetJson)) {
                log.info("Comparison result: {}", integrationUtil.compareJson(sourceJson, targetJson));
                log.info("Source and Target records did not match.");
                log.info("Source:\n {}", sourceJson);
                log.info("Target:\n {}", targetJson);
            }
            else {
                log.info("Source and Target records matched. Hooray!");
                log.info("Comparison result: {}", integrationUtil.compareJson(sourceJson, targetJson));
            }

            //assertEquals(sourceJson, targetJson);

            redisClient.shutdown();
            } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }

    private void createSourceJson() {
        IntegrationUtil integrationUtil = new IntegrationUtil();
        try {
            File selectFilePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(select));
            // Prepare Source data
            if(loadQuery == null) {
                query = readFile.readFileAsString(selectFilePath.getAbsolutePath());
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
                    if (Objects.equals(columnTypeName, "money")) {
                        sourceMap.put(rsmd.getColumnName(columnIndex), IntegrationUtil.fmtDouble(rs.getDouble(columnIndex)));
                        //sourceMap.put(rsmd.getColumnName(columnIndex), rs.getString(columnIndex));
                    } else if (Objects.equals(columnTypeName, "datetime")) {
                        String input = rs.getString(columnIndex).replace( " " , "T" );
                        LocalDateTime ldt = LocalDateTime.parse(input);
                        sourceMap.put(rsmd.getColumnName(columnIndex), ldt.toString());
                    } else {
                        sourceMap.put(rsmd.getColumnName(columnIndex), rs.getString(columnIndex));
                    }
                }
                sourceList.add(sourceMap);
            }
            File sourceJsonFilePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(sourceJsonFile));
            integrationUtil.writeToFileAsJson(sourceJsonFilePath.getAbsolutePath(),sourceList);
            log.info("Got {} rows to process from {}.", sourceList.size(), sourceJsonFilePath.getAbsolutePath());

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

}