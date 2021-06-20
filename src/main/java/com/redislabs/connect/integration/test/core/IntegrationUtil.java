package com.redislabs.connect.integration.test.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.redislabs.connect.integration.test.config.IntegrationConfig;
import com.redislabs.connect.integration.test.connections.JDBCConnectionProvider;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Virag Tripathi
 *
 */

@Getter
@Setter
@Slf4j
public class IntegrationUtil {
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private static final CoreConfig coreConfig = new CoreConfig();

    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");
    private static final String redisURI = (String) targetConfig.get("redisUrl");
    private RedisClient redisClient = null;
    private String redisUri;

    /*
    public static String removeTrailingZeroes(String  s) {
        //return s.indexOf(".") < 0 ? s : s.replaceAll("0*$", "").replaceAll("\\.$", "");
        return s.replaceAll("(?!^)0+$", "");
    }
     */

    public static String fmtDouble(double d) {
        return String.format("%s", d);
    }

    public void writeToFile(String file, String content) throws IOException {
        RandomAccessFile stream = new RandomAccessFile(file, "rw");
        // clear the contents before writing new contents
        FileChannel.open(Paths.get(file), StandardOpenOption.WRITE).truncate(0).close();
        FileChannel channel = stream.getChannel();
        byte[] strBytes = content.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(strBytes.length);
        buffer.put(strBytes);
        buffer.flip();
        channel.write(buffer);
        stream.close();
        channel.close();
    }

    public void writeToFileAsJson(String file, Object content) throws IOException {
        RandomAccessFile stream = new RandomAccessFile(file, "rw");
        // clear the contents before writing new contents
        FileChannel.open(Paths.get(file), StandardOpenOption.WRITE).truncate(0).close();
        FileChannel channel = stream.getChannel();
        ObjectMapper om = new ObjectMapper();
        String value = om.writeValueAsString(content);
        byte[] strBytes = value.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(strBytes.length);
        buffer.put(strBytes);
        buffer.flip();
        channel.write(buffer);
        stream.close();
        channel.close();
    }

    public boolean compareJson(JsonElement json1, JsonElement json2) {
        boolean isEqual = true;
        // Check whether both jsonElement are not null
        if(json1 !=null && json2 !=null) {

            // Check whether both jsonElement are objects
            if (json1.isJsonObject() && json2.isJsonObject()) {
                Set<Map.Entry<String, JsonElement>> ens1 = ((JsonObject) json1).entrySet();
                Set<Map.Entry<String, JsonElement>> ens2 = ((JsonObject) json2).entrySet();
                JsonObject json2obj = (JsonObject) json2;
                if (ens1 != null && ens2 != null && (ens2.size() == ens1.size())) {
                    // Iterate JSON Elements with Key values
                    for (Map.Entry<String, JsonElement> en : ens1) {
                        isEqual = isEqual && compareJson(en.getValue() , json2obj.get(en.getKey()));
                    }
                } else {
                    return false;
                }
            }

            // Check whether both jsonElement are arrays
            else if (json1.isJsonArray() && json2.isJsonArray()) {
                JsonArray jarr1 = json1.getAsJsonArray();
                JsonArray jarr2 = json2.getAsJsonArray();
                if(jarr1.size() != jarr2.size()) {
                    return false;
                } else {
                    int i = 0;
                    // Iterate JSON Array to JSON Elements
                    for (JsonElement je : jarr1) {
                        isEqual = isEqual && compareJson(je , jarr2.get(i));
                        i++;
                    }
                }
            }

            // Check whether both jsonElement are null
            else if (json1.isJsonNull() && json2.isJsonNull()) {
                return true;
            }

            // Check whether both jsonElement are primitives
            else if (json1.isJsonPrimitive() && json2.isJsonPrimitive()) {
                if(json1.equals(json2)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else if(json1 == null && json2 == null) {
            return true;
        } else {
            return false;
        }
        return isEqual;
    }

    /*
    public RedisCommands<String, String> traceRedis(RedisClient client) {
        redisClient = client;
        StatefulRedisConnection<String, String> redisConnection = null;

        if (client != null) {
            client =
                    RedisClient.create(
                            DefaultClientResources.builder()
                                    .tracing(BraveTracing.create(Tracing.newBuilder().build()))
                                    .build(),
                            redisUri);

            redisConnection = client.connect();
            log.info("Connected to target Redis at {}", redisUri);
        }

        return redisConnection.sync();
    }
     */

    public RedisCommands<String, String> execRedis(RedisClient client) {
        redisClient = client;
        StatefulRedisConnection<String, String> redisConnection = null;

        if (client != null) {
            redisConnection = client.connect();
            if(log.isDebugEnabled())
                log.debug("Connected to target Redis at {}", redisUri);
        }

        assert redisConnection != null;
        return redisConnection.sync();
    }

    /*
    public JSONArray traceHgetAllAsJsonArray(RedisClient client, String[] keys) {
        redisClient = client;
        client =
                RedisClient.create(
                        DefaultClientResources.builder()
                                .tracing(BraveTracing.create(Tracing.newBuilder().build()))
                                .build(),
                        redisURI);

        StatefulRedisConnection<String, String> redisConnection = client.connect();
        log.info("Connected to target Redis at {}", redisURI);
        RedisCommands<String, String> syncCommands = redisConnection.sync();

        JSONArray value = new JSONArray();
        for (String key : keys) {
            value.add(syncCommands.hgetall(key));
        }

        return value;
    }
     */

    /**
     * @param keys <keys>List of Redis keys.</keys>
     * @return output as org.json.simple.JSONArray
     */
    public JSONArray hgetAllAsJsonArray(RedisClient client, String[] keys) {
        JSONArray value = new JSONArray();
        if (client != null && keys.length != 0) {
            redisClient = client;
            StatefulRedisConnection<String, String> redisConnection = client.connect();
            log.info("Connected to target Redis at {}", redisURI);
            RedisCommands<String, String> syncCommands = redisConnection.sync();

            for (String key : keys) {
                value.add(syncCommands.hgetall(key));
            }
        }

        return value;
    }

    /*
    public ArrayList<String> pkToProcess(String tableName) throws SQLException {
        Connection sqlConnection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
        log.info("Connected to Provider at {}", sqlConnection.toString());

        String[] schemaAndTable = tableName.split("\\.");
        DatabaseMetaData databaseMetaData = sqlConnection.getMetaData();
        ResultSet resultSet = databaseMetaData.getPrimaryKeys(null,schemaAndTable.length == 2 ? schemaAndTable[0] : null,schemaAndTable.length == 2 ? schemaAndTable[1] : schemaAndTable[0]);
        ArrayList<String> primaryKeys = new ArrayList<>();
        while (resultSet.next()) {
            primaryKeys.add(resultSet.getString("COLUMN_NAME").toLowerCase());
        }
        resultSet.close();

        return primaryKeys;
    }
     */

}
