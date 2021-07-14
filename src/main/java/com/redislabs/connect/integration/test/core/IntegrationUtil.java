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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.simple.JSONArray;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * @author Virag Tripathi
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

    /**
     * @param fileName <fileName>Name of the file.</fileName>
     * @return Array result
     */
    public String[] readFileInToArray(String fileName) {
        List<String> lines = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.length() > 0) {
                    line = line.replaceAll("\" ", "|").replaceAll("\"", "");
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }
        return lines.toArray(new String[0]);

    }

    /**
     * @param fileName file name with absolute path
     * @param content  contents to write
     * @throws IOException throws IOException
     */
    public void writeToFile(String fileName, String content) throws IOException {
        FileWriter fw = new FileWriter(fileName, true);
        PrintWriter pw = new PrintWriter(fw);
        pw.println(content);
        pw.close();
        fw.close();
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

    public void appendToFileAsJson(String file, Object content) throws IOException {
        Set<StandardOpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.CREATE);
        options.add(StandardOpenOption.APPEND);
        FileChannel channel = FileChannel.open(Paths.get(file), options);
        ObjectMapper om = new ObjectMapper();
        String value = om.writeValueAsString(content);
        byte[] strBytes = value.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(strBytes.length);
        buffer.put(strBytes);
        buffer.flip();
        channel.write(buffer);
        channel.close();
    }

    public boolean equalMaps(Map<String, String> m1, Map<String, String> m2) {
        if (m1.size() != m2.size())
            return false;
        for (String key : m1.keySet())
            if (!m1.get(key).equals(m2.get(key)))
                return false;
        return true;
    }

    public boolean compareJson(JsonElement json1, JsonElement json2) {
        boolean isEqual = true;
        // Check whether both jsonElement are not null
        if (json1 != null && json2 != null) {

            // Check whether both jsonElement are objects
            if (json1.isJsonObject() && json2.isJsonObject()) {
                Set<Map.Entry<String, JsonElement>> ens1 = ((JsonObject) json1).entrySet();
                Set<Map.Entry<String, JsonElement>> ens2 = ((JsonObject) json2).entrySet();
                JsonObject json2obj = (JsonObject) json2;
                if (ens1 != null && ens2 != null && (ens2.size() == ens1.size())) {
                    // Iterate JSON Elements with Key values
                    for (Map.Entry<String, JsonElement> en : ens1) {
                        isEqual = isEqual && compareJson(en.getValue(), json2obj.get(en.getKey()));
                    }
                } else {
                    return false;
                }
            }

            // Check whether both jsonElement are arrays
            else if (json1.isJsonArray() && json2.isJsonArray()) {
                JsonArray jarr1 = json1.getAsJsonArray();
                JsonArray jarr2 = json2.getAsJsonArray();
                if (jarr1.size() != jarr2.size()) {
                    return false;
                } else {
                    int i = 0;
                    // Iterate JSON Array to JSON Elements
                    for (JsonElement je : jarr1) {
                        isEqual = isEqual && compareJson(je, jarr2.get(i));
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
                if (json1.equals(json2)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else if (json1 == null && json2 == null) {
            return true;
        } else {
            return false;
        }
        return isEqual;
    }

    public RedisCommands<String, String> execRedis(RedisClient client) {
        redisClient = client;
        StatefulRedisConnection<String, String> redisConnection = null;

        if (client != null) {
            redisConnection = client.connect();
            if (log.isDebugEnabled())
                log.debug("Connected to target Redis at {}", redisUri);
        }

        assert redisConnection != null;
        return redisConnection.sync();
    }

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

}
