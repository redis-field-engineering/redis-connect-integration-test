package com.redislabs.cdc.integration.test.core;

import brave.Tracing;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.tracing.BraveTracing;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Virag Tripathi
 *
 */

@Getter
@Setter
@Slf4j
public class IntegrationUtil {
    // List of all date formats that we want to parse.
    // Add your own format here.
    public static List<SimpleDateFormat>
            dateFormats = new ArrayList<SimpleDateFormat>() {
        private static final long serialVersionUID = 1L;
        {
            add(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"));
            add(new SimpleDateFormat("M/dd/yyyy"));
            add(new SimpleDateFormat("dd.M.yyyy"));
            add(new SimpleDateFormat("M/dd/yyyy hh:mm:ss a"));
            add(new SimpleDateFormat("dd.M.yyyy hh:mm:ss a"));
            add(new SimpleDateFormat("dd.MMM.yyyy"));
            add(new SimpleDateFormat("dd-MMM-yyyy"));
        }
    };

    public static String removeLeadingZeroes(String s) {
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() > 0 && sb.charAt(0) == '0') {
            sb.deleteCharAt(0);
        }
        return sb.toString();
    }

    public static String removeTrailingZeroes(String s) {
        //return s.indexOf(".") < 0 ? s : s.replaceAll("0*$", "").replaceAll("\\.$", "");
        return s.replaceAll("(?!^)0+$", "");
    }

    public static void writeToFile(String file, String content) throws IOException {
        RandomAccessFile stream = new RandomAccessFile(file, "rw");
        // clear the contents before writing new contents
        FileChannel.open(Paths.get(file), StandardOpenOption.WRITE).truncate(0).close();
        FileChannel channel = stream.getChannel();
        String value = content;
        byte[] strBytes = value.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(strBytes.length);
        buffer.put(strBytes);
        buffer.flip();
        channel.write(buffer);
        stream.close();
        channel.close();
    }

    public static void writeToFileAsJson(String file, Object content) throws IOException {
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

    public static boolean compareJson(JsonElement json1, JsonElement json2) {
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

    /**
     * Convert String with various formats into java.util.Date
     *
     * @param input
     *            Date as a string
     * @return java.util.Date object if input string is parsed
     *          successfully else returns null
     */
    public static Date convertToDate(String input) {
        Date date = null;
        if(null == input) {
            return null;
        }
        for (SimpleDateFormat format : dateFormats) {
            try {
                format.setLenient(false);
                date = format.parse(input);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (date != null) {
                break;
            }
        }

        return date;
    }

    public static RedisCommands<String, String> traceRedis(String redisUri) {
        RedisClient redisClient =
                RedisClient.create(
                        DefaultClientResources.builder()
                                .tracing(BraveTracing.create(Tracing.newBuilder().build()))
                                .build(),
                        redisUri);

        StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        log.info("Connected to target Redis at {}", redisUri);

        return redisConnection.sync();
    }
}
