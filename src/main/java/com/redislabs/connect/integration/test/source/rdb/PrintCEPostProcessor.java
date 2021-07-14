package com.redislabs.connect.integration.test.source.rdb;

import com.redislabs.connect.ConnectConstants;
import com.redislabs.connect.core.model.ChangeEvent;
import com.redislabs.connect.core.model.OperationType;
import com.redislabs.connect.mapper.ColumnField;
import com.redislabs.connect.mapper.MapperConfig;
import com.redislabs.connect.mapper.MapperProvider;
import com.redislabs.connect.model.Col;
import com.redislabs.connect.model.Operation;
import com.redislabs.connect.transformer.Transformer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Virag Tripathi
 */

public class PrintCEPostProcessor implements Transformer<ChangeEvent<Map<String, Object>>, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger("redisconnect");

    String userDirectory = System.getProperty("user.dir");

    @Override
    public String getId() {
        return "PRINT_RAW_CE";
    }

    /**
     * Performs this operation on the given arguments.
     *
     * @param changeEvent the first input argument
     * @param o           the second input argument
     */
    @Override
    public void accept(ChangeEvent<Map<String, Object>> changeEvent, Object o) {

        LOGGER.debug("PRINT_RAW_CE : {}", changeEvent);

        if (changeEvent.getPayload() != null && changeEvent.getPayload().get(ConnectConstants.VALUE) != null) {
            Operation op = (Operation) changeEvent.getPayload().get(ConnectConstants.VALUE);

            //String[] keys = op.getCols().getCol().stream().filter(Objects::nonNull).map(Col::getName).toArray(String[]::new);
            //String[] values = op.getCols().getCol().stream().filter(Objects::nonNull).map(Col::getValue).toArray(String[]::new);

            MapperConfig mapperConfig = MapperProvider.INSTANCE.getTableConfig(op.getSchema().concat(".").concat(op.getTable())).getMapper();

            if (mapperConfig != null) {

                String cKey = mapperConfig.getColumns().stream().filter(ColumnField::isKey).map(c -> op.getCols().getCol(c.getTarget()).getValue()).collect(Collectors.joining(":"));
                String dKey = mapperConfig.getColumns().stream().filter(ColumnField::isKey).map(c -> op.getCols().getCol(c.getTarget()).getBefore()).collect(Collectors.joining(":"));

                LOGGER.debug("No. of columns on the source: {}", mapperConfig.getColumns().size());

                JSONObject jsonObject = new JSONObject();

                StringBuffer cBuffer = new StringBuffer();
                cBuffer.append("\"HMSET\"").
                        append(" ").
                        append("\"").
                        append(op.getTable().concat(":").concat(cKey)).
                        append("\" ");

                StringBuffer dBuffer = new StringBuffer();
                dBuffer.append("\"HDEL\"").
                        append(" ").
                        append("\"").
                        append(op.getTable().concat(":").concat(dKey)).
                        append("\" ");
                for (ColumnField columnField : mapperConfig.getColumns()) {
                    Col col = op.getCols().getCol(columnField.getTarget());
                    if ((op.getType().equals(OperationType.I)) || (op.getType().equals(OperationType.C)) ||
                            (op.getType().equals(OperationType.U))) {
                        jsonObject.put("op", op.getType());
                        jsonObject.put("key", op.getTable().concat(":").concat(cKey));
                        cBuffer.append("\"").
                                append(col.getName()).
                                append("\"").
                                append(" ").
                                append("\"").
                                append(col.getValue()).
                                append("\"").
                                append(" ");
                        jsonObject.put("fields", cBuffer);
                    }
                    if (op.getType().equals(OperationType.D)) {
                        jsonObject.put("op", op.getType());
                        jsonObject.put("key", op.getTable().concat(":").concat(dKey));
                        dBuffer.append("\"").
                                append(col.getName()).
                                append("\"").
                                append(" ");
                        jsonObject.put("fields", dBuffer);
                    }
                }

                LOGGER.info("Raw Event - ReadTime={} TxTime={} {}", op.getReadTime(), op.getTxTime(), jsonObject);

                try {
                    if ((op.getType().equals(OperationType.I)) || (op.getType().equals(OperationType.C)) ||
                            (op.getType().equals(OperationType.U))) {
                        writeToFile(userDirectory.concat(File.separator)
                                        .concat("redis-connect-raw-events.out"),
                                cBuffer.toString());
                    }
                    if (op.getType().equals(OperationType.D)) {
                        writeToFile(userDirectory.concat(File.separator)
                                        .concat("redis-connect-raw-events.out"),
                                dBuffer.toString());
                    }
                    if (!jsonObject.isEmpty()) {
                        writeToFile(userDirectory.concat(File.separator)
                                        .concat("redis-connect-raw-events.json"),
                                jsonObject.toString());
                    }
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                    e.printStackTrace();
                }
            }

        }

    }

    /**
     * @param fileName file name with absolute path
     * @param content  contents to write
     * @throws IOException throws IOException
     */
    private void writeToFile(String fileName, String content) throws IOException {
        FileWriter fw = new FileWriter(fileName, true);
        PrintWriter pw = new PrintWriter(fw);
        pw.println(content);
        pw.close();
        fw.close();
    }

}