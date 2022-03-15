package com.redis.connect.integration.test.source.rdb;

import com.redis.connect.dto.ChangeEvent;
import com.redis.connect.dto.OperationType;
import com.redis.connect.pipeline.event.model.Col;
import com.redis.connect.pipeline.event.model.Operation;
import com.redis.connect.pipeline.event.translators.mapper.ColumnField;
import com.redis.connect.pipeline.event.translators.mapper.MapperConfig;
import com.redis.connect.pipeline.event.translators.mapper.MapperProvider;
import com.redis.connect.pipeline.event.translators.transformer.Transformer;
import com.redis.connect.utils.ConnectConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Virag Tripathi
 */

public class PrintCEPostProcessor implements Transformer<ChangeEvent<Map<String, Object>>, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");
    private static final String WHOAMI = "PrintCEPostProcessor";
    private static final String OUT_FILE_PREFIX = "redis.connect.";
    private static final String OUT_FILE_SUFFIX = ".raw-events.out";

    private final String instanceId;

    String userDirectory = System.getProperty("redis.connect.outFile.path", System.getProperty("user.dir"));

    public PrintCEPostProcessor() {
        this.instanceId = ManagementFactory.getRuntimeMXBean().getName();
    }
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

        LOGGER.info("Instance: {} {} PRINT_RAW_CE : {}", instanceId, WHOAMI, changeEvent);

        if (changeEvent.getPayload() != null && changeEvent.getPayload().get(ConnectConstants.VALUE) != null) {
            Operation op = (Operation) changeEvent.getPayload().get(ConnectConstants.VALUE);

            String tableName = op.getSchema() + "." + op.getTable();

            /*
            String[] keys = op.getCols().getCol().stream().filter(Objects::nonNull).map(Col::getName).toArray(String[]::new);
            String[] values = op.getCols().getCol().stream().filter(Objects::nonNull).map(Col::getValue).toArray(String[]::new)
             */

            MapperConfig mapperConfig = MapperProvider.INSTANCE.getTableConfig(op.getSchema().concat(".").concat(op.getTable())).getMapper();

            if (mapperConfig != null) {

                String cKey = mapperConfig.getColumns().stream().filter(ColumnField::isKey).map(c -> op.getCols().getCol(c.getTarget()).getValue()).collect(Collectors.joining(":"));
                String dKey = mapperConfig.getColumns().stream().filter(ColumnField::isKey).map(c -> op.getCols().getCol(c.getTarget()).getBefore()).collect(Collectors.joining(":"));

                LOGGER.info("Instance: {} {} No. of columns on the source: {}", instanceId, WHOAMI, mapperConfig.getColumns().size());

                StringBuilder cBuilder = new StringBuilder();
                cBuilder.append("\"HSET\"").
                        append(" ").
                        append("\"").
                        append(op.getTable().concat(":").concat(cKey)).
                        append("\" ");

                StringBuilder dBuilder = new StringBuilder();
                dBuilder.append("\"HDEL\"").
                        append(" ").
                        append("\"").
                        append(op.getTable().concat(":").concat(dKey)).
                        append("\" ");

                for (ColumnField columnField : mapperConfig.getColumns()) {
                    Col col = op.getCols().getCol(columnField.getTarget());
                    if ((op.getType().equals(OperationType.I)) || (op.getType().equals(OperationType.C)) ||
                            (op.getType().equals(OperationType.U))) {

                        cBuilder.append("\"").
                                append(col.getName()).
                                append("\"").
                                append(" ").
                                append("\"").
                                append(col.getValue()).
                                append("\"").
                                append(" ");
                    }
                    if (op.getType().equals(OperationType.D)) {
                        dBuilder.append("\"").
                                append(col.getName()).
                                append("\"").
                                append(" ");
                    }
                }

                LOGGER.info("Instance: {} {} Raw Event - ReadTime={} TxTime={} {}", instanceId, WHOAMI, op.getReadTime(), op.getTxTime(), changeEvent);

                try {
                    if ((op.getType().equals(OperationType.I)) || (op.getType().equals(OperationType.C)) ||
                            (op.getType().equals(OperationType.U))) {
                        writeToFile(userDirectory.concat(File.separator)
                                        .concat(OUT_FILE_PREFIX + tableName + OUT_FILE_SUFFIX),
                                cBuilder.toString());
                    }
                    if (op.getType().equals(OperationType.D)) {
                        writeToFile(userDirectory.concat(File.separator)
                                        .concat(OUT_FILE_PREFIX + tableName + OUT_FILE_SUFFIX),
                                dBuilder.toString());
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
