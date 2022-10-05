package com.redis.connect.integration.test.source.rdb;

import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobDTO;
import com.redis.connect.dto.JobSourceDTO;
import com.redis.connect.dto.JobSourceTableColumnDTO;
import com.redis.connect.pipeline.event.translator.transformer.impl.BaseTransformer;
import com.redis.connect.utils.ConnectConstants;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import static com.redis.connect.utils.ConnectConstants.*;

/**
 * @author Virag Tripathi
 */

public class PrintChangeEventTransformer extends BaseTransformer {

    private static final String WHOAMI = "PrintChangeEventTransformer";

    private static final String OUT_FILE_PREFIX = "redis.connect.";
    private static final String OUT_FILE_SUFFIX = ".raw-events.out";

    private JobSourceDTO jobSourceDTO;
    private final Map<String, FileWriter> fileWriters = new HashMap<>();
    private final Map<String, PrintWriter> printWriters = new HashMap<>();
    private String userDirectory;

    public PrintChangeEventTransformer(String jobId, String jobType, JobDTO job) {
        super(jobId, jobType, job);
    }

    @Override
    public void init() throws Exception {
        jobSourceDTO = job.getSource();
        userDirectory = System.getProperty("redis.connect.outfile.path", System.getProperty("user.dir"));
    }

    @Override
    public void accept(ChangeEventDTO<Map<String, Object>> changeEvent, Object o) {
        try {
            if (LOGGER.isTraceEnabled())
                LOGGER.trace("Instance: {} entered Transformer: {}", instanceId, WHOAMI);

            if (!changeEvent.isValid())
                LOGGER.error("Instance: " + instanceId + " received an invalid transition event payload for JobId: " + jobId);

            for (Map<String, Object> payload : changeEvent.getPayloads()) {

                String schemaAndTableName = (String) payload.get(CHANGE_EVENT_SCHEMA_AND_TABLE);

                if (!fileWriters.containsKey(schemaAndTableName)) {
                    FileWriter fileWriter = new FileWriter(userDirectory.concat(File.separator).concat(OUT_FILE_PREFIX + schemaAndTableName + OUT_FILE_SUFFIX), true);
                    fileWriters.put(schemaAndTableName, fileWriter);
                    printWriters.put(schemaAndTableName, new PrintWriter(fileWriter, true));
                }

                JobSourceDTO.Table table = jobSourceDTO.getTables().get(schemaAndTableName);
                if (table != null) {

                    Map<String, String> values = (Map<String, String>) payload.get(ConnectConstants.CHANGE_EVENT_VALUES);

                    String primaryKey = (String) payload.get(ConnectConstants.CHANGE_EVENT_PRIMARY_KEY);

                    if(LOGGER.isTraceEnabled())
                        LOGGER.trace("Instance: {} {} #columns: {} pre-configured for this table", instanceId, WHOAMI, table.getColumns().size());

                    StringBuilder saveOperationsStringBuilder = new StringBuilder();
                    saveOperationsStringBuilder.append("\"HSET\"").append(" ").append("\"").append(primaryKey).append("\" ");

                    StringBuilder deleteOperationsStringBuilder = new StringBuilder();
                    deleteOperationsStringBuilder.append("\"UNLINK\"").append(" ").append("\"").append(primaryKey).append("\" ");

                    String operationType = (String) payload.get(ConnectConstants.CHANGE_EVENT_OPERATION_TYPE);

                    for (JobSourceTableColumnDTO tableColumn : table.getColumns()) {

                        if (operationType.equals(CHANGE_EVENT_OPERATION_CREATE) || operationType.equals(CHANGE_EVENT_OPERATION_UPDATE)) {
                            saveOperationsStringBuilder.append("\"").append(tableColumn.getTargetColumn()).append("\"").append(" ")
                                    .append("\"").append(values.get(tableColumn.getTargetColumn())).append("\"").append(" ");
                        }

                        if (operationType.equals(CHANGE_EVENT_OPERATION_DELETE)) {
                            deleteOperationsStringBuilder.append("\"").append(tableColumn.getTargetColumn()).append("\"").append(" ");
                        }
                    }

                    if (LOGGER.isTraceEnabled())
                        LOGGER.trace("Instance: {} Transformer: {} TxTime: {} Payload: {}", instanceId, WHOAMI, changeEvent.getSourceTxTime(), payload);

                    if ((operationType.equals(CHANGE_EVENT_OPERATION_CREATE)) || (operationType.equals(CHANGE_EVENT_OPERATION_UPDATE)))
                        printWriters.get(schemaAndTableName).println(saveOperationsStringBuilder);

                    if (operationType.equals(CHANGE_EVENT_OPERATION_DELETE))
                        printWriters.get(schemaAndTableName).println(deleteOperationsStringBuilder);
                }
            }
        } catch (Exception e) {
            //Since this class if for internal use it's ok to consume this exception
            LOGGER.error("Instance: {} {} MESSAGE: {} STACKTRACE: {}", instanceId, WHOAMI, ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }
    }

    @Override
    public void destroy() throws Exception {

        for(PrintWriter printWriter : printWriters.values()) {
            if(printWriter != null) {
                printWriter.close();
            }
        }
        printWriters.clear();

        for(FileWriter fileWriter : fileWriters.values()) {
            if(fileWriter != null) {
                fileWriter.close();
            }
        }
        fileWriters.clear();
    }

}