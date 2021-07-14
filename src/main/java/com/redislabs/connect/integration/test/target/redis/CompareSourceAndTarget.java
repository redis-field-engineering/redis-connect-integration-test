package com.redislabs.connect.integration.test.target.redis;

import com.redislabs.connect.integration.test.config.IntegrationConfig;
import com.redislabs.connect.integration.test.core.IntegrationUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import picocli.CommandLine;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Virag Tripathi
 */

@Slf4j
@CommandLine.Command(name = "compare",
        description = "Compares Source and Target raw events in the same sequence as it occurs.")
public class CompareSourceAndTarget implements Runnable {

    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");
    private static final String sourceFile = (String) targetConfig.get("sourceFile");
    private static final String targetFile = (String) targetConfig.get("targetFile");

    @Override
    public void run() {
        compareSourceAndTargetEvents();
    }

    private void compareSourceAndTargetEvents() {
        IntegrationUtil integrationUtil = new IntegrationUtil();

        try {

            File sourceFilePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(sourceFile));
            String sourceFile = sourceFilePath.getAbsolutePath();
            File targetFilePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(targetFile));
            String targetFile = targetFilePath.getAbsolutePath();

            log.info("Going to compare {} source records to {} Redis target records.", sourceFile, targetFile);

            String[] sourceLineArray = integrationUtil.readFileInToArray(sourceFile);
            String[] targetLineArray = integrationUtil.readFileInToArray(targetFile);

            Object[][] sourceArray = new Object[sourceLineArray.length][3];
            Object[][] targetArray = new Object[targetLineArray.length][3];

            for (int i = 0; i < sourceLineArray.length; i++) {
                String[] sourceFields = sourceLineArray[i].split("\\|");
                Map<String, String> fieldsMap = new HashMap<>();
                sourceArray[i][0] = sourceFields[0]; // Operation
                sourceArray[i][1] = sourceFields[1]; // Key

                // Will match any Redis commands that ends with SET
                if (sourceFields[0].matches("^[a-zA-Z]+SET")) {
                    for (int j = 2; j < sourceFields.length; j++) {
                        fieldsMap.put(sourceFields[j], sourceFields[j + 1]);
                        j++;
                    }
                    sourceArray[i][2] = fieldsMap;
                }
            }

            for (int i = 0; i < targetLineArray.length; i++) {
                String[] targetFields = targetLineArray[i].split("\\|");
                Map<String, String> fieldsMap = new HashMap<>();
                //String key = targetFields[0] + "-" + targetFields[1];
                targetArray[i][0] = targetFields[0];
                targetArray[i][1] = targetFields[1];

                if (targetFields[0].matches("^[a-zA-Z]+SET")) {
                    for (int j = 2; j < targetFields.length; j++) {
                        fieldsMap.put(targetFields[j], targetFields[j + 1]);
                        j++;
                    }
                    targetArray[i][2] = fieldsMap;
                }
                // above captures delete as well since we are filtering at the key level
            }

            if (!(sourceArray.length == targetArray.length)) {
                log.info("Source and Target Records do not match.");
                log.info("Source Records: {}, Target Records: {}", sourceArray.length, targetArray.length);
                System.exit(0); // exit since the no. of records don't match
            }
            for (int i = 0; i < sourceArray.length; i++) {
                if (!sourceArray[i][0].equals(targetArray[i][0])) { // match keys
                    log.info("Source and Target Operation did not match.");
                    log.info("Source:{}", Arrays.deepToString(sourceArray[i]));
                    log.info("Target:{}", Arrays.deepToString(targetArray[i]));
                    break;
                }

                if (!sourceArray[i][1].equals(targetArray[i][1])) { // match operation
                    log.info("Source and Target Keys did not match.");
                    log.info("Source:{}", Arrays.deepToString(sourceArray[i]));
                    log.info("Target:{}", Arrays.deepToString(targetArray[i]));
                    break;
                }

                Map<String, String> sourceFieldMap = (Map<String, String>) sourceArray[i][2];

                Map<String, String> targetFieldMap = (Map<String, String>) targetArray[i][2];

                if ((sourceFieldMap != null) && (targetFieldMap != null) &&
                        (!integrationUtil.equalMaps(sourceFieldMap, targetFieldMap))) {
                    log.info("Source and Target attributes did not match.");
                    log.info("Source:{}", Arrays.deepToString(sourceArray[i]));
                    log.info("Target:{}", Arrays.deepToString(targetArray[i]));
                    continue;
                }

                log.info("Matched: Operation={} Key={}", sourceArray[i][0], sourceArray[i][1]);

            }

        } catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }
    }
}
