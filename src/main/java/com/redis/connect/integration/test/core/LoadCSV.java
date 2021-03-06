package com.redis.connect.integration.test.core;

import com.opencsv.CSVReader;
import com.redis.connect.integration.test.config.IntegrationConfig;
import com.redis.connect.integration.test.connections.JDBCConnectionProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.validator.GenericValidator;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

/**
 *
 * @author Virag Tripathi
 *
 */
@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "loadcsvdata",
        description = "Load CSV data to source table.")
public class LoadCSV implements Runnable {

    private String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
    private static final String TABLE_REGEX = "\\$\\{table}";
    private static final String KEYS_REGEX = "\\$\\{keys}";
    private static final String VALUES_REGEX = "\\$\\{values}";

    private Connection connection;
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private CoreConfig coreConfig = new CoreConfig();
    private static final Map<String, Object> sourceConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private static final int batchSize = (int) sourceConfig.get("batchSize");
    private static final String type = (String) sourceConfig.get("type");

    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");
    @CommandLine.Option(names = {"-s", "--separator"}, description = "CSV records separator", paramLabel = "<char>", defaultValue = ",", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private char separator = ',';
    @CommandLine.Option(names = {"-t", "--truncateBeforeLoad"}, description = "Truncate the source table before load", paramLabel = "<boolean>", defaultValue = "true", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private boolean truncateBeforeLoad = true;

    private void loadCSV(boolean truncateBeforeLoad) throws Exception {

        CSVReader csvReader;
        connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
        try {
            String csvFile = (String) sourceConfig.get("csvFile");
            File filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(csvFile));

            csvReader = new CSVReader(new FileReader(filePath));
            log.info("Loading {} into {} table with batchSize={}.", filePath, LoadCSV.tableName, batchSize);

        } catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
            throw new Exception("Error occurred while executing file. "
                    + e.getMessage());
        }

        String[] headerRow = csvReader.readNext();

        if (null == headerRow) {
            throw new FileNotFoundException(
                    "No columns defined in given CSV file." +
                            "Please check the CSV file format.");
        }

        String questionmarks = StringUtils.repeat("?"+getSeparator(), headerRow.length);
        questionmarks = (String) questionmarks.subSequence(0, questionmarks
                .length() - 1);

        String insert_query = SQL_INSERT.replaceFirst(TABLE_REGEX, LoadCSV.tableName);
        insert_query = insert_query
                .replaceFirst(KEYS_REGEX, StringUtils.join(headerRow,
                        getSeparator()));
        insert_query = insert_query.replaceFirst(VALUES_REGEX, questionmarks);

        log.info("Insert Query: {}.", insert_query);

        String[] rowData;
        Connection con;
        PreparedStatement ps;

        try {
            con = this.connection;
            if(truncateBeforeLoad) {
                //delete data from table before loading csv
                con.createStatement().execute("DELETE FROM " + LoadCSV.tableName);
            }

            // INSERT
            ps = con.prepareStatement(insert_query);

            int count = 0;
            String datePattern = "yyyy-MM-dd hh:mm:ss";
            ArrayList<String[]> rowDataList = new ArrayList<>();

            while ((rowData = csvReader.readNext()) != null) {

                int index = 1;
                for (String columnData : rowData) {
                    if (GenericValidator.isDate(columnData, datePattern, true)) {
                        String input = columnData.replace( " " , "T" ) ;
                        LocalDateTime ldt = LocalDateTime.parse( input ) ;
                        ps.setTimestamp(index++, Timestamp.valueOf(ldt));
                    } else if (GenericValidator.isDouble(columnData)) {
                        ps.setDouble(index++, Double.parseDouble(columnData));
                    } else if (GenericValidator.isInt(columnData)) {
                        ps.setInt(index++, Integer.parseInt(columnData));
                    } else {
                        ps.setString(index++, columnData);
                    }
                }
                ps.addBatch();
                rowDataList.add(rowData);

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }

            }
            ps.executeBatch(); // insert remaining

            log.info("Inserted {} row(s) into {} table.", rowDataList.size(), LoadCSV.tableName);
            // close connections
            ps.close();
            csvReader.close();
        } catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from file to database."
                            + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            LoadCSV loadCsv = new LoadCSV();
            loadCsv.loadCSV(isTruncateBeforeLoad());
        } catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }
    }

}