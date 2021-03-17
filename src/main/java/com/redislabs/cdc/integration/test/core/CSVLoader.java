package com.redislabs.cdc.integration.test.core;

import com.opencsv.CSVReader;
import com.redislabs.cdc.integration.test.config.IntegrationConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.validator.GenericValidator;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */
@Getter
@Setter
@Slf4j
public class CSVLoader {

    private static final
    String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
    private static final String TABLE_REGEX = "\\$\\{table}";
    private static final String KEYS_REGEX = "\\$\\{keys}";
    private static final String VALUES_REGEX = "\\$\\{values}";

    private Connection connection;
    private char separator;
    private static final Map<String, Object> sourceConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String csvFile = (String) sourceConfig.get("csvFile");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private static final int batchSize = (int) sourceConfig.get("batchSize");
    private static final String sourceJsonFile = (String) sourceConfig.get("sourceJsonFile");

    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");
    private static final String redisURI = (String) targetConfig.get("redisUrl");


    /**
     * Public constructor to build CSVLoader object with
     * Connection details. The connection is closed on success
     * or failure.
     * @param connection <connection>JDBC Connection</connection>
     * @param separator
     */
    public CSVLoader(Connection connection, char separator) {
        this.connection = connection;
        this.separator = separator;
    }

    /**
     * Parse CSV file using OpenCSV library and load in
     * given database table.
     * @param csvFile Input CSV file
     * @param tableName Database table name to import data
     * @param truncateBeforeLoad Truncate the table before inserting
     * 			new records.
     * @throws Exception
     */
    public void loadCSVToTable(String csvFile, String tableName,
                               boolean truncateBeforeLoad) throws Exception {

        CSVReader csvReader = null;
        if(null == this.connection) {
            throw new Exception("Not a valid connection.");
        }
        try {

            csvReader = new CSVReader(new FileReader(csvFile));

        } catch (Exception e) {
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

        String questionmarks = StringUtils.repeat("?,", headerRow.length);
        questionmarks = (String) questionmarks.subSequence(0, questionmarks
                .length() - 1);

        String query = SQL_INSERT.replaceFirst(TABLE_REGEX, tableName);
        query = query
                .replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, separator));
        query = query.replaceFirst(VALUES_REGEX, questionmarks);

        log.info("Query: {}", query);

        String[] rowData;
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = this.connection;
            con.setAutoCommit(false);
            ps = con.prepareStatement(query);

            if(truncateBeforeLoad) {
                //delete data from table before loading csv
                con.createStatement().execute("DELETE FROM " + tableName);
            }

            int count = 0;
            String datePattern = "yyyy-MM-dd hh:mm:ss";
            List<Object> rowDataList = new ArrayList<>();
            String[] table = tableName.split("\\.", tableName.length());

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
                /* TODO
                *   single row, this can be useful to compare with redis*/
                rowDataList.add(rowData);
                log.info("Querying Redis for {} key", table[1]+":"+rowData[0]);


                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
                //

                log.info("Redis record {}",IntegrationUtil.traceRedis(redisURI).hgetall( table[1]+":"+rowData[0]));

                //
            }
            ps.executeBatch(); // insert remaining
            for(int i=0; i < rowDataList.size(); i++){
                log.info("Source record {}", rowDataList.get(i));
            }
            // can create a file with loaded record
            //IntegrationUtil.writeToFileAsJson(sourceJsonFile,rowDataList);
            log.info("Total records to load {}.", count);
            log.info("Loading {} into {} table with batchSize={}.", csvFile, tableName, batchSize);

            con.commit();

            csvReader.close();
        } catch (Exception e) {
            con.rollback();
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from file to database."
                            + e.getMessage());
        } /*finally {
            if (null != ps)
                ps.close();
            if (null != con)
                con.close();

            csvReader.close();
        }*/
    }

}
