package com.redislabs.connect.integration.test.core;

import com.opencsv.CSVReader;
import com.redislabs.connect.integration.test.config.IntegrationConfig;
import com.redislabs.connect.integration.test.connections.JDBCConnectionProvider;
import io.lettuce.core.RedisClient;
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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */
@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "loadcsvandcompare",
        description = "Load CSV data to source and print live comparisons.")
public class LoadCSVAndCompare implements Runnable {

    private String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
    private static final String TABLE_REGEX = "\\$\\{table}";
    private static final String KEYS_REGEX = "\\$\\{keys}";
    private static final String VALUES_REGEX = "\\$\\{values}";

    private Connection connection;
    private File filePath;
    private ReadFile readFile;
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private CoreConfig coreConfig = new CoreConfig();
    private static final Map<String, Object> sourceConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private static final String csvFile = (String) sourceConfig.get("csvFile");
    private static final String select = (String) sourceConfig.get("select");
    private static final String updatedSelect = (String) sourceConfig.get("updatedSelect");
    private static final String update = (String) sourceConfig.get("update");
    private static final String delete = (String) sourceConfig.get("delete");
    private static final int batchSize = (int) sourceConfig.get("batchSize");
    private static final int iteration = (int) sourceConfig.get("iteration");
    private static final String type = (String) sourceConfig.get("type");

    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");
    private static final String redisURI = (String) targetConfig.get("redisUrl");
    private RedisClient redisClient = null;
    @CommandLine.Option(names = {"-s", "--separator"}, description = "CSV records separator", paramLabel = "<char>", defaultValue = ",", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private char separator = ',';
    @CommandLine.Option(names = {"-t", "--truncateBeforeLoad"}, description = "Truncate the source table before load", paramLabel = "<boolean>")
    private boolean truncateBeforeLoad = true;

    /**
     * Parse CSV file using OpenCSV library and load in
     * given database table.
     * @throws Exception Throws exception
     */
    private void doInsert(Connection connection) throws Exception {
        Instant start = Instant.now();
        CSVReader csvReader;
        PreparedStatement ps;
        IntegrationUtil integrationUtil = new IntegrationUtil();
        try {
            filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(csvFile));

            csvReader = new CSVReader(new FileReader(filePath));
            log.info("Loading {} into {} table with batchSize={}.", filePath, LoadCSVAndCompare.tableName, batchSize);

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

        String questionmarks = StringUtils.repeat("?"+getSeparator(), headerRow.length);
        questionmarks = (String) questionmarks.subSequence(0, questionmarks
                .length() - 1);

        String insert_query = SQL_INSERT.replaceFirst(TABLE_REGEX, LoadCSVAndCompare.tableName);
        insert_query = insert_query
                .replaceFirst(KEYS_REGEX, StringUtils.join(headerRow,
                        getSeparator()));
        insert_query = insert_query.replaceFirst(VALUES_REGEX, questionmarks);

        log.info("Insert Query: {}", insert_query);

        try {
            ps = connection.prepareStatement(insert_query);

            int count = 0;
            String[] rowData;
            ArrayList<String[]> rowDataList = new ArrayList<>();
            String[] table = LoadCSVAndCompare.tableName.split("\\.", LoadCSVAndCompare.tableName.length());

            while ((rowData = csvReader.readNext()) != null) {

                int index = 1;
                for (String columnData : rowData) {
                    if (GenericValidator.isDate(columnData, DateTimeUtil.yyyy_MM_dd_HH_mm_ss.getDisplayName(), true)) {
                        String input = columnData.replace(" ", "T");
                        LocalDateTime ldt = LocalDateTime.parse(input);
                        ps.setTimestamp(index++, Timestamp.valueOf(ldt));
                    } else if ( (GenericValidator.isDate(columnData, DateTimeUtil.yyyy_mm_dd.getDisplayName(),
                            true)) || (GenericValidator.isDate(columnData,
                            DateTimeUtil.yyyy_MM_dd.getDisplayName(), true)) ) {
                        Date d = Date.valueOf(columnData);
                        ps.setDate(index++, d);
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

                if (log.isDebugEnabled()) {
                    log.debug("##### Start Record {} #####", rowData[0]);
                    log.debug("{} record for primary key {}-> {}", type, rowData[0], rowData);
                    log.debug("##### End Record {} #####", rowData[0]);
                }

                for (String[] rows : rowDataList) {
                    log.info("##### Start Record {} #####", rows[0]);
                    log.info("{} record for primary key {}-> {}", type, rows[0], rows);
                    log.info("Redis record for key {}->", table[1] + ":" + rows[0]);
                    log.info(String.valueOf(integrationUtil.execRedis(redisClient).hgetall(table[1] + ":" + rows[0])));
                    log.info("##### End Record {} #####", rows[0]);
                }

            }

            log.info("Inserted {} row(s) into {} table.", count, tableName);
            ps.executeBatch(); // insert remaining records
            log.info("{} row(s) affected!", count);

            // close connections
            ps.close();
            csvReader.close();

            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            log.info("It took {} ms to load {} csv records.", timeElapsed, count);
        } catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from csv file to the database."
                            + e.getMessage());
        }
    }

    private void doCount(Connection connection) {
        int select_count;
        try {
            Statement stmt = connection.createStatement();
            String select_count_query = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = stmt.executeQuery(select_count_query);
            rs.next();
            select_count = rs.getInt(1);
            log.info("Total records in {}={}.", tableName, select_count);

            stmt.close();
            rs.close();
        } catch(Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }

    }

    private void doSelect(Connection connection) {
        filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(select));
        log.info("[OUTPUT FROM SELECT] {}", filePath.getAbsolutePath());
        try {
            readFile = new ReadFile();
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(readFile.readFileAsString(filePath.getAbsolutePath()));
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next())
            {
                for (int i=1; i<=rsmd.getColumnCount(); i++)
                    //if (log.isDebugEnabled()) {
                    log.info("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
                //}
            }
            rs.close();
            st.close();
        }
        catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }
    }

    private void doUpdate(Connection connection) {
        int count = 0;
        filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(update));
        log.info("\n[Performing UPDATE] ... ");
        try {
            readFile = new ReadFile();
            ArrayList<String> updateDataList;
            updateDataList = readFile.readFileAsList(filePath.getAbsolutePath());
            Statement st = connection.createStatement();

            for (String sql : updateDataList) {
                st.addBatch(sql);
                if(++count % batchSize == 0) {
                    st.executeBatch();
                }
            }

            log.info("Updated {} row(s) in {} table.", count, tableName);
            st.executeBatch(); // update remaining records
            log.info("{} row(s) affected!", count);

            st.close();
        }
        catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }
    }

    private void doUpdatedSelect(Connection connection) {
        filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(updatedSelect));
        log.info("[OUTPUT FROM UPDATED SELECT] {}", filePath.getAbsolutePath());
        try {
            readFile = new ReadFile();
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(readFile.readFileAsString(filePath.getAbsolutePath()));
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next())
            {
                for (int i=1; i<=rsmd.getColumnCount(); i++)
                    log.info("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
            }
            rs.close();
            st.close();

        }
        catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }
    }

    private void doDelete(Connection connection) {
        int count = 0;
        filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(delete));
        log.info("\n[Performing DELETE] ... ");
        try {
            readFile = new ReadFile();
            ArrayList<String> deletedDataList;
            deletedDataList = readFile.readFileAsList(filePath.getAbsolutePath());
            Statement st = connection.createStatement();

            for (String sql : deletedDataList) {
                st.addBatch(sql);
                if(++count % batchSize == 0) {
                    st.executeBatch();
                }
            }

            log.info("Deleted {} row(s) in {} table.", count, tableName);
            st.executeBatch(); // update remaining records
            log.info("{} row(s) affected!", count);

            st.close();
        }
        catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }
    }

    private void doDeleteAll(Connection connection) {
        log.info("\n[Performing DELETE ALL ROWS] ... ");
        try {
            Statement st = connection.createStatement();
            st.executeUpdate("DELETE FROM " + tableName);

            st.close();
        }
        catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }
    }

    private void runAll()
    {
        try {
            log.info("##### LoadCSVAndCompare started with {} iteration(s).", iteration);
            Instant start = Instant.now();
            CoreConfig coreConfig = new CoreConfig();
            JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            for (int i=1; i<=iteration; i++) {
                doDeleteAll(connection);
                doSelect(connection);
                doCount(connection);
                try {
                    if (csvFile != null) {
                        doInsert(connection);
                        doCount(connection);
                    } else {
                        log.error("CSV data file is missing for the load.");
                        log.info("Skipping csv load and exiting..");
                        System.exit(0);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    log.error(e.getMessage());
                }
                doSelect(connection); doCount(connection);
                if (update != null) {
                    doUpdate(connection);  doCount(connection);
                } else {
                    log.info("Skipping update..");
                }
                doUpdatedSelect(connection); doCount(connection);
                if (delete != null) {
                    doDelete(connection);  doCount(connection);
                } else {
                    log.info("Skipping delete..");
                }
                doSelect(connection); doCount(connection);

            }
            log.info("##### LoadCSVAndCompare ended with {} iteration(s).", iteration);
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            log.info("It took {} ms to run {} iterations.", timeElapsed, iteration);
            connection.close();
        } catch (Exception e) {
            log.error("MESSAGE: {} STACKTRACE: {}",
                    ExceptionUtils.getRootCauseMessage(e),
                    ExceptionUtils.getRootCauseStackTrace(e));
            e.printStackTrace();
        }

    }

    @Override
    public void run() {
        runAll();
    }

}
