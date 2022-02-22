package com.redis.connect.integration.test.core;

import com.opencsv.CSVReader;
import com.redis.connect.integration.test.config.IntegrationConfig;
import com.redis.connect.integration.test.connections.JDBCConnectionProvider;
import com.redis.lettucemod.RedisModulesClient;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.validator.GenericValidator;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.management.ManagementFactory;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;

/**
 * @author Virag Tripathi
 */
@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "loadcsvandcompare",
        description = "Load CSV data to source and print live comparisons.")
public class LoadCSVAndCompare implements Runnable {

    private static final String WHOAMI = "LoadCSVAndCompare";
    private final String instanceId;

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
    private RedisModulesClient redisClient = null;
    @CommandLine.Option(names = {"-s", "--separator"}, description = "CSV records separator", paramLabel = "<char>", defaultValue = ",", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private char separator = ',';
    @CommandLine.Option(names = {"-t", "--truncateBeforeLoad"}, description = "Truncate the source table before load", paramLabel = "<boolean>")
    private boolean truncateBeforeLoad = (boolean) sourceConfig.getOrDefault("truncateBeforeLoad", true);

    LoadCSVAndCompare() {
        instanceId = ManagementFactory.getRuntimeMXBean().getName();
    }

    /**
     * Parse CSV file using OpenCSV library and load in
     * given database table.
     *
     * @throws Exception Throws exception
     */
    private void doInsert(Connection connection) throws Exception {
        Instant start = Instant.now();
        CSVReader csvReader;
        PreparedStatement ps;
        try {
            filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(csvFile));

            csvReader = new CSVReader(new FileReader(filePath));
            log.info("Instance: {} {} Loading {} into {} table with batchSize={}.", instanceId, WHOAMI, filePath, LoadCSVAndCompare.tableName, batchSize);

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error occurred while executing file. "
                    + ExceptionUtils.getRootCauseMessage(e));
        }

        String[] headerRow = csvReader.readNext();

        if (null == headerRow) {
            throw new FileNotFoundException(
                    "No columns defined in given CSV file. " +
                            "Please check the CSV file format.");
        }

        String questionmarks = org.apache.commons.lang3.StringUtils.repeat("?" + getSeparator(), headerRow.length);
        questionmarks = (String) questionmarks.subSequence(0, questionmarks
                .length() - 1);

        String insert_query = SQL_INSERT.replaceFirst(TABLE_REGEX, LoadCSVAndCompare.tableName);
        insert_query = insert_query
                .replaceFirst(KEYS_REGEX, StringUtils.join(headerRow,
                        this.separator));
        insert_query = insert_query.replaceFirst(VALUES_REGEX, questionmarks);

        log.info("Instance: {} {} Insert Query: {}", instanceId, WHOAMI, insert_query);

        try {
            ps = connection.prepareStatement(insert_query);

            int count = 0;
            String[] rowData;

            while ((rowData = csvReader.readNext()) != null) {

                int index = 1;
                for (String columnData : rowData) {
                    if (GenericValidator.isDate(columnData, DateTimeUtil.yyyy_MM_dd_HH_mm_ss.getDisplayName(), true)) {
                        String input = columnData.replace(" ", "T");
                        LocalDateTime ldt = LocalDateTime.parse(input);
                        ps.setTimestamp(index++, Timestamp.valueOf(ldt));
                    } else if ((GenericValidator.isDate(columnData, DateTimeUtil.yyyy_mm_dd.getDisplayName(),
                            true)) || (GenericValidator.isDate(columnData,
                            DateTimeUtil.yyyy_MM_dd.getDisplayName(), true))) {
                        Date d = Date.valueOf(columnData);
                        ps.setDate(index++, d);
                    } else if ((GenericValidator.isDate(columnData, DateTimeUtil.dd_MM_yyyy.getDisplayName(),
                            true))) {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DateTimeUtil.dd_MM_yyyy.getDisplayName(), Locale.ENGLISH);
                        LocalDate d = LocalDate.parse(columnData, formatter);
                        ps.setDate(index++, Date.valueOf(d));
                    } else if (GenericValidator.isDouble(columnData)) {
                        ps.setDouble(index++, Double.parseDouble(columnData));
                    } else if (GenericValidator.isInt(columnData)) {
                        ps.setInt(index++, Integer.parseInt(columnData));
                    } else {
                        ps.setString(index++, columnData);
                    }
                }
                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }

                log.debug("Instance: {} {} ##### Start Record {} #####", instanceId, WHOAMI, rowData[0]);
                log.debug("Instance: {} {} {} record for primary key {}-> {}", instanceId, WHOAMI, type, rowData[0], rowData);
                log.debug("Instance: {} {} ##### End Record {} #####", instanceId, WHOAMI, rowData[0]);


            }

            log.info("Instance: {} {} Inserted {} row(s) into {} table.", instanceId, WHOAMI, count, tableName);
            ps.executeBatch(); // insert remaining records
            log.info("{} row(s) affected!", count);

            ps.close();
            csvReader.close();

            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            log.info("It took {} ms to load {} csv records.", timeElapsed, count);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from csv file to the database. "
                            + ExceptionUtils.getRootCauseMessage(e));
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
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    log.debug("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
            }
            rs.close();
            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during select " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }
    }

    private void doUpdate(Connection connection) {
        int count = 0;
        filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(update));
        log.info("Instance: {} {} [Performing UPDATE] ... ", instanceId, WHOAMI);
        try {
            readFile = new ReadFile();
            ArrayList<String> updateDataList;
            updateDataList = readFile.readFileAsList(filePath.getAbsolutePath());
            Statement st = connection.createStatement();

            for (String sql : updateDataList) {
                st.addBatch(sql);
                if (++count % batchSize == 0) {
                    st.executeBatch();
                }
            }

            log.info("Instance: {} {} Updated {} row(s) in {} table.", instanceId, WHOAMI, count, tableName);
            st.executeBatch(); // update remaining records
            log.info("Instance: {} {} {} row(s) affected!", instanceId, WHOAMI, count);

            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during update " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }
    }

    private void doUpdatedSelect(Connection connection) {
        filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(updatedSelect));
        log.info("Instance: {} {} [OUTPUT FROM UPDATED SELECT] {}", instanceId, WHOAMI, filePath.getAbsolutePath());
        try {
            readFile = new ReadFile();
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(readFile.readFileAsString(filePath.getAbsolutePath()));
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    log.info("Instance: {} {} {{} : {}}", instanceId, WHOAMI, rsmd.getColumnName(i), rs.getString(i));
            }
            rs.close();
            st.close();

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during updatedSelect " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }
    }

    private void doDelete(Connection connection) {
        int count = 0;
        filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(delete));
        log.info("Instance: {} {} [Performing DELETE] ... ", instanceId, WHOAMI);
        try {
            readFile = new ReadFile();
            ArrayList<String> deletedDataList;
            deletedDataList = readFile.readFileAsList(filePath.getAbsolutePath());
            Statement st = connection.createStatement();

            for (String sql : deletedDataList) {
                st.addBatch(sql);
                if (++count % batchSize == 0) {
                    st.executeBatch();
                }
            }

            log.info("Instance: {} {} Deleted {} row(s) in {} table.", instanceId, WHOAMI, count, tableName);
            st.executeBatch(); // update remaining records
            log.info("Instance: {} {} {} row(s) affected!", instanceId, WHOAMI, count);

            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during delete " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }
    }

    private void doDeleteAll(Connection connection) {
        log.info("Instance: {} {} [Performing DELETE ALL ROWS] ... ", instanceId, WHOAMI);
        try {
            Statement st = connection.createStatement();
            st.executeUpdate("DELETE FROM " + tableName);

            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during deleteAll " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
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
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during count " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }

    }

    private void runAll() {
        try {
            log.info("Instance: {} {} started with {} iteration(s).", ManagementFactory.getRuntimeMXBean().getName(), WHOAMI, iteration);
            Instant start = Instant.now();
            CoreConfig coreConfig = new CoreConfig();
            JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            for (int i = 1; i <= iteration; i++) {
                if (truncateBeforeLoad) {
                    doDeleteAll(connection);
                } else {
                    log.info("Skipping truncate..");
                }
                if (select != null) {
                    doSelect(connection);
                } else {
                    log.info("Skipping select..");
                }

                doCount(connection);

                if (csvFile != null) {
                    doInsert(connection);
                    doCount(connection);
                } else {
                    log.error("CSV data file is missing for the load.");
                    log.info("Skipping csv load and exiting..");
                    System.exit(0);
                }

                if (update != null) {
                    doUpdate(connection);
                    doCount(connection);
                } else {
                    log.info("Skipping update..");
                }
                if (updatedSelect != null) {
                    doUpdatedSelect(connection);
                } else {
                    log.info("Skipping updatedSelect..");
                }
                if (delete != null) {
                    doDelete(connection);
                    doCount(connection);
                } else {
                    log.info("Skipping delete..");
                }
                if (select != null) {
                    doSelect(connection);
                } else {
                    log.info("Skipping select..");
                }
                doCount(connection);

            }

            log.info("Instance: {} {} ended with {} iteration(s).", ManagementFactory.getRuntimeMXBean().getName(), WHOAMI, iteration);

            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            log.info("It took {} ms to finish {} iterations.", timeElapsed, iteration);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during runAll " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }

    }

    @Override
    public void run() {
        runAll();
    }
}
