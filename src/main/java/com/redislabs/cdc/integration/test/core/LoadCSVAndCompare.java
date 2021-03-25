package com.redislabs.cdc.integration.test.core;

import com.opencsv.CSVReader;
import com.redislabs.cdc.integration.test.config.IntegrationConfig;
import com.redislabs.cdc.integration.test.connections.JDBCConnectionProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.validator.GenericValidator;
import picocli.CommandLine;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
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

    private Connection connection = null;
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private CoreConfig coreConfig = new CoreConfig();
    private static final Map<String, Object> sourceConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String csvFile = (String) sourceConfig.get("csvFile");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private static final String select = (String) sourceConfig.get("select");
    private static final String updatedSelect = (String) sourceConfig.get("updatedSelect");
    private static final String update = (String) sourceConfig.get("update");
    private static final String delete = (String) sourceConfig.get("delete");
    private static final int batchSize = (int) sourceConfig.get("batchSize");
    private static final int iteration = (int) sourceConfig.get("iteration");
    private static final String type = (String) sourceConfig.get("type");

    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");
    private static final String redisURI = (String) targetConfig.get("redisUrl");
    @CommandLine.Option(names = {"-s", "--separator"}, description = "CSV records separator", paramLabel = "<char>", defaultValue = ",", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private char separator = ',';
    @CommandLine.Option(names = {"-t", "--truncateBeforeLoad"}, description = "Truncate the source table before load", paramLabel = "<boolean>")
    private boolean truncateBeforeLoad = true;

    /**
     * Parse CSV file using OpenCSV library and load in
     * given database table.
     * @throws Exception Throws exception
     */
    private void doInsert() throws Exception {
        CSVReader csvReader;
        try {

            csvReader = new CSVReader(new FileReader(LoadCSVAndCompare.csvFile));
            log.info("Loading {} into {} table with batchSize={}.", LoadCSVAndCompare.csvFile, LoadCSVAndCompare.tableName, batchSize);

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

        String[] rowData;
        PreparedStatement ps;

        try {
            connection.setAutoCommit(false);

            ps = connection.prepareStatement(insert_query);

            int count = 0;
            String datePattern = "yyyy-MM-dd hh:mm:ss";
            ArrayList<String[]> rowDataList = new ArrayList<>();
            String[] table = LoadCSVAndCompare.tableName.split("\\.", LoadCSVAndCompare.tableName.length());

            while ((rowData = csvReader.readNext()) != null) {

                int index = 1;
                for (String columnData : rowData) {
                    if (GenericValidator.isDate(columnData, datePattern, true)) {
                        String input = columnData.replace(" ", "T");
                        LocalDateTime ldt = LocalDateTime.parse(input);
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


            for (String[] rows : rowDataList) {
                log.info("##### Start Record {} #####", rows[0]);
                log.info("{} record for primary key {}-> {}", type, rows[0], rows);
                log.info("Redis record for key {}->", table[1] + ":" + rows[0]);
                log.info(String.valueOf(IntegrationUtil.execRedis(redisURI).hgetall(table[1] + ":" + rows[0])));
                log.info("##### End Record {} #####", rows[0]);
            }

            /*
            int selCount = 0;
            for (int i=1; i<=doCount(); i++) {
                selCount++;
                log.info("##### Start Record {} #####", selCount);
                log.info("{} record for primary key {}-> {}", type, i, rowDataList.get(i-1));
                log.info("Redis record for key {}->", table[1] + ":" + selCount);
                log.info(String.valueOf(IntegrationUtil.execRedis(redisURI).hgetall(table[1] + ":" + selCount)));
                log.info("##### End Record {} #####", selCount);
            }
             */

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("InterruptedException..", e);
            }
            // DELETE and Compare

            // commit and close connection
            connection.commit();
            ps.close();
            csvReader.close();
        } catch (Exception e) {
            connection.rollback();
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from file to database."
                            + e.getMessage());
        }
    }

    private int doCount() {
        int select_count = 0;
        try {
            Statement stmt = connection.createStatement();
            String select_count_query = "SELECT COUNT(*) FROM " + LoadCSVAndCompare.tableName;
            ResultSet rs = stmt.executeQuery(select_count_query);
            rs.next();
            select_count = rs.getInt(1);
        } catch(Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

        return select_count;
    }

    private void doSelect() {
        log.info("[OUTPUT FROM SELECT] {}", select);
        try {
            ReadFile readFile = new ReadFile();
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(readFile.readFileAsString(select));
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next())
            {
                for (int i=1; i<=rsmd.getColumnCount(); i++)
                    log.info("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
            }
            rs.close();
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doUpdate() {
        log.info("\n[Performing UPDATE] ... ");
        try {
            ReadFile readFile = new ReadFile();
            Statement st = connection.createStatement();
            st.executeUpdate(readFile.readFileAsString(update));
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doUpdatedSelect() {
        log.info("[OUTPUT FROM SELECT] {}", updatedSelect);
        try {
            ReadFile readFile = new ReadFile();
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(readFile.readFileAsString(updatedSelect));
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next())
            {
                for (int i=1; i<=rsmd.getColumnCount(); i++)
                log.info("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
            }
            rs.close();
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doDelete() {
        log.info("\n[Performing DELETE] ... ");
        try {
            ReadFile readFile = new ReadFile();
            Statement st = connection.createStatement();
            st.executeUpdate(readFile.readFileAsString(delete));
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doDeleteAll() {
        log.info("\n[Performing DELETE ALL ROWS] ... ");
        try {
            Statement st = connection.createStatement();
            st.executeUpdate("DELETE FROM " + tableName);
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void runAll()
    {
        try {
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            for (int i=0; i<iteration; i++) {
                doDeleteAll();
                doSelect();
                try {
                    doInsert();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error(e.getMessage());
                }
                doSelect();
                doUpdate();  doUpdatedSelect();
                doDelete();  doSelect();
            }
            connection.close();
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

    @Override
    public void run() {
        runAll();
    }

}
