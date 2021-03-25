package com.redislabs.cdc.integration.test.source.rdb;

import com.redislabs.cdc.integration.test.config.IntegrationConfig;
import com.redislabs.cdc.integration.test.connections.JDBCConnectionProvider;
import com.redislabs.cdc.integration.test.core.CoreConfig;
import com.redislabs.cdc.integration.test.core.ReadFile;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */

@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "loadsqldata",
        description = "Load data into source table using sql insert statements.")
public class LoadRDB implements Runnable {
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private static final Map<String, Object> sourceConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private CoreConfig coreConfig = new CoreConfig();
    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");

    protected int batchSize = 5000;
    private Connection connection;
    private int startRecord;
    private String query;
    private File sqlFileName;
    private StringBuffer sb;
    @CommandLine.Option(names = "--truncateBeforeLoad", description = "Truncate the source table before load", paramLabel = "<boolean>")
    private boolean truncateBeforeLoad = true;

    @Override
    public void run() {
        try {
            batchSize = (int) sourceConfig.get("batchSize");
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            //log.info(connection.getCatalog());
            if(truncateBeforeLoad) {
                //delete data from table before loading csv
                connection.createStatement().execute("DELETE FROM " + tableName);
            }
            load();

            //connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }

    private void load() {
        try {
            ReadFile readFile = new ReadFile();
            Statement loadStatement = connection.createStatement();
            loadStatement.setFetchSize(batchSize);

            query = (String) sourceConfig.get("loadQuery");

            if(query == null) {
                String fileName = (String) sourceConfig.get("loadQueryFile");
                query = readFile.readFileAsString(fileName);
            }
            loadStatement.executeUpdate(query);

            loadStatement.close();
            connection.close();
        } catch (SQLException sqe) {
            sqe.printStackTrace();
            log.error(String.valueOf(sqe));
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }

}