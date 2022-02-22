package com.redis.connect.integration.test.source.rdb;

import com.redis.connect.integration.test.config.IntegrationConfig;
import com.redis.connect.integration.test.connections.JDBCConnectionProvider;
import com.redis.connect.integration.test.core.CoreConfig;
import com.redis.connect.integration.test.core.ReadFile;
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
    private static String loadQuery = (String) sourceConfig.get("loadQuery");
    private static final String loadQueryFile = (String) sourceConfig.get("loadQueryFile");
    private CoreConfig coreConfig = new CoreConfig();
    private static final Map<String, Object> targetConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("target");

    protected int batchSize = 5000;
    private Connection connection;
    private File filePath;
    private int startRecord;
    private File sqlFileName;
    private StringBuffer sb;
    @CommandLine.Option(names = "--truncateBeforeLoad", description = "Truncate the source table before load", paramLabel = "<boolean>")
    private boolean truncateBeforeLoad = true;

    @Override
    public void run() {
        try {
            batchSize = (int) sourceConfig.get("batchSize");
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            if(truncateBeforeLoad) {
                //delete data from table before loading csv
                connection.createStatement().execute("DELETE FROM " + tableName);
            }
            load();

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

            if(loadQuery == null) {
                filePath = new File(System.getProperty(IntegrationConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                        .concat(File.separator).concat(loadQueryFile));
                loadQuery = readFile.readFileAsString(filePath.getAbsolutePath());
            }
            loadStatement.executeUpdate(loadQuery);

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