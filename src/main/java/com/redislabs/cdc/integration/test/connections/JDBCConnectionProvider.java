package com.redislabs.cdc.integration.test.connections;

import com.redislabs.cdc.integration.test.config.IntegrationConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.lang.IllegalArgumentException;

/**
 *
 * @author Virag Tripathi
 *
 */

@Slf4j
public class JDBCConnectionProvider implements ConnectionProvider<Connection> {

    private static final Map<String,HikariDataSource> DATA_SOURCE_MAP = new HashMap<>();

    public Connection getConnection(String connectionId) {
        HikariDataSource dataSource = DATA_SOURCE_MAP.get(connectionId);
        if(dataSource == null) {
            Map<String,Object> databaseConfig = IntegrationConfig.INSTANCE.getEnvConfig().getConnection("source");
            HikariConfig jdbcConfig = new HikariConfig();
            jdbcConfig.setPoolName((String)databaseConfig.get("name"));
            String dbType = "";
            switch (((String)databaseConfig.get("type")).toLowerCase()) {
                case "mssqlserver":
                case "sqlserver":
                    dbType = "sqlserver";
                    break;
                default:
                    throw new IllegalArgumentException("unsupported database type in config: "+databaseConfig.get("type"));
            }
            // generate the jdbc string from the arguments
            jdbcConfig.setJdbcUrl(String.format(
                "jdbc:%s://%s:%s;database=%s",
                dbType,
                databaseConfig.get("hostname"),
                databaseConfig.get("port"),
                databaseConfig.get("db")
            ));

            jdbcConfig.setUsername((String) databaseConfig.get("username"));
            jdbcConfig.setPassword((String) databaseConfig.get("password"));
            jdbcConfig.setMaximumPoolSize((Integer)databaseConfig.get("maximumPoolSize"));
            jdbcConfig.setMinimumIdle((Integer)databaseConfig.get("minimumIdle"));

            log.info("establishing connection pool");
            dataSource = new HikariDataSource(jdbcConfig);
            log.info("connection pool established");
            DATA_SOURCE_MAP.put(connectionId,dataSource);
        }
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return connection;
    }
}
