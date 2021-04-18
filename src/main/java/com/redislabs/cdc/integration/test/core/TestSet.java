package com.redislabs.cdc.integration.test.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.javafaker.Faker;
import com.opencsv.CSVReader;
import com.redislabs.cdc.integration.test.config.IntegrationConfig;
import com.redislabs.cdc.integration.test.config.model.EnvConfig;
import com.redislabs.cdc.integration.test.connections.JDBCConnectionProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.validator.GenericValidator;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Modifier;
import java.lang.reflect.Method;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;
import java.lang.IllegalArgumentException;

/**
 *
 * @author Oren Elias
 *
 */
@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "generate", description = "Generate a random test set based on parameters")
public class TestSet implements Runnable {

    // TODO: refactor into a shared Enum of SQL_TEMPLATES to be used across classes
    private String SQL_INSERT_TEMPLATE = "INSERT INTO ${table}(${columns}) VALUES(${values})";

    @CommandLine.Option(names = { "-d",
            "--duration" }, description = "Duration, in HH:MM:SS format. Default 5 minutes", paramLabel = "<string>", defaultValue = "00:00:05", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String durationStr;

    @CommandLine.Option(names = { "-t",
            "--tablename" }, description = "Table name", paramLabel = "<string>", defaultValue = "test_table", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String tableName;

    @CommandLine.Option(names = { "-tps",
            "--tps" }, description = "Average number of transactions per second", paramLabel = "<int>", defaultValue = "10", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private int tps;

    @CommandLine.Option(names = { "-col",
            "--columns" }, description = "Number of columns in table", paramLabel = "<int>", defaultValue = "5", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    int numberOfColumns;

    @CommandLine.Option(names = { "-row",
            "--rows" }, description = "Number of initial rows in table", paramLabel = "<int>", defaultValue = "100", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    int numberOfRows;

    @CommandLine.Option(names = { "-pk",
            "--primary-keys" }, description = "Number of primary keys in table", paramLabel = "<int>", defaultValue = "2", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private int numberOfPrimaryKeys = 2; // number of columns to use as a composite primary key

    @CommandLine.Option(names = { "-l",
            "--locale" }, description = "Locale to use for fake values", paramLabel = "<string>", defaultValue = "en-US", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String locale;

    @CommandLine.Option(names = { "-c",
            "--config-file" }, description = "Location of config folder to use", paramLabel = "<string>", defaultValue = "config", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String configFileLocation = "config";

    private Connection connection;
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private CoreConfig coreConfig = new CoreConfig();

    private EnvConfig envConfig;

    TestSet() {
        System.setProperty(IntegrationConfig.CONFIG_LOCATION_PROPERTY, configFileLocation);
        this.envConfig = IntegrationConfig.INSTANCE.getEnvConfig();
    }

    class Column {
        public String name;
        public Object fakerDomain;
        public String fakerDomainName;
        public Method fakerMethod;
        private Faker faker = new Faker();
        private final String[] allFakerDomains = { "address", "ancient", "app", "artist", "avatar", "backToTheFuture",
                "aviation", "beer", "book", "bool", "business", "chuckNorris", "cat", "code", "color", "commerce",
                "company", "crypto", "date", "demographic", "dog", "dragonBall", "educator", "esports", "file",
                "finance", "food", "friends", "funnyName", "gameOfThrones", "hacker", "harryPotter", "hipster",
                "hitchhikersGuideToTheGalaxy", "hobbit", "howIMetYourMother", "idNumber", "internet", "job",
                "leagueOfLegends", "lebowski", "lordOfTheRings", "lorem", "matz", "music", "name", "nation", "number",
                "overwatch", "phoneNumber", "pokemon", "rickAndMorty", "robin", "rockBand", "shakespeare", "slackEmoji",
                "space", "starTrek", "stock", "superhero", "team", "twinPeaks", "university", "weather", "witcher",
                "yoda", "zelda" };
        public String type;
        public int typeLength = 0;
        public int typePrecision = 0;

        public Column() throws Exception {
            // randomly pick a type and faker generator
            this.fakerDomainName = this.allFakerDomains[new Random().nextInt(this.allFakerDomains.length)];
            this.fakerDomain = faker.getClass().getMethod(this.fakerDomainName).invoke(faker);
            System.err.println(this.fakerDomain.getClass().getName());
            Method[] allFakerMethods = this.getAllMethods(this.fakerDomain);

            // randomly pick a method
            this.fakerMethod = allFakerMethods[new Random().nextInt(allFakerMethods.length)];

            // randomly generate column name
            this.name = this.fakerMethod.getName() + "_" + UUID.randomUUID().toString().replace("-", "_");

            // try to fetch a value to determine type
            Object tmpValue = this.getRandomValue();
            if (String.class.isInstance(tmpValue)) {
                this.type = "VARCHAR";
                this.typeLength = 128;
            } else if (Character.class.isInstance(tmpValue)) {
                this.type = "TEXT";
            } else if (Integer.class.isInstance(tmpValue)) {
                this.type = "INT";
            } else if (Long.class.isInstance(tmpValue)) {
                this.type = "DECIMAL";
                this.typeLength = 38;
                this.typePrecision = 18;
            } else if (Date.class.isInstance(tmpValue)) {
                this.type = "DATETIME";
            } else if (Boolean.class.isInstance(tmpValue)) {
                this.type = "BIT";
            } else {
                throw new IllegalArgumentException("unknown data type:" + tmpValue.getClass().getName());
            }
            // TODO : add randomized data types
            System.err.println(this.name);
            System.err.println(this.getRandomValue());
        }

        public String getSqlType() {
            String typeClause = this.type;
            if (this.typeLength > 0) {
                typeClause += "(" + this.typeLength;
                if (this.typePrecision > 0)
                    typeClause += "," + this.typePrecision;
                typeClause += ")";
            }
            return typeClause;
        }

        private Method[] getAllMethods(Object object) {
            Method[] allMethods = object.getClass().getDeclaredMethods();
            ArrayList<Method> publicMethods = new ArrayList<Method>();
            for (Method method : allMethods) {
                if (Modifier.isPublic(method.getModifiers()) && method.getParameterTypes().length == 0) {
                    publicMethods.add(method);
                }
            }
            return publicMethods.toArray(new Method[0]);
        }

        public Object getRandomValue() {
            // invoke the faker method
            // if sqlFormat is true, wrap in quotes where applicable
            try {
                return this.fakerMethod.invoke(this.fakerDomain);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    // get all public methods of a class. used for the faker instantiation
    // only returns methods that don't accept parameters for easier random use

    private void generate() throws Exception {
        System.err.println(this.numberOfColumns);
        // set up random column types
        ArrayList<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < numberOfColumns; i++) {
            Column column = new Column();
            columns.add(column);
        }

        List<String> primaryKeys = columns.subList(0, numberOfPrimaryKeys).stream().map(column -> column.name)
                .collect(Collectors.toList());

        //
        // Query to create a table
        //
        String query = "CREATE TABLE " + tableName + " (";
        // add columns
        for (Column column : columns) {
            query += String.format("%s %s NOT NULL,\n", column.name, column.getSqlType());
        }
        // add primary key
        String pkClause = String.format("CONSTRAINT %s_pk PRIMARY KEY (%s)", tableName, String.join(",", primaryKeys));
        query += pkClause + ")";
        log.info("Create statement: {}.", query);

        connection = JDBC_CONNECTION_PROVIDER.getConnection("source");

        // Creating the Statement
        Statement stmt = connection.createStatement();
        stmt.execute("DROP TABLE IF EXISTS " + tableName);
        stmt.execute(query);
        log.info("Table Created");

        System.out.println("Generating data......");

        // create insert template
        String columnsClause = String.join(",",
                columns.stream().map(column -> column.name).collect(Collectors.toList()));
        String valuesClause = String.join(",", Collections.nCopies(columns.size(), "?"));
        Map<String, String> substitutes = new HashMap<>();
        substitutes.put("columns", columnsClause);
        substitutes.put("table", tableName);
        substitutes.put("values", valuesClause);
        String insertSql = new StrSubstitutor(substitutes).replace(SQL_INSERT_TEMPLATE);
        System.err.println(insertSql);
        // create prepared statement
        PreparedStatement insertStmt = connection.prepareStatement(insertSql);

        // generate dummy data
        int rowsInserted = 0;
        while (rowsInserted < numberOfRows) {
            // bind variables
            for (int col = 0; col < columns.size(); col++) {
                Column column = columns.get(col);
                Object value = column.getRandomValue();
                switch (column.type) {
                case "VARCHAR":
                    // limit to 128 chars
                    insertStmt.setString(col + 1,
                            value.toString().substring(0, Math.min(128, value.toString().length()))); // bind variables
                                                                                                      // are 1-based
                    break;
                case "TEXT":
                    insertStmt.setString(col + 1, value.toString()); // bind variables are 1-based
                    break;
                case "INT":
                    insertStmt.setInt(col + 1, (int) value); // bind variables are 1-based
                    break;
                case "DECIMAL":
                    insertStmt.setLong(col + 1, (long) value); // bind variables are 1-based
                    break;
                case "BIT":
                    insertStmt.setBoolean(col + 1, (Boolean) value); // bind variables are 1-based
                    break;
                case "DATETIME":
                    insertStmt.setDate(col + 1, new java.sql.Date(((Date) value).getTime())); // bind variables are
                                                                                              // 1-based
                    break;
                default:
                    throw new IllegalArgumentException("unknown data type:" + column.type);
                }
            }
            try {
                insertStmt.executeUpdate();
                rowsInserted++;
            } catch (SQLException e) {
                // TODO: explicitly handle duplicate primary key inserts
            }
        }
    }

    @Override
    public void run() {
        try {
            this.generate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}