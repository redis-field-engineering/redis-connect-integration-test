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
import org.apache.commons.validator.GenericValidator;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Modifier;
import java.lang.reflect.Method;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
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

    @CommandLine.Option(names = { "-d",
            "--duration" }, description = "Duration, in HH:MM:SS format. Default 5 minutes", paramLabel = "<string>", defaultValue = "00:00:05", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String durationStr = "00:00:05";

    @CommandLine.Option(names = { "-tps",
            "--tps" }, description = "Average number of transactions per second", paramLabel = "<int>", defaultValue = "10", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private int tps = 10;

    @CommandLine.Option(names = { "-l",
            "--locale" }, description = "Locale to use for fake values", paramLabel = "<string>", defaultValue = "en-US", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String locale = "en-US";

    @CommandLine.Option(names = { "-c",
            "--config-file" }, description = "Location of config file to use", paramLabel = "<string>", defaultValue = "config/config.yml", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private String configFileLocation = "config/config.yml";

    private Connection connection;
    private static final JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
    private CoreConfig coreConfig = new CoreConfig();
    // private static final Map<String, Object> sourceConfig =
    // IntegrationConfig.INSTANCE.getEnvConfig()
    // .getConnection("source");

    // private static final Map<String, Object> targetConfig =
    // IntegrationConfig.INSTANCE.getEnvConfig()
    // .getConnection("target");

    private EnvConfig envConfig;

    TestSet() {
        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);

        try {
            envConfig = yamlObjectMapper.readValue(new File(configFileLocation), EnvConfig.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        public int typeLength;
        public int typePrecision;

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
            } else if (Character.class.isInstance(tmpValue)) {
                this.type = "TEXT";
            } else if (Integer.class.isInstance(tmpValue)) {
                this.type = "INT";
            } else if (Long.class.isInstance(tmpValue)) {
                this.type = "DECIMAL(38,18)";
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
            try {
                return fakerMethod.invoke(fakerDomain);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    // get all public methods of a class. used for the faker instantiation
    // only returns methods that don't accept parameters for easier random use

    private void generate() throws Exception {
        int numberOfColumns = 150;

        // set up random column types
        ArrayList<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < numberOfColumns; i++) {
            Column column = new Column();
            columns.add(column);
        }

        int numberOfPrimaryKeys = 3; // number of columns to use as a composite primary key
        String tableName = "bla";

        // Query to create a table
        String query = "CREATE TABLE " + tableName + " (";
        // add columns
        for (Column column : columns) {
            query += String.format("%s %s NOT NULL,\n", column.name, column.type);
        }
        query += ")";
        System.err.println(query);
        // + "PRIMARY KEY (ID))";
        connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());

        // Creating the Statement
        Statement stmt = connection.createStatement();
        stmt.execute(query);
        System.out.println("Table Created......");
        // try {

        // } catch (Exception e) {
        // e.printStackTrace();
        // throw new Exception("Error occurred while executing file. "
        // + e.getMessage());
        // }

    }

    @Override
    public void run() {
        try {
            TestSet testSet = new TestSet();
            testSet.generate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}