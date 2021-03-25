package com.redislabs.cdc.integration.test;

import com.redislabs.cdc.integration.test.core.*;
import com.redislabs.cdc.integration.test.source.rdb.LoadRDB;
import com.redislabs.cdc.integration.test.target.redis.QueryAndCompare;
import picocli.CommandLine;

/**
 *
 * @author Virag Tripathi
 *
 */

@CommandLine.Command(sortOptions = false, subcommands = {GenerateCompletionCommand.class, LoadRDB.class, QueryAndCompare.class, LoadCSV.class, LoadCSVAndCompare.class}, abbreviateSynopsis = true)
public class IntegrationApp extends HelpCommand {

    private int executionStrategy(CommandLine.ParseResult parseResult) {
        return new CommandLine.RunLast().execute(parseResult); // default execution strategy
    }

    public int execute(String... args) {
        return commandLine().execute(args);
    }

    public IntegrationCommandLine commandLine() {
        IntegrationCommandLine commandLine = new IntegrationCommandLine(this);
        commandLine.setExecutionStrategy(this::executionStrategy);
        commandLine.setExecutionExceptionHandler(this::handleExecutionException);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        return commandLine;
    }

    private int handleExecutionException(Exception ex, CommandLine cmd, CommandLine.ParseResult parseResult) {
        // bold red error message
        cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
        return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex) : cmd.getCommandSpec().exitCodeOnExecutionException();
    }

}
