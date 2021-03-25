package com.redislabs.cdc.integration.test.core;

import picocli.CommandLine;

public class IntegrationCommandLine extends CommandLine {

    public IntegrationCommandLine(Object command) {
        super(command);
    }

    @Override
    public ParseResult parseArgs(String... args) {
        return super.parseArgs(args);
    }
}