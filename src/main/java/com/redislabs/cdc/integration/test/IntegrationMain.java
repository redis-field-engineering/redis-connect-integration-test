package com.redislabs.cdc.integration.test;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "redis-cdc-integration-test", usageHelpAutoWidth = true, description = "Integration test framework for redis-cdc.")
public class IntegrationMain extends IntegrationApp {

    public static void main(String[] args) {
        log.info("Initializing Integration Test Application.");
        System.exit(new IntegrationMain().execute(args));
    }

}