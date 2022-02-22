package com.redis.connect.integration.test;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "redis-connect-integration-test", usageHelpAutoWidth = true, description = "Integration test framework for Redis Connect connectors.")
public class IntegrationMain extends IntegrationApp {

    public static void main(String[] args) {
        log.info("Initializing Redis Connect Integration Test Instance.");
        System.exit(new IntegrationMain().execute(args));
    }

}