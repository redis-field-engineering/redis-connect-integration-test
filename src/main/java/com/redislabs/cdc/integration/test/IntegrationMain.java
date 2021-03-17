package com.redislabs.cdc.integration.test;

import com.redislabs.cdc.integration.test.source.rdb.LoadRDB;
import com.redislabs.cdc.integration.test.target.redis.QueryAndCompare;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Virag Tripathi
 *
 */

@Slf4j
public class IntegrationMain {

    private Object entry;

    public static void main(String[] args) throws Exception {
        log.info("Initializing Integration Test Application");
        //LoadRDB.class.newInstance().run();
        //Thread.sleep(30000L);
        QueryAndCompare.class.newInstance().run();
        log.info("Integration Test completed.");
    }
}
