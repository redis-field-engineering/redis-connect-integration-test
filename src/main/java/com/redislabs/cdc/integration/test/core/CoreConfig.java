package com.redislabs.cdc.integration.test.core;

import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author Virag Tripathi
 *
 */

@Getter
@Setter
public class CoreConfig extends BaseConfig {
    private String providerId;
    private String connectionId;
    private String source;
    private int batchSize;
    private long pollingInterval;
}