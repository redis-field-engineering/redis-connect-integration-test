package com.redislabs.connect.integration.test.config.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */

@Getter
@Setter
public class EnvConfig {
    private Map<String,Map<String,Object>> connections = new HashMap<>();

    @JsonAnyGetter
    public Map<String,Map<String,Object>> connections() {
        return connections;
    }

    public Map<String,Object> getConnection(String connectionId) {
        return connections.get(connectionId);
    }

}