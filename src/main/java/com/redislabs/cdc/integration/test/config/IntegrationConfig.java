package com.redislabs.cdc.integration.test.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.redislabs.cdc.integration.test.config.model.EnvConfig;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */

@Getter
public enum IntegrationConfig {
    INSTANCE;
    public final static String CONFIG_LOCATION_PROPERTY = "redislabs.integration.test.configLocation";
    ObjectMapper yamlObjectMapper;
    ObjectMapper objectMapper;

    EnvConfig envConfig;

    IntegrationConfig() {
        objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false).configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,false);
        yamlObjectMapper = new ObjectMapper(new YAMLFactory()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false).configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,false);

        try {
            envConfig = yamlObjectMapper.readValue(new File(System.getProperty(CONFIG_LOCATION_PROPERTY).concat(File.separator).concat("config.yml")), EnvConfig.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public EnvConfig getEnvConfig() {
        return envConfig;
    }

    public String serializeVariables(Map<String,String> variables) {
        String value = null;
        try {
            value =  objectMapper.writeValueAsString(variables);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return value;
    }

    public String serialize(Object o) {
        String value = null;
        try {
            value =  objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return value;
    }

    public Map<String,String> deSerializeVariables(String variables) {
        Map<String,String> value = null;
        try {
            value =  (Map<String,String>)objectMapper.readValue(variables,Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return value;
    }

    public Map<String,Object> deSerialize(String variables) {
        Map<String,Object> value = null;
        try {
            value =  objectMapper.readValue(variables,Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return value;
    }

    public Object deSerialize(String value,Class clazz) {
        Object returnVal = null;
        try {
            returnVal =  objectMapper.readValue(value,clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return returnVal;
    }

}