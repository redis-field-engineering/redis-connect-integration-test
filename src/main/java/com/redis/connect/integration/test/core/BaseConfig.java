package com.redis.connect.integration.test.core;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.gson.Gson;
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
public class BaseConfig {
    protected Map<String,Object> configurationDetails = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> configurationDetails() {
        return configurationDetails;
    }

    @JsonAnySetter
    public void setConfigurationDetail(String name, Object value) {
        configurationDetails.put(name, value);
    }

    public String getStringValue(String propertyName,String defaultVal) {
        return configurationDetails.containsKey(propertyName) ? (String)configurationDetails.get(propertyName) : defaultVal;
    }

    public Long getLongValue(String propertyName,Long defaultVal) {
        return configurationDetails.containsKey(propertyName) ? Long.parseLong(configurationDetails.get(propertyName).toString()) : defaultVal;
    }

    public Integer getIntegerValue(String propertyName,Integer defaultVal) {
        return configurationDetails.containsKey(propertyName) ? Integer.parseInt(configurationDetails.get(propertyName).toString()) : defaultVal;
    }

    public Boolean getBooleanValue(String propertyName, Boolean defaultVal) {
        return configurationDetails.containsKey(propertyName) ? Boolean.parseBoolean(configurationDetails.get(propertyName).toString()) : defaultVal;
    }

    public Object getValue(String propertyName, Object defaultVal) {
        return configurationDetails.getOrDefault(propertyName, defaultVal);
    }

    public Object getValue(String propertyName, Map<String,?> defaultVal) {
        return configurationDetails.getOrDefault(propertyName, defaultVal);
    }

    public boolean containsProperty(String propertyName) {
        return configurationDetails.containsKey(propertyName);
    }

    //Object to String
    public String objectToString(Object object){
        Gson gson=new Gson();
        return gson.toJson(object);
    }

}