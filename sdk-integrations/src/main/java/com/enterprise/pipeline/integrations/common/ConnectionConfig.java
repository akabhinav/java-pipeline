package com.enterprise.pipeline.integrations.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic connection configuration for integrations.
 * Provides a flexible key-value store for connection parameters.
 */
public class ConnectionConfig implements Serializable {
    private final Map<String, Object> properties;

    public ConnectionConfig() {
        this.properties = new HashMap<>();
    }

    public ConnectionConfig(Map<String, Object> properties) {
        this.properties = new HashMap<>(properties);
    }

    public ConnectionConfig set(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    public Object get(String key) {
        return properties.get(key);
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public String getString(String key, String defaultValue) {
        Object value = properties.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    public Integer getInt(String key) {
        return getInt(key, null);
    }

    public Integer getInt(String key, Integer defaultValue) {
        Object value = properties.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    public Boolean getBoolean(String key) {
        return getBoolean(key, null);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        Object value = properties.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    public Map<String, Object> getAll() {
        return new HashMap<>(properties);
    }

    public boolean containsKey(String key) {
        return properties.containsKey(key);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<String, Object> properties = new HashMap<>();

        public Builder set(String key, Object value) {
            properties.put(key, value);
            return this;
        }

        public Builder host(String host) {
            return set("host", host);
        }

        public Builder port(int port) {
            return set("port", port);
        }

        public Builder username(String username) {
            return set("username", username);
        }

        public Builder password(String password) {
            return set("password", password);
        }

        public Builder database(String database) {
            return set("database", database);
        }

        public Builder collection(String collection) {
            return set("collection", collection);
        }

        public Builder table(String table) {
            return set("table", table);
        }

        public Builder url(String url) {
            return set("url", url);
        }

        public ConnectionConfig build() {
            return new ConnectionConfig(properties);
        }
    }

    @Override
    public String toString() {
        // Mask sensitive fields
        Map<String, Object> masked = new HashMap<>(properties);
        if (masked.containsKey("password")) {
            masked.put("password", "***");
        }
        if (masked.containsKey("apiKey")) {
            masked.put("apiKey", "***");
        }
        if (masked.containsKey("secret")) {
            masked.put("secret", "***");
        }
        return "ConnectionConfig" + masked;
    }
}
