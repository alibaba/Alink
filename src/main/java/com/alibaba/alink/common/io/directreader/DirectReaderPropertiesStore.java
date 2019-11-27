package com.alibaba.alink.common.io.directreader;

import java.io.Serializable;
import java.util.Properties;

/**
 * Global property store for {@link DirectReader}.
 */
public class DirectReaderPropertiesStore implements Serializable {

    private static Properties PROPERTIES = new Properties();

    /**
     * Set the global properties to new value.
     *
     * @param newProperties to set.
     */
    public static synchronized void setProperties(Properties newProperties) {
        PROPERTIES = newProperties;
    }

    /**
     * Get the current global properties.
     *
     * @return current global properties
     */
    public static synchronized Properties getProperties() {
        return PROPERTIES;
    }

}
