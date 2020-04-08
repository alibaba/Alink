package com.alibaba.alink.params;

import org.apache.flink.ml.api.misc.param.ParamInfo;

/**
 * Util for the parameters.
 */
public class ParamUtil {
    /**
     * Convert string to enum, and throw exception.
     * @param enumeration enum class
     * @param search search item
     * @param paramName param name
     * @param <T> class
     * @return enum
     */
    public static <T extends Enum<?>> T searchEnum(Class<T> enumeration, String search, String paramName) {
        return searchEnum(enumeration, search, paramName, null);
    }

    /**
     * Convert string to enum, and throw exception.
     * @param paramInfo paramInfo
     * @param search search item
     * @param <T> class
     * @return enum
     */
    public static <T extends Enum<?>> T searchEnum(ParamInfo<T> paramInfo, String search) {
        return searchEnum(paramInfo.getValueClass(), search, paramInfo.getName());
    }

    /**
     * Convert string to enum, and throw exception.
     * @param enumeration enum class
     * @param search search item
     * @param paramName param name
     * @param opName op name
     * @param <T> class
     * @return enum
     */
    public static <T extends Enum<?>> T searchEnum(Class<T> enumeration, String search, String paramName, String opName) {
        if (search == null) {
            return null;
        }
        T[] values = enumeration.getEnumConstants();
        for (T each : values) {
            if (each.name().compareToIgnoreCase(search) == 0) {
                return each;
            }
        }

        StringBuilder sbd = new StringBuilder();
        sbd.append(search)
            .append(" is not member of ")
            .append(paramName);
        if (opName != null && opName.isEmpty()) {
            sbd.append(" of ")
                .append(opName);
        }
        sbd.append(".")
            .append("It maybe ")
            .append(values[0].name());
        for (int i = 1; i < values.length; i++) {
            sbd.append(",")
                .append(values[i].name());
        }
        sbd.append(".");
        throw new RuntimeException(sbd.toString());
    }
}
