package com.alibaba.alink.operator.common.io.types;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.utils.TypeStringUtils;

/**
 * Type conversion utils between flink TypeInformation and string.
 */
public class FlinkTypeConverter {

    /**
     * Convert TypeInformation to string representation.
     *
     * @param type TypeInformation
     * @return string representation of the type.
     */
    public static String getTypeString(TypeInformation<?> type) {
        return TypeStringUtils.writeTypeInfo(type);
    }

    /**
     * Convert TypeInformation array to string format array.
     *
     * @param types TypeInformation array
     * @return string representation of the types.
     */
    public static String[] getTypeString(TypeInformation<?>[] types) {
        String[] sqlTypes = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            sqlTypes[i] = TypeStringUtils.writeTypeInfo(types[i]);
        }
        return sqlTypes;
    }

    /**
     * Convert string representation to flink TypeInformation.
     *
     * @param typeSQL string representation of type
     * @return flink TypeInformation
     */
    public static TypeInformation<?> getFlinkType(String typeSQL) {
        return TypeStringUtils.readTypeInfo(typeSQL);
    }
}
