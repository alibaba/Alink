package com.alibaba.alink.operator.common.io.types;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.util.Preconditions;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Type conversion utils against {@link java.sql.Types}.
 *
 * There're two special cases in which Types.LONGVARCHAR and Types.NULL are mapped to String.
 */
public class JdbcTypeConverter {
    /**
     * Mapping from {@link java.sql.Types} (in integer form) to Flink TypeInformation.
     */
    private static final Map<Integer, TypeInformation<?>> MAP_INDEX_TO_FLINK_TYPE;

    /**
     * Mapping from Flink TypeInformation to {@link java.sql.Types} integers.
     */
    private static final Map<TypeInformation<?>, Integer> MAP_FLINK_TYPE_TO_INDEX;

    static {
        HashMap<TypeInformation<?>, Integer> m1 = new HashMap<>();
        m1.put(BasicTypeInfo.STRING_TYPE_INFO, Types.VARCHAR);
        m1.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, Types.BOOLEAN);
        m1.put(BasicTypeInfo.BYTE_TYPE_INFO, Types.TINYINT);
        m1.put(BasicTypeInfo.SHORT_TYPE_INFO, Types.SMALLINT);
        m1.put(BasicTypeInfo.INT_TYPE_INFO, Types.INTEGER);
        m1.put(BasicTypeInfo.LONG_TYPE_INFO, Types.BIGINT);
        m1.put(BasicTypeInfo.FLOAT_TYPE_INFO, Types.FLOAT);
        m1.put(BasicTypeInfo.DOUBLE_TYPE_INFO, Types.DOUBLE);
        m1.put(SqlTimeTypeInfo.DATE, Types.DATE);
        m1.put(SqlTimeTypeInfo.TIME, Types.TIME);
        m1.put(SqlTimeTypeInfo.TIMESTAMP, Types.TIMESTAMP);
        m1.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, Types.DECIMAL);
        m1.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BINARY);
        MAP_FLINK_TYPE_TO_INDEX = Collections.unmodifiableMap(m1);

        HashMap<Integer, TypeInformation<?>> m3 = new HashMap<>();
        m3.put(Types.LONGVARCHAR, BasicTypeInfo.STRING_TYPE_INFO);
        m3.put(Types.VARCHAR, BasicTypeInfo.STRING_TYPE_INFO);
        m3.put(Types.NULL, BasicTypeInfo.STRING_TYPE_INFO);
        m3.put(Types.BOOLEAN, BasicTypeInfo.BOOLEAN_TYPE_INFO);
        m3.put(Types.TINYINT, BasicTypeInfo.BYTE_TYPE_INFO);
        m3.put(Types.SMALLINT, BasicTypeInfo.SHORT_TYPE_INFO);
        m3.put(Types.INTEGER, BasicTypeInfo.INT_TYPE_INFO);
        m3.put(Types.BIGINT, BasicTypeInfo.LONG_TYPE_INFO);
        m3.put(Types.FLOAT, BasicTypeInfo.FLOAT_TYPE_INFO);
        m3.put(Types.DOUBLE, BasicTypeInfo.DOUBLE_TYPE_INFO);
        m3.put(Types.DATE, SqlTimeTypeInfo.DATE);
        m3.put(Types.TIME, SqlTimeTypeInfo.TIME);
        m3.put(Types.TIMESTAMP, SqlTimeTypeInfo.TIMESTAMP);
        m3.put(Types.DECIMAL, BasicTypeInfo.BIG_DEC_TYPE_INFO);
        m3.put(Types.BINARY, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        MAP_INDEX_TO_FLINK_TYPE = Collections.unmodifiableMap(m3);
    }

    /**
     * Get {@link java.sql.Types} (in integer form) from Flink TypeInformation.
     *
     * @param type flink TypeInformation.
     * @return Corresponding type integer in {@link java.sql.Types}.
	 * @throws IllegalArgumentException when unsupported type encountered.
     */
    public static int getIntegerSqlType(TypeInformation<?> type) {
        if (MAP_FLINK_TYPE_TO_INDEX.containsKey(type)) {
            return MAP_FLINK_TYPE_TO_INDEX.get(type);
        } else if (type instanceof ObjectArrayTypeInfo || type instanceof PrimitiveArrayTypeInfo) {
            return Types.ARRAY;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

	/**
	 * Get {@link java.sql.Types} (in integer form) from Flink TypeInformation.
	 *
	 * @param typeIndex type integer in {@link java.sql.Types}.
	 * @return flink TypeInformation.
	 * @throws IllegalArgumentException when unsupported type encountered.
	 */
    public static TypeInformation<?> getFlinkType(int typeIndex) {
		TypeInformation<?> typeInformation = MAP_INDEX_TO_FLINK_TYPE.get(typeIndex);
		Preconditions.checkArgument(typeInformation != null, "Unsupported type: %s", typeIndex);
		return typeInformation;
    }

}
