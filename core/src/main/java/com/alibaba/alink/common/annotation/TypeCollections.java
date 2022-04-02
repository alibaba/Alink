package com.alibaba.alink.common.annotation;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import static com.alibaba.alink.common.AlinkTypes.BOOL_TENSOR;
import static com.alibaba.alink.common.AlinkTypes.BYTE_TENSOR;
import static com.alibaba.alink.common.AlinkTypes.DENSE_VECTOR;
import static com.alibaba.alink.common.AlinkTypes.DOUBLE_TENSOR;
import static com.alibaba.alink.common.AlinkTypes.FLOAT_TENSOR;
import static com.alibaba.alink.common.AlinkTypes.INT_TENSOR;
import static com.alibaba.alink.common.AlinkTypes.LONG_TENSOR;
import static com.alibaba.alink.common.AlinkTypes.M_TABLE;
import static com.alibaba.alink.common.AlinkTypes.SPARSE_VECTOR;
import static com.alibaba.alink.common.AlinkTypes.STRING_TENSOR;
import static com.alibaba.alink.common.AlinkTypes.TENSOR;
import static com.alibaba.alink.common.AlinkTypes.UBYTE_TENSOR;
import static com.alibaba.alink.common.AlinkTypes.VECTOR;
import static org.apache.flink.api.common.typeinfo.Types.BIG_DEC;
import static org.apache.flink.api.common.typeinfo.Types.BIG_INT;
import static org.apache.flink.api.common.typeinfo.Types.BOOLEAN;
import static org.apache.flink.api.common.typeinfo.Types.BYTE;
import static org.apache.flink.api.common.typeinfo.Types.DOUBLE;
import static org.apache.flink.api.common.typeinfo.Types.FLOAT;
import static org.apache.flink.api.common.typeinfo.Types.INT;
import static org.apache.flink.api.common.typeinfo.Types.LONG;
import static org.apache.flink.api.common.typeinfo.Types.PRIMITIVE_ARRAY;
import static org.apache.flink.api.common.typeinfo.Types.SHORT;
import static org.apache.flink.api.common.typeinfo.Types.SQL_DATE;
import static org.apache.flink.api.common.typeinfo.Types.SQL_TIME;
import static org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;
import static org.apache.flink.api.common.typeinfo.Types.STRING;

public enum TypeCollections {
	STRING_TYPES(STRING),
	INT_LONG_TYPES(INT, LONG),
	STRING_TYPE(STRING),
	LONG_TYPES(LONG),
	INT_LONG_STRING_TYPES(INT, LONG, STRING),
	DOUBLE_TYPE(DOUBLE),
	TREE_FEATURE_TYPES(
		INT,
		LONG,
		DOUBLE,
		FLOAT,
		STRING,
		SHORT,
		BOOLEAN,
		SQL_DATE,
		SQL_TIME,
		SQL_TIMESTAMP
	),
	NUMERIC_TYPES(
		INT, LONG, SHORT, BYTE, DOUBLE, FLOAT, BIG_DEC, BIG_INT
	),
	VECTOR_TYPES(
		STRING, VECTOR, SPARSE_VECTOR, DENSE_VECTOR
	),
	TIMESTAMP_TYPES(
		SQL_TIMESTAMP
	),
	MTABLE_TYPES(
		M_TABLE
	),
	TENSOR_TYPES(
		TENSOR,
		BOOL_TENSOR, BYTE_TENSOR, UBYTE_TENSOR,
		DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR,
		STRING_TENSOR,
		STRING
	),
	NUMERIC_TENSOR_TYPES(
		DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR),
	NAIVE_BAYES_CATEGORICAL_TYPES(
		STRING, BOOLEAN, BIG_INT, INT, LONG
	),
	BYTES_TYPES(
		PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
	);

	private final TypeInformation <?>[] types;

	TypeCollections(TypeInformation <?>... types) {
		this.types = types;
	}

	public TypeInformation <?>[] getTypes() {
		return types;
	}
}
