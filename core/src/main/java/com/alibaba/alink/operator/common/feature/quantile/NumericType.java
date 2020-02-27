package com.alibaba.alink.operator.common.feature.quantile;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

public final class NumericType extends TypeInformation<Number> {
	public static final TypeInformation<Number> NUMERIC_TYPE = new NumericType();

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<Number> getTypeClass() {
		return Number.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<Number> createSerializer(ExecutionConfig config) {
		return NumericSerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return getTypeClass().getSimpleName();
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof Number;
	}

	@Override
	public int hashCode() {
		return (31 * Objects.hash(getTypeClass(), NumericSerializer.INSTANCE));
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof Number;
	}
}
