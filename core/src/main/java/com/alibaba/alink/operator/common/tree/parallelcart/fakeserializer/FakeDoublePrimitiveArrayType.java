package com.alibaba.alink.operator.common.tree.parallelcart.fakeserializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

public final class FakeDoublePrimitiveArrayType extends TypeInformation <double[]> {
	public static final TypeInformation <double[]> FAKE_DOUBLE_PRIMITIVE_ARRAY_TYPE
		= new FakeDoublePrimitiveArrayType();
	private static final long serialVersionUID = 3678771800784177148L;

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
	public Class <double[]> getTypeClass() {
		return double[].class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer <double[]> createSerializer(ExecutionConfig config) {
		return FakeDoublePrimitiveArraySerializer.INSTANCE;
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
		return (31 * Objects.hash(getTypeClass(), FakeDoublePrimitiveArraySerializer.INSTANCE));
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof Number;
	}
}
