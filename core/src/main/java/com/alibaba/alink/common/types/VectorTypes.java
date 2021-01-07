package com.alibaba.alink.common.types;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorTypes {

	public static class VectorTypeInternal extends CompositeType <Vector> {

		public VectorTypeInternal() {
			super(Vector.class);
		}

		@Override
		public void getFlatFields(String fieldExpression, int offset, List <FlatFieldDescriptor> result) {

		}

		@Override
		public <X> TypeInformation <X> getTypeAt(String fieldExpression) {
			return null;
		}

		@Override
		public <X> TypeInformation <X> getTypeAt(int pos) {
			return null;
		}

		@Override
		protected TypeComparatorBuilder <Vector> createTypeComparatorBuilder() {
			return null;
		}

		@Override
		public String[] getFieldNames() {
			return new String[0];
		}

		@Override
		public int getFieldIndex(String fieldName) {
			return 0;
		}

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
			return 0;
		}

		@Override
		public int getTotalFields() {
			return 0;
		}

		@Override
		public TypeSerializer <Vector> createSerializer(ExecutionConfig config) {
			return new KryoSerializer <>(Vector.class, config);
		}
	}
}
