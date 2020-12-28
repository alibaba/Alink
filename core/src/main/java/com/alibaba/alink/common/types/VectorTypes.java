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

	//public static class PlaceHolderSerializer<T> extends TypeSerializerSingleton <T> {
	//
	//	@Override
	//	public boolean isImmutableType() {
	//		return false;
	//	}
	//
	//	@Override
	//	public T createInstance() {
	//		return null;
	//	}
	//
	//	@Override
	//	public T copy(T from) {
	//		return null;
	//	}
	//
	//	@Override
	//	public T copy(T from, T reuse) {
	//		return null;
	//	}
	//
	//	@Override
	//	public int getLength() {
	//		return 0;
	//	}
	//
	//	@Override
	//	public void serialize(T record, DataOutputView target) throws IOException {
	//
	//	}
	//
	//	@Override
	//	public T deserialize(DataInputView source) throws IOException {
	//		return null;
	//	}
	//
	//	@Override
	//	public T deserialize(T reuse, DataInputView source) throws IOException {
	//		return null;
	//	}
	//
	//	@Override
	//	public void copy(DataInputView source, DataOutputView target) throws IOException {
	//
	//	}
	//
	//	@Override
	//	public TypeSerializerSnapshot <T> snapshotConfiguration() {
	//		return null;
	//	}
	//}
	//
	//public static final BasicTypeInfo <Vector> VECTOR = new BasicTypeInfo <Vector>(
	//	Vector.class, new Class[0], new PlaceHolderSerializer <>(), null) {
	//
	//	@Override
	//	public TypeSerializer <Vector> createSerializer(ExecutionConfig executionConfig) {
	//		return new KryoSerializer <>(Vector.class, executionConfig);
	//	}
	//};
	//
	//public static final BasicTypeInfo <SparseVector> SPARSE_VECTOR = new BasicTypeInfo <SparseVector>(
	//	SparseVector.class, new Class[0], new PlaceHolderSerializer <>(), null) {
	//
	//	@Override
	//	public TypeSerializer <SparseVector> createSerializer(ExecutionConfig executionConfig) {
	//		return new KryoSerializer <>(SparseVector.class, executionConfig);
	//	}
	//};
	//
	//public static final BasicTypeInfo <DenseVector> DENSE_VECTOR = new BasicTypeInfo <DenseVector>(
	//	DenseVector.class, new Class[0], new PlaceHolderSerializer <>(), null) {
	//
	//	@Override
	//	public TypeSerializer <DenseVector> createSerializer(ExecutionConfig executionConfig) {
	//		return new KryoSerializer <>(DenseVector.class, executionConfig);
	//	}
	//};

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

	public static class SparseVectorTypeInternal extends PojoTypeInfo <SparseVector> {
		public SparseVectorTypeInternal() {
			super(SparseVector.class, getSparseVectorFields());
		}
	}

	private static List <PojoField> getSparseVectorFields() {
		Map <String, TypeInformation <?>> fields = new HashMap <>();
		fields.put("n", Types.INT);
		fields.put("indices", Types.PRIMITIVE_ARRAY(Types.INT));
		fields.put("values", Types.PRIMITIVE_ARRAY(Types.DOUBLE));

		final List <PojoField> pojoFields = new ArrayList <>(fields.size());
		for (Map.Entry <String, TypeInformation <?>> field : fields.entrySet()) {
			final Field f = TypeExtractor.getDeclaredField(SparseVector.class, field.getKey());
			if (f == null) {
				throw new InvalidTypesException("Field '" + field.getKey() + "'could not be accessed.");
			}
			pojoFields.add(new PojoField(f, field.getValue()));
		}

		return pojoFields;
	}

	public static class DenseVectorTypeInternal extends PojoTypeInfo <DenseVector> {
		public DenseVectorTypeInternal() {
			super(DenseVector.class, getDenseVectorFields());
		}
	}

	private static List <PojoField> getDenseVectorFields() {
		Map <String, TypeInformation <?>> fields = new HashMap <>();
		fields.put("data", Types.PRIMITIVE_ARRAY(Types.DOUBLE));

		final List <PojoField> pojoFields = new ArrayList <>(fields.size());
		for (Map.Entry <String, TypeInformation <?>> field : fields.entrySet()) {
			final Field f = TypeExtractor.getDeclaredField(DenseVector.class, field.getKey());
			if (f == null) {
				throw new InvalidTypesException("Field '" + field.getKey() + "'could not be accessed.");
			}
			pojoFields.add(new PojoField(f, field.getValue()));
		}

		return pojoFields;
	}
}
