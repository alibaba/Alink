package com.alibaba.alink.common.type;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.ByteTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.UByteTensor;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.google.common.collect.HashBiMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Built-in mTable types. <br/>
 * This class contains bi-direction mapping between <code>TypeInformation</code>s and their names.
 */
public class AlinkTypes extends Types {
	private static final HashBiMap <String, TypeInformation <?>> TYPES = HashBiMap.create();

	/**
	 * <code>MTable</code> type information.
	 */
	public static final TypeInformation <MTable> M_TABLE = TypeInformation.of(MTable.class);

	/**
	 * <code>DenseVector</code> type information.
	 */
	public static final TypeInformation <DenseVector> DENSE_VECTOR = TypeInformation.of(DenseVector.class);

	/**
	 * <code>SparseVector</code> type information.
	 */
	public static final TypeInformation <SparseVector> SPARSE_VECTOR = TypeInformation.of(SparseVector.class);

	/**
	 * <code>Vector</code> type information.
	 * For efficiency, use type information of sub-class <code>DenseVector</code> and <code>SparseVector</code>
	 * as much as possible. When an operator output both sub-class type of vectors, use this one.
	 */
	public static final TypeInformation <Vector> VECTOR = TypeInformation.of(Vector.class);

	public static final TypeInformation <Tensor <?>> TENSOR = TypeInformation.of(new TypeHint <Tensor <?>>() {});
	public static final TypeInformation <BoolTensor> BOOL_TENSOR = TypeInformation.of(BoolTensor.class);
	public static final TypeInformation <ByteTensor> BYTE_TENSOR = TypeInformation.of(ByteTensor.class);
	public static final TypeInformation <UByteTensor> UBYTE_TENSOR = TypeInformation.of(UByteTensor.class);
	public static final TypeInformation <DoubleTensor> DOUBLE_TENSOR = TypeInformation.of(DoubleTensor.class);
	public static final TypeInformation <FloatTensor> FLOAT_TENSOR = TypeInformation.of(FloatTensor.class);
	public static final TypeInformation <IntTensor> INT_TENSOR = TypeInformation.of(IntTensor.class);
	public static final TypeInformation <LongTensor> LONG_TENSOR = TypeInformation.of(LongTensor.class);
	public static final TypeInformation <StringTensor> STRING_TENSOR = TypeInformation.of(StringTensor.class);

	static final Set <TypeInformation <?>> ALL_TENSOR_TYPES = new HashSet <>(Arrays.asList(
		TENSOR,
		BOOL_TENSOR, BYTE_TENSOR, UBYTE_TENSOR,
		DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR,
		STRING_TENSOR
	));

	public static final TypeInformation <byte[]> VARBINARY = PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

	static {
		TYPES.put("M_TABLE", M_TABLE);
		TYPES.put("DENSE_VECTOR", DENSE_VECTOR);
		TYPES.put("SPARSE_VECTOR", SPARSE_VECTOR);
		TYPES.put("VECTOR", VECTOR);
		TYPES.put("TENSOR", TENSOR);
		TYPES.put("BOOL_TENSOR", BOOL_TENSOR);
		TYPES.put("BYTE_TENSOR", BYTE_TENSOR);
		TYPES.put("UBYTE_TENSOR", UBYTE_TENSOR);
		TYPES.put("DOUBLE_TENSOR", DOUBLE_TENSOR);
		TYPES.put("FLOAT_TENSOR", FLOAT_TENSOR);
		TYPES.put("INT_TENSOR", INT_TENSOR);
		TYPES.put("LONG_TENSOR", LONG_TENSOR);
		TYPES.put("STRING_TENSOR", STRING_TENSOR);
		TYPES.put("VARBINARY", VARBINARY);
	}

	/**
	 * Get type name from <code>TypeInformation</code>.
	 *
	 * @param type <code>TypeInformation</code>
	 * @return Corresponding type name, or null if  not found.
	 */
	public static String getTypeName(TypeInformation <?> type) {
		return TYPES.inverse().get(type);
	}

	public static TypeInformation <Row> getRowType(TypeInformation <?>... types) {
		return new RowTypeInfo(types);
	}

	/**
	 * Get <code>TypeInformation</code> from type name.
	 *
	 * @param name type name string.
	 * @return Corresponding <code>TypeInformation</code>, or null if not found.
	 */
	public static TypeInformation <?> getTypeInformation(String name) {
		if (TYPES.containsKey(name)) {
			return TYPES.get(name);
		} else {
			return FlinkTypeConverter.getFlinkType(name);
		}
	}

	public static boolean isMTableType(TypeInformation <?> type) {
		return type.equals(M_TABLE);
	}

	public static boolean isVectorType(TypeInformation <?> type) {
		return type.equals(DENSE_VECTOR) || type.equals(SPARSE_VECTOR) || type.equals(VECTOR);
	}

	public static boolean isTensorType(TypeInformation <?> type) {
		return ALL_TENSOR_TYPES.contains(type);
	}
}
