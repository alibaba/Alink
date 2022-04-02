package com.alibaba.alink.common.pyrunner.fn.conversion;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import com.alibaba.alink.common.pyrunner.fn.JavaObjectWrapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.tensorflow.ndarray.Shape;

import java.util.Arrays;
import java.util.List;

public class TensorWrapper implements JavaObjectWrapper<Tensor<?>> {

	private static final BiMap <DataType, String> TYPE_MAPPING = HashBiMap.create();

	static {
		TYPE_MAPPING.put(DataType.FLOAT, "<f4");
		TYPE_MAPPING.put(DataType.DOUBLE, "<f8");
		TYPE_MAPPING.put(DataType.INT, "<i4");
		TYPE_MAPPING.put(DataType.LONG, "<i8");
		TYPE_MAPPING.put(DataType.BOOLEAN, "<?");
		TYPE_MAPPING.put(DataType.BYTE, "<b");
		TYPE_MAPPING.put(DataType.UBYTE, "<B");
		// TODO: DataType.STRING
	}

	private Tensor <?> tensor;
	private byte[] bytes;
	private String dtypeStr;
	private long[] shape;

	private TensorWrapper() {
	}

	public static TensorWrapper fromJava(Tensor <?> tensor) {
		TensorWrapper wrapper = new TensorWrapper();
		wrapper.tensor = tensor;
		wrapper.dtypeStr = TYPE_MAPPING.get(tensor.getType());
		wrapper.shape = tensor.shape();
		wrapper.bytes = TensorInternalUtils.tensorToBytes(tensor);
		return wrapper;
	}

	public static TensorWrapper fromPy(byte[] bytes, String dtypeStr, List <Integer> shape) {
		return fromPy(bytes, dtypeStr, shape.stream().mapToLong(d -> d).toArray());
	}

	public static TensorWrapper fromPy(byte[] bytes, String dtypeStr, long[] shape) {
		TensorWrapper wrapper = new TensorWrapper();
		wrapper.bytes = bytes;
		wrapper.dtypeStr = dtypeStr;
		wrapper.shape = shape;
		Preconditions.checkArgument(TYPE_MAPPING.inverse().containsKey(dtypeStr),
			String.format("Numpy array of type %s is not supported.", dtypeStr));
		wrapper.tensor = TensorInternalUtils.bytesToTensor(bytes, TYPE_MAPPING.inverse().get(dtypeStr),
			Shape.of(shape));
		return wrapper;
	}

	@Override
	public String toString() {
		return "TensorWrapper{" +
			"tensor=" + tensor +
			", bytes=" + Arrays.toString(bytes) +
			", dtypeStr='" + dtypeStr + '\'' +
			", shape=" + Arrays.toString(shape) +
			'}';
	}

	@Override
	public Tensor <?> getJavaObject() {
		return tensor;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public long[] getShape() {
		return shape;
	}

	public String getDtypeStr() {
		return dtypeStr;
	}
}
