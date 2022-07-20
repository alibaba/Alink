package com.alibaba.alink.common.pyrunner.fn.conversion;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import com.alibaba.alink.common.pyrunner.fn.JavaObjectWrapper;
import org.tensorflow.ndarray.Shape;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TensorWrapper implements JavaObjectWrapper <Tensor <?>> {

	private static final Map <DataType, String> TO_NUMPY_DTYPE = new HashMap <>();
	private static final Map <String, DataType> FROM_NUMPY_DTYPE = new HashMap <>();

	static {
		TO_NUMPY_DTYPE.put(DataType.FLOAT, "<f4");
		TO_NUMPY_DTYPE.put(DataType.DOUBLE, "<f8");
		TO_NUMPY_DTYPE.put(DataType.INT, "<i4");
		TO_NUMPY_DTYPE.put(DataType.LONG, "<i8");
		TO_NUMPY_DTYPE.put(DataType.BOOLEAN, "<?");
		TO_NUMPY_DTYPE.put(DataType.BYTE, "<b");
		TO_NUMPY_DTYPE.put(DataType.UBYTE, "<B");
		TO_NUMPY_DTYPE.put(DataType.STRING, "<U");

		TO_NUMPY_DTYPE.forEach((k, v) -> FROM_NUMPY_DTYPE.put(v, k));
		FROM_NUMPY_DTYPE.put("|b1", DataType.BOOLEAN);
		FROM_NUMPY_DTYPE.put("|i1", DataType.BYTE);
		FROM_NUMPY_DTYPE.put("|u1", DataType.UBYTE);
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
		wrapper.dtypeStr = TO_NUMPY_DTYPE.get(tensor.getType());
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
		AkPreconditions.checkArgument(FROM_NUMPY_DTYPE.containsKey(dtypeStr),
			new AkUnsupportedOperationException(
				String.format("Numpy array of type %s is not supported.", dtypeStr)));
		wrapper.tensor = TensorInternalUtils.bytesToTensor(bytes, FROM_NUMPY_DTYPE.get(dtypeStr),
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
