package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.buffer.DataBuffer;

import java.util.Arrays;

public class TensorUtil {

	/**
	 * Delimiter between elements.
	 */
	static final char ELEMENT_DELIMITER = ' ';
	static final String ELEMENT_DELIMITER_STR = "" + ELEMENT_DELIMITER;
	/**
	 * Delimiter between vector size and vector data.
	 */
	static final char HEADER_DELIMITER = '#';
	static final String HEADER_DELIMITER_STR = "" + HEADER_DELIMITER;
	/**
	 * Delimiter between shape dimensions.
	 */
	static final char SHAPE_DELIMITER = ',';
	static final String SHAPE_DELIMITER_STR = "" + SHAPE_DELIMITER;

	static final int NULL_STRING_LENGTH = -1;

	public static Tensor <?> getTensor(Object obj) {
		if (null == obj) {
			return null;
		}
		if (obj instanceof Tensor <?>) {
			return (Tensor <?>) obj;
		} else if (obj instanceof Vector) {
			return fromDenseVector(VectorUtil.getDenseVector(obj));
		} else if (obj instanceof String) {
			String objStr = (String) obj;
			if (isTensor(objStr)) {
				return parseTensor(objStr);
			} else {
				try {
					return fromDenseVector(VectorUtil.getDenseVector(objStr));
				} catch (Exception ex) {
					return new StringTensor(objStr);
				}
			}
		} else if (obj instanceof Number) {
			return fromDenseVector(new DenseVector(new double[] {((Number) obj).doubleValue()}));
		} else {
			throw new AkIllegalArgumentException("Can not get the tensor from " + obj);
		}
	}

	/**
	 * Parse a tensor from s. See {@link TensorUtil#toString}.
	 */
	public static Tensor <?> parseTensor(String s) {
		String[] split = StringUtils.splitPreserveAllTokens(s, TensorUtil.HEADER_DELIMITER_STR, 3);
		if (split.length != 3) {
			throw new AkParseErrorException("Illegal tensor string: " + s);
		}
		DataType type = DataType.valueOf(split[0]);
		Shape shape = TensorUtil.parseShapeStr(split[1]);

		String[] valueStrs;
		if (DataType.STRING.equals(type)) {
			valueStrs = parseStringValueStr(split[2], Math.toIntExact(shape.size()));
		} else {
			valueStrs = split[2].split(TensorUtil.ELEMENT_DELIMITER_STR);
		}
		Tensor <?> tensor;
		switch (type) {
			case FLOAT:
				tensor = new FloatTensor(shape);
				break;
			case DOUBLE:
				tensor = new DoubleTensor(shape);
				break;
			case INT:
				tensor = new IntTensor(shape);
				break;
			case LONG:
				tensor = new LongTensor(shape);
				break;
			case BOOLEAN:
				tensor = new BoolTensor(shape);
				break;
			case BYTE:
				tensor = new ByteTensor(shape);
				break;
			case UBYTE:
				tensor = new UByteTensor(shape);
				break;
			case STRING:
				tensor = new StringTensor(shape);
				break;
			default:
				throw new AkUnsupportedOperationException("Data type is not supported: " + type);
		}
		tensor.parseFromValueStrings(valueStrs);
		return tensor;
	}

	private static String[] parseStringValueStr(String s, int size) {
		String[] split = StringUtils.split(s, HEADER_DELIMITER_STR, 2);
		int[] lengths = Arrays.stream(StringUtils.split(split[0], ELEMENT_DELIMITER))
			.mapToInt(Integer::parseInt)
			.toArray();
		if (lengths.length != size) {
			throw new AkIllegalDataException("Illegal lengths section in tensor string: " + s);
		}
		String content = (split.length > 1) ? split[1] : "";
		String[] strs = new String[size];
		int start = 0;
		for (int i = 0; i < size; i += 1) {
			if (NULL_STRING_LENGTH != lengths[i]) {
				strs[i] = content.substring(start, start + lengths[i]);
				start += lengths[i] + 1;
			}
		}
		return strs;
	}

	/**
	 * Convert a tensor to a string.
	 * <p>
	 * For tensors except StringTensor, the result includes 3 sections: tensor type, tensor shape, and tensor data,
	 * which are separated by {@link TensorUtil#HEADER_DELIMITER}. The tensor data section stores the concatenation of
	 * String representations of all values separated by {@link TensorUtil#ELEMENT_DELIMITER}. An example is:
	 * "FLOAT_TENSOR#2,2#0.6 0.7 0.8 0.9 ".
	 * <p>
	 * As for {@link StringTensor}, an additional section is used to store the lengths of all data, i.e. strings. There
	 * are also separated by {@link TensorUtil#ELEMENT_DELIMITER}. An example is : "STRING_TENSOR#2 2 #2 3 3 2#ab bcd
	 * def fg ".
	 */
	public static String toString(Tensor <?> tensor) {
		StringBuilder sbd = new StringBuilder();
		sbd.append(tensor.type.name());
		sbd.append(HEADER_DELIMITER);
		sbd.append(toString(Shape.fromNdArrayShape(tensor.data.shape())));
		sbd.append(HEADER_DELIMITER);
		String[] valueStrs = tensor.getValueStrings();
		if (tensor instanceof StringTensor) {
			for (String valueStr : valueStrs) {
				int length = null != valueStr ? valueStr.length() : NULL_STRING_LENGTH;
				sbd.append(length).append(TensorUtil.ELEMENT_DELIMITER);
			}
			sbd.append(HEADER_DELIMITER);
		}
		for (String valueStr : valueStrs) {
			if (null != valueStr) {
				sbd.append(valueStr).append(TensorUtil.ELEMENT_DELIMITER);
			}
		}
		return sbd.toString();
	}

	/**
	 * Serialize the tensor to a string.
	 */
	public static String serialize(Object tensor) {
		return toString((Tensor<?>) tensor);
	}

	static Shape parseShapeStr(String s) {
		if (s.isEmpty()) {
			return new Shape();
		}
		long[] dimensions = Arrays.stream(s.split(SHAPE_DELIMITER_STR))
			.mapToLong((Long::parseLong))
			.toArray();
		return new Shape(dimensions);
	}

	static String toString(Shape shape) {
		return Longs.join(SHAPE_DELIMITER_STR, shape.asArray());
	}

	private static Tensor <?> fromDenseVector(DenseVector denseVector) {
		if (denseVector == null) {
			return null;
		}
		return new DoubleTensor(denseVector.getData());
	}

	private static boolean isTensor(String str) {
		return str.contains(HEADER_DELIMITER_STR);
	}

	static <DT, T extends Tensor <DT>> T of(long[] size, DataType dtype) {
		switch (dtype) {
			case INT:
				return (T) new IntTensor(new Shape(size));
			case BYTE:
				return (T) new ByteTensor(new Shape(size));
			case LONG:
				return (T) new LongTensor(new Shape(size));
			case FLOAT:
				return (T) new FloatTensor(new Shape(size));
			case DOUBLE:
				return (T) new DoubleTensor(new Shape(size));
			case UBYTE:
				return (T) new UByteTensor(new Shape(size));
			case BOOLEAN:
				return (T) new BoolTensor(new Shape(size));
			case STRING:
				return (T) new StringTensor(new Shape(size));
			default:
				throw new AkUnsupportedOperationException("Failed to cast to tensor, unsupported DataType");
		}
	}

	static long wrapDim(long dim, long nDims) {
		if (nDims == 0) {
			if (dim == 0) {
				return dim;
			} else {
				throw new AkIllegalDataException("Dim is not 0 when nDims is 0.");
			}
		}

		final long min = -nDims;
		final long max = nDims - 1;

		if (dim < min || dim > max) {
			throw new AkIllegalDataException(
				String.format("Dimension is outbound. Dim: %d, min: %d, max: %d.", dim, min, max)
			);
		}

		if (dim < 0) {
			return dim + nDims;
		}

		return dim;
	}

	static <DT, DTARRAY, T extends Tensor <DT>>
	T doCalc(T tensor, int dim, boolean keepDim, DoCalcFunctions <DT, DTARRAY> functions) {

		long[] shape = tensor.shape();
		int size = shape.length;
		int wrappedDim = (int) wrapDim(dim, size);

		int outer = 1;
		for (int i = 0; i < wrappedDim; ++i) {
			outer *= shape[i];
		}

		int inner = 1;
		for (int i = wrappedDim + 1; i < size; ++i) {
			inner *= shape[i];
		}

		long[] outerCoords = new long[wrappedDim + 1];
		CoordInc outerCoordInc = new CoordInc(shape, wrappedDim, outerCoords);

		long[] innerCoords = new long[size - wrappedDim - 1];
		CoordInc innerCoordInc = new CoordInc(shape, wrappedDim + 1, size, innerCoords);

		long[] oShape;

		if (keepDim) {
			oShape = new long[size];
			System.arraycopy(shape, 0, oShape, 0, size);
			oShape[wrappedDim] = 1;
		} else {
			oShape = ArrayUtils.remove(shape, wrappedDim);
		}

		int dimSize = (int) shape[wrappedDim];

		T ret = of(oShape, tensor.getType());

		DTARRAY buffer = functions.createArray(inner);

		for (int i = 0; i < outer; ++i, outerCoordInc.inc()) {

			functions.initial(buffer);

			for (int j = 0; j < dimSize; ++j) {

				outerCoords[wrappedDim] = j;

				NdArray <DT> ndArray = tensor.getData().get(outerCoords);

				innerCoordInc.reset();
				for (int z = 0; z < inner; ++z, innerCoordInc.inc()) {
					functions.calc(buffer, ndArray, innerCoords, z);
				}
			}

			functions.post(buffer, dimSize);

			long[] writeCoords;

			if (keepDim) {
				outerCoords[wrappedDim] = 0;
				writeCoords = outerCoords;
			} else {
				writeCoords = ArrayUtils.remove(outerCoords, wrappedDim);
			}

			ret.getData().get(writeCoords).write(functions.write(buffer));
		}

		return ret;
	}

	interface DoCalcFunctions<DT, DTARRAY> {
		DTARRAY createArray(int size);

		void initial(DTARRAY array);

		void calc(DTARRAY array, NdArray <DT> ndArray, long[] coords, int index);

		DataBuffer <DT> write(DTARRAY array);

		default void post(DTARRAY array, int count) {
			// pass
		}
	}

	static class CoordInc {
		private final long[] refs;
		private final long[] shapes;
		private final int start;
		private final int end;

		public CoordInc(long[] shape, int dim, long[] refs) {
			this(shape, 0, dim, refs);
		}

		public CoordInc(long[] shape, int start, int end, long[] refs) {
			this.refs = refs;
			this.shapes = shape;
			this.start = start;
			this.end = end;
		}

		public void inc() {
			for (int i = end - 1, j = end - start - 1; i >= start; --i, --j) {
				if (refs[j] < shapes[i] - 1) {
					refs[j] += 1L;
					return;
				}

				refs[j] = 0L;
			}
		}

		public void reset() {
			Arrays.fill(refs, 0, end - start, 0L);
		}
	}
}
