package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.tensor.TensorUtil.DoCalcFunctions;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.DataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.ndarray.buffer.LongDataBuffer;

import java.util.Arrays;

public final class LongTensor extends NumericalTensor <Long> {

	public LongTensor(Shape shape) {
		this(NdArrays.ofLongs(shape.toNdArrayShape()));
	}

	public LongTensor(long data) {
		this(NdArrays.scalarOf(data));
	}

	public LongTensor(long[] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public LongTensor(long[][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public LongTensor(long[][][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	LongTensor(LongNdArray data) {
		super(data, DataType.LONG);
	}

	@Override
	void parseFromValueStrings(String[] valueStrings) {
		long[] arr = new long[valueStrings.length];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = Long.parseLong(valueStrings[i]);
		}
		data.write(DataBuffers.of(arr));
	}

	@Override
	String[] getValueStrings() {
		int isize = Math.toIntExact(size());
		long[] arr = new long[isize];
		LongDataBuffer buffer = DataBuffers.of(arr, false, false);
		data.read(buffer);
		String[] valueStrs = new String[isize];
		for (int i = 0; i < isize; i += 1) {
			valueStrs[i] = Long.toString(arr[i]);
		}
		return valueStrs;
	}

	public long getLong(long... coordinates) {
		return ((LongNdArray) data).getLong(coordinates);
	}

	public LongTensor setLong(long value, long... coordinates) {
		((LongNdArray) data).setLong(value, coordinates);
		return this;
	}

	@Override
	public LongTensor reshape(Shape newShape) {
		AkPreconditions.checkArgument(newShape.size() == size(), "Shape not matched.");
		LongDataBuffer buffer = DataBuffers.ofLongs(size());
		data.read(buffer);
		return new LongTensor(NdArrays.wrap(newShape.toNdArrayShape(), buffer));
	}

	@Override
	public LongTensor min(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Long, long[]>() {
				@Override
				public long[] createArray(int size) {
					return new long[size];
				}

				@Override
				public void initial(long[] array) {
					Arrays.fill(array, Long.MAX_VALUE);
				}

				@Override
				public void calc(long[] array, NdArray <Long> ndArray, long[] coords, int index) {
					array[index] = Math.min(ndArray.getObject(coords), array[index]);
				}

				@Override
				public DataBuffer <Long> write(long[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public LongTensor max(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Long, long[]>() {
				@Override
				public long[] createArray(int size) {
					return new long[size];
				}

				@Override
				public void initial(long[] array) {
					Arrays.fill(array, Long.MIN_VALUE);
				}

				@Override
				public void calc(long[] array, NdArray <Long> ndArray, long[] coords, int index) {
					array[index] = Math.max(ndArray.getObject(coords), array[index]);
				}

				@Override
				public DataBuffer <Long> write(long[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public LongTensor sum(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Long, long[]>() {
				@Override
				public long[] createArray(int size) {
					return new long[size];
				}

				@Override
				public void initial(long[] array) {
					Arrays.fill(array, 0L);
				}

				@Override
				public void calc(long[] array, NdArray <Long> ndArray, long[] coords, int index) {
					array[index] = ndArray.getObject(coords) + array[index];
				}

				@Override
				public DataBuffer <Long> write(long[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public LongTensor mean(int dim, boolean keepDim) {
		throw new AkUnsupportedOperationException("Not support exception. ");
	}

	public static LongTensor of(Tensor <?> other) {
		if (other instanceof IntTensor) {
			IntTensor intTensor = (IntTensor) other;
			final LongTensor longTensor = new LongTensor(new Shape(intTensor.shape()));

			((IntNdArray) intTensor.getData())
				.scalars()
				.forEachIndexed((longs, intNdArray) -> {
					longTensor.setLong((long) intNdArray.getInt(), longs);
				});

			return longTensor;
		} else if (other instanceof LongTensor) {
			return (LongTensor) other;
		} else {
			throw new AkUnsupportedOperationException(String.format(
				"Failed to cast to long tensor. tensor type: %s", other.getType()
			));
		}
	}
}
