package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.tensor.TensorUtil.DoCalcFunctions;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.DataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.ndarray.buffer.IntDataBuffer;

import java.util.Arrays;

public final class IntTensor extends NumericalTensor <Integer> {

	public IntTensor(Shape shape) {
		this(NdArrays.ofInts(shape.toNdArrayShape()));
	}

	public IntTensor(int data) {
		this(NdArrays.scalarOf(data));
	}

	public IntTensor(int[] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public IntTensor(int[][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public IntTensor(int[][][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	IntTensor(IntNdArray data) {
		super(data, DataType.INT);
	}

	@Override
	void parseFromValueStrings(String[] valueStrings) {
		int[] arr = new int[valueStrings.length];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = Integer.parseInt(valueStrings[i]);
		}
		data.write(DataBuffers.of(arr));
	}

	@Override
	String[] getValueStrings() {
		int isize = Math.toIntExact(size());
		int[] arr = new int[isize];
		IntDataBuffer buffer = DataBuffers.of(arr, false, false);
		data.read(buffer);
		String[] valueStrs = new String[isize];
		for (int i = 0; i < isize; i += 1) {
			valueStrs[i] = Integer.toString(arr[i]);
		}
		return valueStrs;
	}

	public int getInt(long... coordinates) {
		return ((IntNdArray) data).getInt(coordinates);
	}

	public IntTensor setInt(int value, long... coordinates) {
		((IntNdArray) data).setInt(value, coordinates);
		return this;
	}

	@Override
	public IntTensor reshape(Shape newShape) {
		AkPreconditions.checkArgument(newShape.size() == size(), "Shape not matched.");
		IntDataBuffer buffer = DataBuffers.ofInts(size());
		data.read(buffer);
		return new IntTensor(NdArrays.wrap(newShape.toNdArrayShape(), buffer));
	}

	@Override
	public IntTensor min(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Integer, int[]>() {
				@Override
				public int[] createArray(int size) {
					return new int[size];
				}

				@Override
				public void initial(int[] array) {
					Arrays.fill(array, Integer.MAX_VALUE);
				}

				@Override
				public void calc(int[] array, NdArray <Integer> ndArray, long[] coords, int index) {
					array[index] = Math.min(ndArray.getObject(coords), array[index]);
				}

				@Override
				public DataBuffer <Integer> write(int[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public IntTensor max(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Integer, int[]>() {
				@Override
				public int[] createArray(int size) {
					return new int[size];
				}

				@Override
				public void initial(int[] array) {
					Arrays.fill(array, Integer.MIN_VALUE);
				}

				@Override
				public void calc(int[] array, NdArray <Integer> ndArray, long[] coords, int index) {
					array[index] = Math.max(ndArray.getObject(coords), array[index]);
				}

				@Override
				public DataBuffer <Integer> write(int[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public IntTensor sum(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Integer, int[]>() {
				@Override
				public int[] createArray(int size) {
					return new int[size];
				}

				@Override
				public void initial(int[] array) {
					Arrays.fill(array, 0);
				}

				@Override
				public void calc(int[] array, NdArray <Integer> ndArray, long[] coords, int index) {
					array[index] = ndArray.getObject(coords) + array[index];
				}

				@Override
				public DataBuffer <Integer> write(int[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public IntTensor mean(int dim, boolean keepDim) {
		throw new AkUnsupportedOperationException("Not support exception. ");
	}
}
