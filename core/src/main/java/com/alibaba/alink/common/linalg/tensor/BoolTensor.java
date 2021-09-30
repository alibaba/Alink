package com.alibaba.alink.common.linalg.tensor;

import org.apache.flink.util.Preconditions;

import org.tensorflow.ndarray.BooleanNdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.BooleanDataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;

public final class BoolTensor extends Tensor <Boolean> {

	public BoolTensor(Shape shape) {
		this(NdArrays.ofBooleans(shape.toNdArrayShape()));
	}

	public BoolTensor(boolean[] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public BoolTensor(boolean[][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public BoolTensor(boolean[][][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	BoolTensor(BooleanNdArray data) {
		super(data, DataType.BOOLEAN);
	}

	@Override
	void parseFromValueStrings(String[] valueStrings) {
		boolean[] arr = new boolean[valueStrings.length];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = Boolean.parseBoolean(valueStrings[i]);
		}
		data.write(DataBuffers.of(arr));
	}

	@Override
	String[] getValueStrings() {
		int isize = Math.toIntExact(size());
		boolean[] arr = new boolean[isize];
		BooleanDataBuffer buffer = DataBuffers.of(arr, false, false);
		data.read(buffer);
		String[] valueStrs = new String[isize];
		for (int i = 0; i < isize; i += 1) {
			valueStrs[i] = Boolean.toString(arr[i]);
		}
		return valueStrs;
	}

	public boolean getBoolean(long... coordinates) {
		return ((BooleanNdArray) data).getBoolean(coordinates);
	}

	public BoolTensor setBoolean(boolean value, long... coordinates) {
		((BooleanNdArray) data).setBoolean(value, coordinates);
		return this;
	}

	@Override
	public BoolTensor reshape(Shape newShape) {
		Preconditions.checkArgument(newShape.size() == size(), "Shape not matched.");
		BooleanDataBuffer buffer = DataBuffers.ofBooleans(size());
		data.read(buffer);
		return new BoolTensor(NdArrays.wrap(newShape.toNdArrayShape(), buffer));
	}
}
