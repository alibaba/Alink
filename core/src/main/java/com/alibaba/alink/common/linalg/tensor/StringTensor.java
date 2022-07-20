package com.alibaba.alink.common.linalg.tensor;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.DataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;

public final class StringTensor extends Tensor <String> {

	public StringTensor(Shape shape) {
		this(NdArrays.ofObjects(String.class, shape.toNdArrayShape()));
	}

	public StringTensor(String data) {
		this(NdArrays.scalarOfObject(data));
	}

	public StringTensor(String[] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public StringTensor(String[][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public StringTensor(String[][][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	StringTensor(NdArray <String> data) {
		super(data, DataType.STRING);
	}

	@Override
	void parseFromValueStrings(String[] valueStrings) {
		String[] arr = new String[valueStrings.length];
		System.arraycopy(valueStrings, 0, arr, 0, arr.length);
		data.write(DataBuffers.of(arr, false, false));
	}

	@Override
	String[] getValueStrings() {
		int isize = Math.toIntExact(size());
		String[] arr = new String[isize];
		DataBuffer <String> buffer = DataBuffers.of(arr, false, false);
		data.read(buffer);
		String[] valueStrs = new String[isize];
		System.arraycopy(arr, 0, valueStrs, 0, isize);
		return valueStrs;
	}

	public String getString(long... coordinates) {
		return data.getObject(coordinates);
	}

	public StringTensor setString(String value, long... coordinates) {
		data.setObject(value, coordinates);
		return this;
	}

	@Override
	public StringTensor reshape(Shape newShape) {
		AkPreconditions.checkArgument(newShape.size() == size(), "Shape not matched.");
		DataBuffer <String> buffer = DataBuffers.ofObjects(String.class, size());
		data.read(buffer);
		return new StringTensor(NdArrays.wrap(newShape.toNdArrayShape(), buffer));
	}
}
