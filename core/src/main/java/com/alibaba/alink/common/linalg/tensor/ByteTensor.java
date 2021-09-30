package com.alibaba.alink.common.linalg.tensor;

import org.apache.flink.util.Preconditions;

import org.tensorflow.ndarray.ByteNdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.ByteDataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;

public final class ByteTensor extends Tensor <Byte> {

	public ByteTensor(Shape shape) {
		this(NdArrays.ofBytes(shape.toNdArrayShape()));
	}

	public ByteTensor(byte[] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public ByteTensor(byte[][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public ByteTensor(byte[][][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	ByteTensor(ByteNdArray data) {
		super(data, DataType.BYTE);
	}

	@Override
	void parseFromValueStrings(String[] valueStrings) {
		byte[] arr = new byte[valueStrings.length];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = Byte.parseByte(valueStrings[i]);
		}
		data.write(DataBuffers.of(arr));
	}

	@Override
	String[] getValueStrings() {
		int isize = Math.toIntExact(size());
		byte[] arr = new byte[isize];
		ByteDataBuffer buffer = DataBuffers.of(arr, false, false);
		data.read(buffer);
		String[] valueStrs = new String[isize];
		for (int i = 0; i < isize; i += 1) {
			valueStrs[i] = Byte.toString(arr[i]);
		}
		return valueStrs;
	}

	public byte getByte(long... coordinates) {
		return ((ByteNdArray) data).getByte(coordinates);
	}

	public ByteTensor setByte(byte value, long... coordinates) {
		((ByteNdArray) data).setByte(value, coordinates);
		return this;
	}

	@Override
	public ByteTensor reshape(Shape newShape) {
		Preconditions.checkArgument(newShape.size() == size(), "Shape not matched.");
		ByteDataBuffer buffer = DataBuffers.ofBytes(size());
		data.read(buffer);
		return new ByteTensor(NdArrays.wrap(buffer, newShape.toNdArrayShape()));
	}
}
