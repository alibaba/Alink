package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import org.tensorflow.ndarray.ByteNdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.ByteDataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;

public final class UByteTensor extends Tensor <Byte> {

	public UByteTensor(Shape shape) {
		this(NdArrays.ofBytes(shape.toNdArrayShape()));
	}

	public UByteTensor(byte data) {
		this(NdArrays.scalarOf(data));
	}

	public UByteTensor(byte[] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public UByteTensor(byte[][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public UByteTensor(byte[][][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	UByteTensor(ByteNdArray data) {
		super(data, DataType.UBYTE);
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

	public byte getUByte(long... coordinates) {
		return ((ByteNdArray) data).getByte(coordinates);
	}

	public UByteTensor setUByte(byte value, long... coordinates) {
		((ByteNdArray) data).setByte(value, coordinates);
		return this;
	}

	@Override
	public UByteTensor reshape(Shape newShape) {
		AkPreconditions.checkArgument(newShape.size() == size(), "Shape not matched.");
		ByteDataBuffer buffer = DataBuffers.ofBytes(size());
		data.read(buffer);
		return new UByteTensor(NdArrays.wrap(buffer, newShape.toNdArrayShape()));
	}
}
