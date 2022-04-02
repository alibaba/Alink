package com.alibaba.alink.common.pyrunner.fn.conversion;

import com.alibaba.alink.common.linalg.DenseVector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;

public class DenseVectorWrapper implements VectorWrapper <DenseVector> {

	private DenseVector vector;
	private byte[] bytes;
	private int size;

	private DenseVectorWrapper() {
	}

	public static DenseVectorWrapper fromJava(DenseVector vector) {
		DenseVectorWrapper wrapper = new DenseVectorWrapper();
		wrapper.vector = vector;
		wrapper.size = vector.size();
		wrapper.bytes = denseVectorToBytes(vector);
		return wrapper;
	}

	public static DenseVectorWrapper fromPy(byte[] bytes, int size) {
		DenseVectorWrapper wrapper = new DenseVectorWrapper();
		wrapper.bytes = bytes;
		wrapper.size = size;
		wrapper.vector = bytesToDenseVector(bytes, size);
		return wrapper;
	}

	private static DenseVector bytesToDenseVector(byte[] bytes, int size) {
		DoubleBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
		DenseVector vector = new DenseVector(size);
		buffer.get(vector.getData());
		return vector;
	}

	private static byte[] denseVectorToBytes(DenseVector vector) {
		int size = vector.size();
		byte[] bytes = new byte[size * Double.BYTES];
		DoubleBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
		buffer.put(vector.getData());
		return bytes;
	}

	@Override
	public DenseVector getJavaObject() {
		return vector;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public int getSize() {
		return size;
	}
}
