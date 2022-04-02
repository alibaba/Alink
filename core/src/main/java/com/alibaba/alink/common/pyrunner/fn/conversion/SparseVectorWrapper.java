package com.alibaba.alink.common.pyrunner.fn.conversion;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.linalg.SparseVector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;

public class SparseVectorWrapper implements VectorWrapper<SparseVector> {

	private SparseVector vector;
	private byte[] indicesBytes;
	private byte[] valuesBytes;
	private int size;

	private SparseVectorWrapper() {
	}

	public static SparseVectorWrapper fromJava(SparseVector vector) {
		SparseVectorWrapper wrapper = new SparseVectorWrapper();
		wrapper.vector = vector;
		wrapper.size = vector.size();

		// If the size of vector is un-determined, infer the size from indices.
		if (wrapper.size < 0) {
			int[] indices = vector.getIndices();
			wrapper.size = indices[indices.length - 1] + 1;
		}

		Tuple2 <byte[], byte[]> bytesTuple2 = sparseVectorToBytes(vector);
		wrapper.indicesBytes = bytesTuple2.f0;
		wrapper.valuesBytes = bytesTuple2.f1;
		return wrapper;
	}

	public static SparseVectorWrapper fromPy(byte[] indicesBytes, byte[] valuesBytes, int size) {
		SparseVectorWrapper wrapper = new SparseVectorWrapper();
		wrapper.indicesBytes = indicesBytes;
		wrapper.valuesBytes = valuesBytes;
		wrapper.size = size;
		wrapper.vector = bytesToSparseVector(indicesBytes, valuesBytes, size);
		return wrapper;
	}

	private static SparseVector bytesToSparseVector(byte[] indicesBytes, byte[] valuesBytes, int size) {
		IntBuffer indicesBuffer = ByteBuffer.wrap(indicesBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		DoubleBuffer valuesBuffer = ByteBuffer.wrap(valuesBytes).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
		int[] indices = new int[indicesBuffer.limit()];
		indicesBuffer.get(indices);
		double[] values = new double[valuesBuffer.limit()];
		valuesBuffer.get(values);
		return new SparseVector(size, indices, values);
	}

	private static Tuple2 <byte[], byte[]> sparseVectorToBytes(SparseVector vector) {
		int[] indices = vector.getIndices();
		byte[] indicesBytes = new byte[indices.length * Integer.BYTES];
		IntBuffer indicesBuffer = ByteBuffer.wrap(indicesBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
		indicesBuffer.put(indices);

		double[] values = vector.getValues();
		byte[] valuesBytes = new byte[values.length * Double.BYTES];
		DoubleBuffer valuesBuffer = ByteBuffer.wrap(valuesBytes).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
		valuesBuffer.put(values);

		return Tuple2.of(indicesBytes, valuesBytes);
	}

	@Override
	public SparseVector getJavaObject() {
		return vector;
	}

	public byte[] getIndicesBytes() {
		return indicesBytes;
	}

	public int getSize() {
		return size;
	}

	public byte[] getValuesBytes() {
		return valuesBytes;
	}
}
