package com.alibaba.alink.common.linalg.tensor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

public class TensorKryoSerializer extends Serializer <Tensor <?>> implements Serializable {

	final static byte NULL_TYPE_BIT = 0x1;
	final static byte NULL_DATA_BIT = 0x2;

	@Override
	public void write(Kryo kryo, Output output, Tensor <?> object) {
		byte nullMask = 0;
		if (null == object.getType()) {
			nullMask |= NULL_TYPE_BIT;
		}
		if (null == object.getData()) {
			nullMask |= NULL_DATA_BIT;
		}
		output.writeByte(nullMask);

		if (null != object.getType()) {
			output.writeInt(object.getType().getIndex(), true);
		}
		if (null != object.getData()) {
			output.writeInt(object.data.rank(), true);
			output.writeLongs(object.data.shape().asArray(), true);
			byte[] bytes = TensorInternalUtils.tensorToBytes(object);
			output.writeInt(bytes.length, true);
			output.writeBytes(bytes);
		}
	}

	@Override
	public Tensor <?> read(Kryo kryo, Input input, Class <Tensor <?>> type) {
		byte nullMask = input.readByte();
		if ((nullMask & NULL_TYPE_BIT) > 0) {
			return kryo.newInstance(type);
		}
		int typeIndex = input.readInt(true);
		DataType dataType = DataType.fromIndex(typeIndex);
		if ((nullMask & NULL_DATA_BIT) > 0) {
			Tensor <?> tensor = kryo.newInstance(type);
			tensor.type = dataType;
			return tensor;
		}
		int rank = input.readInt(true);
		long[] shape = input.readLongs(rank, true);
		int numBytes = input.readInt(true);
		byte[] bytes = new byte[numBytes];
		input.readBytes(bytes);
		return TensorInternalUtils.bytesToTensor(bytes, dataType, org.tensorflow.ndarray.Shape.of(shape));
	}
}
