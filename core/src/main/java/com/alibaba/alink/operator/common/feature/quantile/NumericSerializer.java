package com.alibaba.alink.operator.common.feature.quantile;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;

public final class NumericSerializer extends TypeSerializerSingleton <Number> {
	private static final long serialVersionUID = 1L;

	/**
	 * Sharable instance of the NumericSerializer.
	 */
	public static final NumericSerializer INSTANCE = new NumericSerializer();

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Number createInstance() {
		return null;
	}

	@Override
	public Number copy(Number from) {
		return from;
	}

	@Override
	public Number copy(Number from, Number reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return -1;
	}

	private interface BiConsumerThrows {
		void write(DataOutputView target, Number record) throws IOException;
	}

	private interface FunctionThrows {
		Number read(DataInputView source) throws IOException;
	}

	@Override
	public void serialize(Number record, DataOutputView target) throws IOException {
		target.writeByte(TYPE_TO_BYTE.get(record.getClass()).f1);
		TYPE_TO_BYTE.get(record.getClass()).f0.write(target, record);
	}

	@Override
	public Number deserialize(DataInputView source) throws IOException {
		int sign = source.readByte();
		return BYTE_TO_TYPE.get(sign).read(source);
	}

	@Override
	public Number deserialize(Number reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public TypeSerializerSnapshot <Number> snapshotConfiguration() {
		return new NumericSerializerSnapshot();
	}

	@SuppressWarnings("WeakerAccess")
	public static final class NumericSerializerSnapshot extends SimpleTypeSerializerSnapshot <Number> {
		public NumericSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}

	private static final HashMap <Class <?>, Tuple2 <BiConsumerThrows, Integer>> TYPE_TO_BYTE = new HashMap <>();
	private static final HashMap <Integer, FunctionThrows> BYTE_TO_TYPE = new HashMap <>();

	static {
		TYPE_TO_BYTE.put(Long.class, Tuple2.of((x, y) -> {
			x.writeLong(y.longValue());
		}, 0));
		TYPE_TO_BYTE.put(Double.class, Tuple2.of((x, y) -> {
			x.writeDouble(y.doubleValue());
		}, 1));
		TYPE_TO_BYTE.put(Integer.class, Tuple2.of((x, y) -> {
			x.writeInt(y.intValue());
		}, 2));
		TYPE_TO_BYTE.put(Byte.class, Tuple2.of((x, y) -> {
			x.writeByte(y.byteValue());
		}, 3));
		TYPE_TO_BYTE.put(Short.class, Tuple2.of((x, y) -> {
			x.writeShort(y.shortValue());
		}, 4));
		TYPE_TO_BYTE.put(Float.class, Tuple2.of((x, y) -> {
			x.writeFloat(y.floatValue());
		}, 5));

		BYTE_TO_TYPE.put(0, DataInput::readLong);
		BYTE_TO_TYPE.put(1, DataInput::readDouble);
		BYTE_TO_TYPE.put(2, DataInput::readInt);
		BYTE_TO_TYPE.put(3, DataInput::readByte);
		BYTE_TO_TYPE.put(4, DataInput::readShort);
		BYTE_TO_TYPE.put(5, DataInput::readFloat);
	}
}
