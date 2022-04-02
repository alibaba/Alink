package com.alibaba.alink.operator.common.pytorch;

import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorKryoSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.util.Arrays;
import java.util.List;

public class ListSerializer {

	private final Kryo kryo = new Kryo();

	public ListSerializer() {
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.addDefaultSerializer(Tensor.class, TensorKryoSerializer.class);
	}

	public byte[] serialize(List <?> values) {
		Object[] objects = values.toArray(new Object[0]);
		Output output = new Output(1, Integer.MAX_VALUE);
		kryo.writeClassAndObject(output, objects);
		return output.toBytes();
	}

	public List <?> deserialize(byte[] bytes) {
		Input input = new Input(bytes);
		Object[] objects = (Object[]) kryo.readClassAndObject(input);
		return Arrays.asList(objects);
	}
}
