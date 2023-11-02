package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.testutil.AlinkTestBase;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.StdArrays;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

@RunWith(Parameterized.class)
public class TensorKryoSerializerTest extends AlinkTestBase {

	private static final Kryo kryo;
	private final Object arr;
	private final Tensor <?> tensor;
	private final String b64str;
	private final Function <NdArray <?>, Object> ndarray2arr;

	public TensorKryoSerializerTest(Object arr, Tensor <?> tensor, String b64str,
									Function <NdArray <?>, Object> ndarray2arr) {
		this.arr = arr;
		this.tensor = tensor;
		this.b64str = b64str;
		this.ndarray2arr = ndarray2arr;
	}

	static {
		kryo = new Kryo();
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.addDefaultSerializer(Tensor.class, TensorKryoSerializer.class);
	}

	@Parameters
	public static Collection <Object[]> data() {
		List <Object[]> items = new ArrayList <>();
		Random random = new Random(0);
		int n = 2;
		int m = 3;
		int l = 4;
		int[][][] intArr = new int[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					intArr[i][j][k] = random.nextInt();
				}
			}
		}
		items.add(new Object[] {intArr, new IntTensor(intArr),
			"AQBjb20uYWxpYmFiYS5hbGluay5jb21tb24ubGluYWxnLnRlbnNvci5JbnRUZW5zb/IBAAMDAgMEYGC0ILs4UdnUesuTPb5wOZv2yS2jOvAdT7dw6YwDJfQdPrr4mG2nEsgrzU1VS/C1QCPCm2JN6e+cL5Me/FgPmvsIGxLhB7HoBfK09fDx0AwtD2JjRnCSHFBYZ/8g9qgzXg==",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((IntNdArray) d)});

		return items;
	}

	@Test
	public void testWrite() {
		Output output = new Output(1, Integer.MAX_VALUE);
		kryo.writeClassAndObject(output, tensor);
		byte[] buffer = output.toBytes();
		String s = Base64.getEncoder().encodeToString(buffer);
		Assert.assertEquals(b64str, s);
	}

	@Test
	public void testRead() {
		byte[] buffer = Base64.getDecoder().decode(b64str);
		Input input = new Input(buffer);
		Tensor <?> resultTensor = (Tensor <?>) kryo.readClassAndObject(input);
		Assert.assertEquals(tensor.getType(), resultTensor.getType());
		if (null != tensor.data && null != resultTensor.data) {
			Assert.assertArrayEquals(tensor.shape(), resultTensor.shape());
			Object actualArr = ndarray2arr.apply(TensorInternalUtils.getTensorData(resultTensor));
			Assert.assertArrayEquals((Object[]) arr, (Object[]) actualArr);
		} else {
			Assert.assertNull(tensor.data);
			Assert.assertNull(resultTensor.data);
		}
	}

	@Test
	public void testDefaultSer() {
		Kryo kryo = new Kryo();
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		int times = 100;
		long total = 0;
		int bufferSize = 1024;
		for (int i = 0; i < times; i += 1) {
			long startTime = System.nanoTime();
			Output output = new Output(bufferSize, Integer.MAX_VALUE);
			kryo.writeClassAndObject(output, tensor);
			byte[] bytes = output.toBytes();
			long endTime = System.nanoTime();
			total += endTime - startTime;
		}
		System.out.println(1. * total / times / 1e9);
		// Kryo default deserializer does not work, because Tensor has no no-arg contructors.
	}

	@Test
	public void testTensorSer() {
		Kryo kryo = new Kryo();
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.addDefaultSerializer(Tensor.class, TensorKryoSerializer.class);
		int times = 100;
		long total = 0;
		int bufferSize = 1024;
		for (int i = 0; i < times; i += 1) {
			long startTime = System.nanoTime();
			Output output = new Output(bufferSize, Integer.MAX_VALUE);
			kryo.writeClassAndObject(output, tensor);
			byte[] bytes = output.toBytes();
			//System.out.println(bytes.length);
			long endTime = System.nanoTime();
			total += endTime - startTime;
		}
		System.out.println(1. * total / times / 1e9);
	}

	@Test
	public void testJsonSer() {
		int times = 100;
		long total = 0;
		for (int i = 0; i < times; i += 1) {
			long startTime = System.nanoTime();
			String s = TensorUtil.toString(tensor);
			long endTime = System.nanoTime();
			total += endTime - startTime;
		}
		System.out.println(1. * total / times / 1e9);
	}

	@Test
	public void testDefaultDe() {
		Kryo kryo = new Kryo();
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		int bufferSize = 1024;
		Output output = new Output(bufferSize, Integer.MAX_VALUE);
		kryo.writeClassAndObject(output, tensor);
		byte[] bytes = output.toBytes();

		int times = 100;
		long total = 0;
		for (int i = 0; i < times; i += 1) {
			long startTime = System.nanoTime();
			Input input = new Input(bytes);
			Tensor <?> resultTensor = (Tensor <?>) kryo.readClassAndObject(input);
			long endTime = System.nanoTime();
			total += endTime - startTime;
			Assert.assertEquals(tensor.getType(), resultTensor.getType());
			Assert.assertArrayEquals(tensor.shape(), resultTensor.shape());
		}
		System.out.println(1. * total / times / 1e9);
	}

	@Test
	public void testTensorDe() {
		Kryo kryo = new Kryo();
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.addDefaultSerializer(Tensor.class, TensorKryoSerializer.class);
		int bufferSize = 1024;
		Output output = new Output(bufferSize, Integer.MAX_VALUE);
		kryo.writeClassAndObject(output, tensor);
		byte[] bytes = output.toBytes();

		int times = 100;
		long total = 0;
		for (int i = 0; i < times; i += 1) {
			long startTime = System.nanoTime();
			Input input = new Input(bytes);
			Tensor <?> resultTensor = (Tensor <?>) kryo.readClassAndObject(input);
			long endTime = System.nanoTime();
			total += endTime - startTime;
			Assert.assertEquals(tensor.getType(), resultTensor.getType());
			Assert.assertArrayEquals(tensor.shape(), resultTensor.shape());
		}
		System.out.println(1. * total / times / 1e9);
	}

	@Test
	public void testJsonDe() {
		String s = TensorUtil.toString(tensor);
		int times = 100;
		long total = 0;
		for (int i = 0; i < times; i += 1) {
			long startTime = System.nanoTime();
			Tensor <?> resultTensor = TensorUtil.parseTensor(s);
			long endTime = System.nanoTime();
			total += endTime - startTime;
			Assert.assertEquals(tensor.getType(), resultTensor.getType());
			Assert.assertArrayEquals(tensor.shape(), resultTensor.shape());
		}
		System.out.println(1. * total / times / 1e9);
	}
}
