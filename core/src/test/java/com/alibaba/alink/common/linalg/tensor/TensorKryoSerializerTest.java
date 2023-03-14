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

		IntTensor t = kryo.newInstance(IntTensor.class);
		items.add(new Object[] {null, t,
			"AQBjb20uYWxpYmFiYS5hbGluay5jb21tb24ubGluYWxnLnRlbnNvci5JbnRUZW5zb/IBAw==",
			(Function <NdArray <?>, Object>) d -> null});

		IntTensor t2 = kryo.newInstance(IntTensor.class);
		t2.type = DataType.INT;
		items.add(new Object[] {null, t2,
			"AQBjb20uYWxpYmFiYS5hbGluay5jb21tb24ubGluYWxnLnRlbnNvci5JbnRUZW5zb/IBAgM=",
			(Function <NdArray <?>, Object>) d -> null});

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
}
