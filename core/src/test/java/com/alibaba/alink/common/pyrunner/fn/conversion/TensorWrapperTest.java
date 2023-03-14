package com.alibaba.alink.common.pyrunner.fn.conversion;

import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.StdArrays;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

@RunWith(Parameterized.class)
public class TensorWrapperTest extends AlinkTestBase {

	private final Object arr;
	private final Tensor <?> tensor;
	private final String b64str;
	private final Function <NdArray <?>, Object> ndarray2arr;
	private final String dtypeStr;

	public TensorWrapperTest(Object arr, Tensor <?> tensor, String b64str,
							 Function <NdArray <?>, Object> ndarray2arr, String dtypeStr) {
		this.arr = arr;
		this.tensor = tensor;
		this.b64str = b64str;
		this.ndarray2arr = ndarray2arr;
		this.dtypeStr = dtypeStr;
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
			"YLQguzhR2dR6y5M9vnA5m/bJLaM68B1Pt3DpjAMl9B0+uviYbacSyCvNTVVL8LVAI8KbYk3p75wvkx78WA+a+wgbEuEHsegF8rT18PHQDC0PYmNGcJIcUFhn/yD2qDNe",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((IntNdArray) d), "<i4"});
		return items;
	}

	@Test
	public void testFromJava() {
		TensorWrapper tensorWrapper = TensorWrapper.fromJava(tensor);
		Assert.assertEquals(b64str, Base64.encodeBase64String(tensorWrapper.getBytes()));
		Assert.assertEquals(dtypeStr, tensorWrapper.getDtypeStr());
		Assert.assertArrayEquals(tensor.shape(), tensorWrapper.getShape());
	}

	@Test
	public void testFromPy() {
		TensorWrapper tensorWrapper = TensorWrapper.fromPy(Base64.decodeBase64(b64str), dtypeStr, tensor.shape());
		Tensor <?> resultTensor = tensorWrapper.getJavaObject();
		Assert.assertArrayEquals(tensor.shape(), resultTensor.shape());
		Object actualArr = ndarray2arr.apply(TensorInternalUtils.getTensorData(resultTensor));
		Assert.assertArrayEquals((Object[]) arr, (Object[]) actualArr);
	}
}
