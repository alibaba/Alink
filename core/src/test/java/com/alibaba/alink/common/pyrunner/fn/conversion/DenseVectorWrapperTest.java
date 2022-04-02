package com.alibaba.alink.common.pyrunner.fn.conversion;

import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

public class DenseVectorWrapperTest {
	private static final double EPS = 1e-6;

	@Test
	public void testFromJava() {
		DenseVector vector = new DenseVector(new double[] {1, 2, 3, 4, 5});
		String b64str = "AAAAAAAA8D8AAAAAAAAAQAAAAAAAAAhAAAAAAAAAEEAAAAAAAAAUQA==";

		DenseVectorWrapper denseVectorWrapper = DenseVectorWrapper.fromJava(vector);
		Assert.assertEquals(b64str, Base64.encodeBase64String(denseVectorWrapper.getBytes()));
		Assert.assertEquals(vector.size(), denseVectorWrapper.getSize());
	}

	@Test
	public void testFromPy() {
		DenseVector vector = new DenseVector(new double[] {1, 2, 3, 4, 5});
		String b64str = "AAAAAAAA8D8AAAAAAAAAQAAAAAAAAAhAAAAAAAAAEEAAAAAAAAAUQA==";

		DenseVectorWrapper denseVectorWrapper = DenseVectorWrapper.fromPy(Base64.decodeBase64(b64str), 5);
		DenseVector resultVector = denseVectorWrapper.getJavaObject();
		Assert.assertArrayEquals(resultVector.getData(), vector.getData(), EPS);
	}
}
