package com.alibaba.alink.common.pyrunner.fn.conversion;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

public class SparseVectorWrapperTest extends AlinkTestBase {

	private static final double EPS = 1e-6;

	@Test
	public void testFromJava() {
		int size = 20000;
		int[] indices = new int[] {1, 10, 100, 1000, 10000};
		double[] values = new double[] {1, 2, 3, 4, 5};
		SparseVector vector = new SparseVector(size, indices, values);

		String indicesB64str = "AQAAAAoAAABkAAAA6AMAABAnAAA=";
		String valuesB64str = "AAAAAAAA8D8AAAAAAAAAQAAAAAAAAAhAAAAAAAAAEEAAAAAAAAAUQA==";

		SparseVectorWrapper sparseVectorWrapper = SparseVectorWrapper.fromJava(vector);
		Assert.assertEquals(indicesB64str, Base64.encodeBase64String(sparseVectorWrapper.getIndicesBytes()));
		Assert.assertEquals(valuesB64str, Base64.encodeBase64String(sparseVectorWrapper.getValuesBytes()));
		Assert.assertEquals(vector.size(), sparseVectorWrapper.getSize());
	}

	@Test
	public void testFromPy() {
		int size = 20000;
		String indicesB64str = "AQAAAAoAAABkAAAA6AMAABAnAAA=";
		String valuesB64str = "AAAAAAAA8D8AAAAAAAAAQAAAAAAAAAhAAAAAAAAAEEAAAAAAAAAUQA==";

		SparseVectorWrapper sparseVectorWrapper =
			SparseVectorWrapper.fromPy(Base64.decodeBase64(indicesB64str), Base64.decodeBase64(valuesB64str), size);
		SparseVector resultVector = sparseVectorWrapper.getJavaObject();
		Assert.assertArrayEquals(new int[] {1, 10, 100, 1000, 10000}, resultVector.getIndices());
		Assert.assertArrayEquals(new double[] {1, 2, 3, 4, 5}, resultVector.getValues(), EPS);
		Assert.assertEquals(size, resultVector.size());
	}
}
