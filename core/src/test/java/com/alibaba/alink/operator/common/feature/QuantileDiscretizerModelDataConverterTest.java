package com.alibaba.alink.operator.common.feature;

import org.junit.Test;

/**
 * Unit test for QuantileDiscretizerModelDataConverter.
 */
public class QuantileDiscretizerModelDataConverterTest {

	@Test
	public void testAssembledVector() throws Exception {
		QuantileDiscretizerModelDataConverter quantileModel = new QuantileDiscretizerModelDataConverter();

		quantileModel.load(QuantileDiscretizerModelMapperTest.model);

		System.out.println(quantileModel.getFeatureValue("col2", 0));
		System.out.println(quantileModel.getFeatureSize("col2"));
		System.out.println(quantileModel.missingIndex("col2"));
	}
}