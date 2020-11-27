package com.alibaba.alink.operator.common.tree;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PreprocessingTest {
	@Test
	public void dense() {
		Row[] rows = new Row[] {
			Row.of(0L, "{\"selectedCols\":\"[\\\"col0\\\",\\\"col1\\\"]\",\"MLEnvironmentId\":\"0\","
				+ "\"version\":\"\\\"v2\\\"\",\"numBuckets\":\"128\"}\n"),
			Row.of(1048576L, "[{\"featureName\":\"col0\",\"featureType\":\"INT\",\"splitsArray\":[0,1,4,5],"
				+ "\"isLeftOpen\":true}]\n"),
			Row.of(2097152L, "[{\"featureName\":\"col1\",\"featureType\":\"INT\",\"splitsArray\":[2,3,4],"
				+ "\"isLeftOpen\":true}]\n")
		};
		List <Row> model = Arrays.asList(rows);
		QuantileDiscretizerModelDataConverter quantileModel
			= new QuantileDiscretizerModelDataConverter();

		quantileModel.load(model);

		Assert.assertEquals(quantileModel.getFeatureSize("col0"), 5);
		Assert.assertEquals(quantileModel.missingIndex("col0"), 5);
		Assert.assertEquals(quantileModel.getFeatureSize("col1"), 4);
		Assert.assertEquals(quantileModel.getFeatureSize("col1"), 4);
	}

	@Test
	public void sparse() {
		Row[] rows = new Row[] {
			Row.of(0L, "{\"vectorCol\":\"\\\"vector\\\"\",\"MLEnvironmentId\":\"0\",\"version\":\"\\\"v2\\\"\","
				+ "\"numBuckets\":\"128\"}\n"),
			Row.of(1048576L, "[{\"featureName\":\"0\",\"featureType\":\"DOUBLE\",\"splitsArray\":[0.0,1.0,4.0,5.0],"
				+ "\"isLeftOpen\":true}]\n"),
			Row.of(2097152L, "[{\"featureName\":\"1\",\"featureType\":\"DOUBLE\",\"splitsArray\":[2.0,3.0,4.0],"
				+ "\"isLeftOpen\":true}]\n")
		};
		List <Row> model = Arrays.asList(rows);
		QuantileDiscretizerModelDataConverter quantileModel
			= new QuantileDiscretizerModelDataConverter();

		quantileModel.load(model);

		Assert.assertEquals(quantileModel.getFeatureSize("0"), 5);
		Assert.assertEquals(quantileModel.missingIndex("0"), 5);
		Assert.assertEquals(quantileModel.getFeatureSize("1"), 4);
		Assert.assertEquals(quantileModel.getFeatureSize("1"), 4);

		Assert.assertEquals(Preprocessing.zeroIndex(quantileModel, "0"), 0);
		Assert.assertEquals(Preprocessing.zeroIndex(quantileModel, "1"), 0);
	}

}