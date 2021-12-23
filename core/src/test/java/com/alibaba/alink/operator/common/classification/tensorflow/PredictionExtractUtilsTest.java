package com.alibaba.alink.operator.common.classification.tensorflow;

import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PredictionExtractUtilsTest {
	@Test
	public void testSingleLogits() {
		Map <Object, Double> predDetail = new HashMap <>();
		Object ret = PredictionExtractUtils.extractFromTensor(
			new FloatTensor(new float[] {2f}),
			Arrays.asList("neg", "pos"),
			predDetail, true);
		System.out.println(predDetail);
		Assert.assertEquals(ret, "pos");
		Assert.assertEquals(predDetail.get("pos"), 0.8807970779778823, 1e-6);
		Assert.assertEquals(predDetail.get("neg"), 0.11920292202211769, 1e-6);
	}

	@Test
	public void testSingleProbs() {
		Map <Object, Double> predDetail = new HashMap <>();
		Object ret = PredictionExtractUtils.extractFromTensor(
			new FloatTensor(new float[] {0.2f}),
			Arrays.asList("neg", "pos"),
			predDetail, false);
		System.out.println(predDetail);
		Assert.assertEquals(ret, "neg");
		Assert.assertEquals(predDetail.get("pos"), 0.2, 1e-6);
		Assert.assertEquals(predDetail.get("neg"), 0.8, 1e-6);
	}

	@Test
	public void testMultiLogits() {
		Map <Object, Double> predDetail = new HashMap <>();
		Object ret = PredictionExtractUtils.extractFromTensor(
			new FloatTensor(new float[] {-1.f, 1.f, 4.f, 3.f}),
			Arrays.asList("1", "2", "3", "4"),
			predDetail, true);
		System.out.println(predDetail);
		Assert.assertEquals(ret, "3");
		Assert.assertEquals(predDetail.get("1"), 0.0047303607961604694, 1e-6);
		Assert.assertEquals(predDetail.get("2"), 0.03495290129101196, 1e-6);
		Assert.assertEquals(predDetail.get("3"), 0.7020477894531546, 1e-6);
		Assert.assertEquals(predDetail.get("4"), 0.25826894845967296, 1e-6);
	}

	@Test
	public void testMultiProbs() {
		Map <Object, Double> predDetail = new HashMap <>();
		Object ret = PredictionExtractUtils.extractFromTensor(
			new FloatTensor(new float[] {0.0047303607961604694f, 0.03495290129101196f, 0.7020477894531546f, 0.25826894845967296f}),
			Arrays.asList("1", "2", "3", "4"),
			predDetail, false);
		System.out.println(predDetail);
		Assert.assertEquals(ret, "3");
		Assert.assertEquals(predDetail.get("1"), 0.0047303607961604694, 1e-6);
		Assert.assertEquals(predDetail.get("2"), 0.03495290129101196, 1e-6);
		Assert.assertEquals(predDetail.get("3"), 0.7020477894531546, 1e-6);
		Assert.assertEquals(predDetail.get("4"), 0.25826894845967296, 1e-6);
	}
}
