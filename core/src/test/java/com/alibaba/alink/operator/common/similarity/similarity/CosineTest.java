package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.similarity.Sample;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit test for cosine.
 */
public class CosineTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private String s1 = "Good Morning!";
	private String s2 = "Good Evening!";
	private Cosine cosine = new Cosine(3);

	@Test
	public void calCosine() {
		Assert.assertEquals(cosine.calc(s1, s2), 0.54, 0.01);
		Assert.assertEquals(cosine.calc(s1.split(" "), s2.split(" ")), 0.0, 0.01);
		thrown.expect(RuntimeException.class);
		cosine = new Cosine(-1);
	}

	@Test
	public void calc() {
		Sample sample1 = new Sample(s1, null);
		Sample sample2 = new Sample(s2, null);
		cosine.updateLabel(sample1, s1);
		cosine.updateLabel(sample2, s2);
		Assert.assertEquals(cosine.calc(sample1, sample2, false), 0.54, 0.01);
		cosine.updateLabel(sample1, s1.split(" "));
		cosine.updateLabel(sample2, s2.split(" "));
		Assert.assertEquals(cosine.calc(sample1, sample2, true), 0.0, 0.01);
	}
}