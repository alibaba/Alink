package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.similarity.Sample;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for LongestCommonSubsequenceSimilarity
 */
public class LongestCommonSubsequenceSimilarityTest {
	private String s1 = "Good Morning!";
	private String s2 = "Good Evening!";
	private LongestCommonSubsequenceSimilarity similarity = new LongestCommonSubsequenceSimilarity();

	@Test
	public void calsimilarity() {
		Assert.assertEquals(similarity.calc(s1, s2), 0.76, 0.01);
		Assert.assertEquals(similarity.calc(s1.split(" "), s2.split(" ")), 0.5, 0.01);
		Assert.assertEquals(similarity.calc("", s1), 0.0, 0.01);
		Assert.assertEquals(similarity.calc("", ""), 0.0, 0.01);
		Assert.assertEquals(similarity.calc(s1, ""), 0.0, 0.01);
		Assert.assertEquals(similarity.calc(s1.split(" "), "".split(" ")), 0.0, 0.01);
		Assert.assertEquals(similarity.calc("".split(" "), s1.split(" ")), 0.0, 0.01);
		Assert.assertEquals(similarity.calc(new String[] {}, new String[] {}), 0.0, 0.01);
	}

	@Test
	public void calc() {
		Sample sample1 = new Sample(s1, null);
		Sample sample2 = new Sample(s2, null);
		similarity.updateLabel(sample1, s1);
		similarity.updateLabel(sample2, s2);
		Assert.assertEquals(similarity.calc(sample1, sample2, false), 0.76, 0.01);
		similarity.updateLabel(sample1, s1.split(" "));
		similarity.updateLabel(sample2, s2.split(" "));
		Assert.assertEquals(similarity.calc(sample1, sample2, true), 0.5, 0.01);

		sample1 = new Sample("", null);
		similarity.updateLabel(sample1, "");
		Assert.assertEquals(similarity.calc(sample1, sample2, false), 0.0, 0.01);

		similarity.updateLabel(sample1, new String[] {});
		Assert.assertEquals(similarity.calc(sample1, sample2, true), 0.0, 0.01);

	}

}