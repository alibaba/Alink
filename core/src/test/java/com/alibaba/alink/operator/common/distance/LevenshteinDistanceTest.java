package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LevenshteinDistanceTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private String s1 = "GAME";
	private String s2 = "FAME";
	private String s3 = "ACM";

	private String array1 = "G A M E";
	private String array2 = "F A M E";
	private String array3 = "A C M";

	@Test
	public void testDistance() {
		LevenshteinDistance distance = new LevenshteinDistance();
		Assert.assertEquals(distance.calc(s1, s2), 1.0, 0.001);
		Assert.assertEquals(distance.calc(s1, s3), 3.0, 0.001);
		Assert.assertEquals(distance.calc(s2, s3), 3.0, 0.001);
		Assert.assertEquals(distance.calc(s3, s2), 3.0, 0.001);
		Assert.assertEquals(distance.calc("", s2), 4.0, 0.1);
		Assert.assertEquals(distance.calc(s2, ""), 4.0, 0.1);
		Assert.assertEquals(distance.calc(array1.split(" "), array2.split(" ")), 1.0, 0.001);
		Assert.assertEquals(distance.calc(array1.split(" "), array3.split(" ")), 3.0, 0.001);
		Assert.assertEquals(distance.calc(array2.split(" "), array3.split(" ")), 3.0, 0.001);
		Assert.assertEquals(distance.calc(array3.split(" "), array2.split(" ")), 3.0, 0.001);
		Assert.assertEquals(distance.calc(array2.split(" "), new String[] {}), 4.0, 0.1);
	}

	@Test
	public void testFastCompareDistance() {
		LevenshteinDistance distance = new LevenshteinDistance();
		Sample sample1 = distance.prepareSample(s1, false);
		Sample sample2 = distance.prepareSample(s2, false);
		Sample sample3 = distance.prepareSample(s3, false);
		Assert.assertEquals(distance.calc(sample2, sample1, false), 1.0, 0.001);
		Assert.assertEquals(distance.calc(sample3, sample1, false), 3.0, 0.001);
		Assert.assertEquals(distance.calc(sample3, sample2, false), 3.0, 0.001);

		Sample t1 = distance.prepareSample(array1, true);
		Sample t2 = distance.prepareSample(array2, true);
		Sample t3 = distance.prepareSample(array3, true);
		Assert.assertEquals(distance.calc(t2, t1, true), 1.0, 0.001);
		Assert.assertEquals(distance.calc(t3, t1, true), 3.0, 0.001);
		Assert.assertEquals(distance.calc(t3, t2, true), 3.0, 0.001);
	}
}