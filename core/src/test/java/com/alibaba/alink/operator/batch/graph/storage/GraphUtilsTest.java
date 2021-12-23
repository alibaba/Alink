package com.alibaba.alink.operator.batch.graph.storage;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.*;

public class GraphUtilsTest extends AlinkTestBase {

	@Test
	public void testPartialSum() {
		double[] psum = new double[100];
		psum[54] = 1;
		psum[55] = 2;
		psum[56] = 3;
		psum[57] = 4;
		GraphUtils.buildPartialSum(54, 58, psum);

		assertEquals(psum[54], 0.1, 1e-9);
		assertEquals(psum[55], 0.3, 1e-9);
		assertEquals(psum[56], 0.6, 1e-9);
		assertEquals(psum[57], 1.0, 1e-9);
	}

	@Test
	public void testAliasTable() {
		int[] alias = new int[100];
		double[] prob = new double[100];
		prob[54] = 1;
		prob[55] = 2;
		prob[56] = 3;
		prob[57] = 4;
		GraphUtils.buildAliasTable(54, 58, alias, prob);

		assertEquals(alias[54], 3, 1e-9);
		assertEquals(alias[55], 3, 1e-9);
		assertEquals(alias[56], 0, 1e-9);
		assertEquals(alias[57], 2, 1e-9);
		assertEquals(prob[54], 0.4, 1e-9);
		assertEquals(prob[55], 0.8, 1e-9);
		assertEquals(prob[56], 1.0, 1e-9);
		assertEquals(prob[57], 0.8, 1e-9);
	}

}