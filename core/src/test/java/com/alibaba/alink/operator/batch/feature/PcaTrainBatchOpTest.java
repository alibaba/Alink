package com.alibaba.alink.operator.batch.feature;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PcaTrainBatchOpTest {
	@Test
	public void testDotProductionCut() {
		int nAll = 5;

		List <Integer> nonEqualColIdx = new ArrayList <>();
		nonEqualColIdx.add(1);
		nonEqualColIdx.add(2);
		nonEqualColIdx.add(4);

		double[] production = new double[] {1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 6.0, 7.0, 8.0, 1.0, 9.0, 10.0, 1.0, 11.0,
			1.0};

		double[] productionCut = PcaTrainBatchOp.VecCombine.dotProdctionCut(production, nonEqualColIdx, nAll);

		Assert.assertArrayEquals(new double[] {1.0, 6.0, 8.0, 1.0, 10.0, 1.0}, productionCut, 10e-10);
	}

}