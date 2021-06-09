package com.alibaba.alink.operator.common.feature.pca;

import com.alibaba.alink.params.feature.HasCalculationType;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class PcaModelDataTest extends AlinkTestBase {
	@Test
	public void test() {
		PcaModelData modelData = new PcaModelData();
		modelData.featureColNames = new String[] {"f0", "f1"};
		modelData.nx = 2;
		modelData.coef = new double[][] {{1, 2}, {3, 4}};
		modelData.lambda = new double[] {1, 2};
		modelData.p = 1;
		modelData.means = new double[] {1, 2};
		modelData.stddevs = new double[] {1, 2};
		modelData.idxNonEqual = new Integer[] {0, 1};
		modelData.vectorColName = "vec";
		modelData.pcaType = HasCalculationType.CalculationType.CORR;
		modelData.nameX = modelData.featureColNames;

		modelData.toString();

		modelData.calcPrinValue(new double[] {1, 2});
	}

}