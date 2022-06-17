package com.alibaba.alink.operator.common.timeseries.teststatistics;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class ADFTest extends AlinkTestBase {

	@Test
	public void testADF() {
		ADF adf = new ADF();
		adf.adfTest(new double[] {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}, 2, 1);
		adf.adfTest(new double[] {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}, 2, 2);
	}

	@Test
	public void testKPSS() {
		KPSS adf = new KPSS();
		adf.autoBandwidth(new double[] {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}, 2, 1);
		adf.autoBandwidth(new double[] {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}, 2, 2);
	}

	@Test
	public void test() {
		double[] data = new double[]{151.0, 188.46, 199.38, 219.75, 241.55, 262.58,
			328.22, 396.26, 442.04, 517.77, 626.52, 717.08, 824.38, 913.38, 1088.39,
			1325.83, 1700.92, 2109.38, 2499.77, 2856.47, 3114.02, 3229.29,
			3545.39, 3880.53, 4212.82, 4757.45, 5633.24, 6590.19, 7617.47,
			9333.4, 11328.92, 12961.1, 15967.61};
		StationaryTest st = new StationaryTest();
		double[][] lb = st.ljungBox(data,0.95,20);
		if (lb[0][0] < lb [1][0]) {
			System.out.println("lb===" + false);
		} else {
			System.out.println("lb===" + true);
		}

	}


}