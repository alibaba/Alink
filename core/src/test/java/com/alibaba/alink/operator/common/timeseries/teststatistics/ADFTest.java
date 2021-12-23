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



}