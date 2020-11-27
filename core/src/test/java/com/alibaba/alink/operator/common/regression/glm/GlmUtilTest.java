package com.alibaba.alink.operator.common.regression.glm;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GlmUtilTest {

	@Test
	public void test() {
		String[] colNames = new String[] {"u", "lot1", "lot2", "offset", "weights"};

		Object[][] dataRows = new Object[][] {
			{Math.log(10), 58.0, 35.0, 10.0, 2.0}
		};

		BatchOperator data = new MemSourceBatchOp(dataRows, colNames);

		try {
			GlmUtil.preProc(data, null,
				null, null, null);
		} catch (Exception ex) {
			assertEquals("featureColNames must be set.", ex.getMessage());
		}

		try {
			GlmUtil.preProc(data, colNames,
				null, null, null);
		} catch (Exception ex) {
			assertEquals("labelColName must be set.", ex.getMessage());
		}
	}

	@Test
	public void testTrain() {
		String[] colNames = new String[] {"u", "lot1", "lot2", "offset", "weights"};

		Object[][] dataRows = new Object[][] {
			{Math.log(10), 58.0, 35.0, 10.0, 2.0}
		};

		BatchOperator data = new MemSourceBatchOp(dataRows, colNames);

		try {
			GlmUtil.preProc(data, null,
				null, null, null);
		} catch (Exception ex) {
			assertEquals("featureColNames must be set.", ex.getMessage());
		}

		try {
			GlmUtil.preProc(data, colNames,
				null, null, null);
		} catch (Exception ex) {
			assertEquals("labelColName must be set.", ex.getMessage());
		}
	}

}