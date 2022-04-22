package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.params.outlier.HasInputMTableCol;
import com.alibaba.alink.params.outlier.HasOutputMTableCol;
import com.alibaba.alink.params.outlier.IForestDetectorParams;
import org.junit.Assert;
import org.junit.Test;

public class IForestDetectorTest {

	@Test
	public void testMTableImmutable() throws Exception {
		MTable input = new MTable(
			new Row[]{
				Row.of(0.1f),
				Row.of(0.2f)
			},
			"f0 float"
		);

		IForestDetector iForestDetector = new IForestDetector(
			input.getSchema(),
			new Params()
				.set(HasInputMTableCol.INPUT_MTABLE_COL, "f0")
				.set(HasOutputMTableCol.OUTPUT_MTABLE_COL, "f0")
				.set(IForestDetectorParams.FEATURE_COLS, new String[]{"f0"})
		);

		iForestDetector.detect(input, false);

		Assert.assertEquals(input.getColTypes()[0], Types.FLOAT);
		Assert.assertTrue(input.getRow(0).getField(0) instanceof Float);
		Assert.assertTrue(input.getRow(1).getField(0) instanceof Float);
	}
}