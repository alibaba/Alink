package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KSigmaOutlier4SeriesBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, 1, 10.0),
			Row.of(1, 2, 11.0),
			Row.of(1, 3, 12.0),
			Row.of(1, 4, 13.0),
			Row.of(1, 5, 14.0),
			Row.of(1, 6, 15.0),
			Row.of(1, 7, 16.0),
			Row.of(1, 8, 17.0),
			Row.of(1, 9, 18.0),
			Row.of(1, 10, 19.0)
		);

		MemSourceBatchOp dataOp = new MemSourceBatchOp(mTableData, new String[] {"group_id", "id", "val"});

		BatchOperator<?> outlierOp = dataOp.link(
			new GroupByBatchOp()
				.setGroupByPredicate("group_id")
				.setSelectClause("group_id, mtable_agg(id, val) as data")
		).link(new KSigmaOutlier4GroupedDataBatchOp()
			.setInputMTableCol("data")
			.setOutputMTableCol("pred")
			.setFeatureCol("val")
			.setPredictionCol("detect_pred")
		);

		MTable pred = (MTable)outlierOp.collect().get(0).getField(2);
		Assert.assertEquals(0, pred.summary().sum("detect_pred"), 10e-10);

	}
}