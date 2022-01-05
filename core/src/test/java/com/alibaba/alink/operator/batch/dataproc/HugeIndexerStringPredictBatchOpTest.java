package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeIndexerStringPredictBatchOpTest extends AlinkTestBase {
	@Test
	public void testStringIndexerPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("basketball", "apple"),
			Row.of("basketball", "apple"),
			Row.of("tennis", "pair"),
			Row.of("tennis", "pair"),
			Row.of("pingpang", "banana"),
			Row.of("pingpang", "banana"),
			Row.of("baseball", "banana")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");

		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setSelectedCols("f1")
			.setStringOrderType("alphabet_asc");
		BatchOperator <?> predictor = new HugeStringIndexerPredictBatchOp().setSelectedCols("f0", "f1")
			.setOutputCols("f0_indexed", "f1_indexed");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		BatchOperator result = predictor.linkFrom(model, data);
		result.lazyPrint(10);

		BatchOperator <?> indexerString = new HugeIndexerStringPredictBatchOp().setSelectedCols("f0_indexed", "f1_indexed")
			.setOutputCols("f0_source", "f1_source");
		indexerString.linkFrom(model, result).print();

		List <Row> idDf = Arrays.asList(
			Row.of(1L, 2L, new Long[]{3L, 4L}),
			Row.of(0L, 5L, new Long[]{6L, 7L}),
			Row.of(2L, 4L, new Long[]{0L, 5L, 1L}),
			Row.of(5L, null, null)
		);

		BatchOperator <?> idData = new MemSourceBatchOp(idDf, new String[] {"f0_indexed", "f1_indexed", "f2_indexed"});
		BatchOperator <?> indexerString2 = new HugeIndexerStringPredictBatchOp().setSelectedCols("f0_indexed", "f1_indexed", "f2_indexed")
			.setOutputCols("f0_source", "f1_source", "f2_source");
		indexerString2.linkFrom(model, idData).print();
	}
}
