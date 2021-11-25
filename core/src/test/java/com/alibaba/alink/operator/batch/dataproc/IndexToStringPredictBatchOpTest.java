package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.StringIndexer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IndexToStringPredictBatchOpTest {
	@Test
	public void testIndexToStringPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football"),
			Row.of("football"),
			Row.of("football"),
			Row.of("basketball"),
			Row.of("basketball"),
			Row.of("tennis")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string");
		StringIndexer stringIndexer = new StringIndexer()
			.setModelName("string_indexer_model")
			.setSelectedCol("f0")
			.setOutputCol("f0_indexed")
			.setStringOrderType("frequency_asc");
		stringIndexer.fit(data).transform(data).print();
	}
}
