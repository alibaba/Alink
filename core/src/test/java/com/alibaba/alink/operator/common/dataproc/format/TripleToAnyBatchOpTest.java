package com.alibaba.alink.operator.common.dataproc.format;

import com.alibaba.alink.operator.batch.dataproc.format.TripleToAnyBatchOp;
import com.alibaba.alink.params.dataproc.format.FromTripleParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.params.dataproc.format.ToKvParams;
import com.alibaba.alink.params.dataproc.format.ToVectorParams;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TripleToAnyBatchOpTest {
	@Test
	public void linkFrom() throws Exception {
		Row[] rowData = new Row[] {
			Row.of(1, 1, 1.0),
			Row.of(1, 2, 1.0),
			Row.of(2, 3, 1.0),
			Row.of(3, 4, 1.0),
			Row.of(4, 2, 1.0),
			Row.of(3, 1, 1.0),
			Row.of(2, 4, 1.0),
			Row.of(4, 1, 1.0)
		};
		BatchOperator data = new TableSourceBatchOp(MLEnvironmentFactory.getDefault().createBatchTable(
			Arrays.asList(rowData),
			new String[] {"start", "dest", "weight"}));

		BatchOperator triple2anyRes = new TripleToAnyBatchOp(
			FormatType.KV,
			new Params()
				.set(FromTripleParams.TRIPLE_ROW_COL, "start")
				.set(FromTripleParams.TRIPLE_COL_COL, "dest")
				.set(FromTripleParams.TRIPLE_VAL_COL, "weight")
				.set(ToKvParams.KV_COL, "kv")
		).linkFrom(data);
		Assert.assertEquals(4, triple2anyRes.collect().size());

		//new TripleToAnyBatchOp(
		//	FormatType.COLUMNS,
		//	new Params()
		//		.set(TripleToVectorParams.ROW_COL, "start")
		//		.set(TripleToVectorParams.COLUMN_COL, "dest")
		//		.set(TripleToVectorParams.VAL_COL, "weight")
		//		.set(ToColumnsParams.SCHEMA_STR, "f0 double, f1 double, f2 double, f3 double, f4 double")
		//).linkFrom(data).lazyPrint(-1);

		BatchOperator triple2anyRes2 = new TripleToAnyBatchOp(
			FormatType.VECTOR,
			new Params()
				.set(FromTripleParams.TRIPLE_ROW_COL, "start")
				.set(FromTripleParams.TRIPLE_COL_COL, "dest")
				.set(FromTripleParams.TRIPLE_VAL_COL, "weight")
				.set(ToVectorParams.VECTOR_COL, "vec")
		).linkFrom(data);
		Assert.assertEquals(4, triple2anyRes2.collect().size());

	}

}