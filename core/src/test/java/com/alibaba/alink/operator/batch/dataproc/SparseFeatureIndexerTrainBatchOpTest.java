package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SparseFeatureIndexerTrainBatchOpTest extends AlinkTestBase {
	@Test
	public void testSparseFeatureIndexer() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("fea1:1,fea2:2,fea3:3"),
			Row.of("fea2:1,fea3:4,fea4:1"),
			Row.of("fea1:1,fea2:1,fea3:1,fea4:1,fea4:2"),
			Row.of("fea1:1,fea3:1,fea5:1")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "feature string");
		BatchOperator indexer = new SparseFeatureIndexerTrainBatchOp()
			.setSelectedCol("feature")
			.setTopN(4)
			.setHasValue(true)
			.linkFrom(data);
		//op.getSideOutput(0).print();
		indexer.getSideOutput(0).lazyPrint();
		indexer.lazyPrint();
		SparseFeatureIndexerPredictBatchOp predict = new SparseFeatureIndexerPredictBatchOp()
			.setSelectedCol("feature")
			//.setOutputCol("indexer")
			.setHandleDuplicate("last")
			//.setHasValue(true)
			.linkFrom(indexer, data);
		predict.print();
	}
}
