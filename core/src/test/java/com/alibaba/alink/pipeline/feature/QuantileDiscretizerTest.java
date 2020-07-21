package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.common.feature.OneHotModelInfo;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelInfo;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class QuantileDiscretizerTest {

	@Test
	public void train() {
		try {
			NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 1000, "col0");

			Pipeline pipeline = new Pipeline()
				.add(new QuantileDiscretizer()
					.setNumBuckets(2)
					.setSelectedCols(new String[] {"col0"})
					.enableLazyPrintModelInfo());

			List<Row> result = pipeline.fit(numSeqSourceBatchOp).transform(numSeqSourceBatchOp).collect();
		} catch (Exception ex) {
			Assert.fail("Should not throw exception here.");
		}
	}

	@Test
	public void testLazy() throws Exception{
		NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 1000, "col0");

		QuantileDiscretizerTrainBatchOp op = new QuantileDiscretizerTrainBatchOp()
			.setSelectedCols("col0")
			.setNumBuckets(10)
			.linkFrom(numSeqSourceBatchOp);

		op.lazyPrintModelInfo();

		op.lazyCollectModelInfo(new Consumer<QuantileDiscretizerModelInfo>() {
			@Override
			public void accept(QuantileDiscretizerModelInfo quantileDiscretizerModelInfo) {
				System.out.println(Arrays.toString(quantileDiscretizerModelInfo.getCutsArray("col0")));
				Assert.assertEquals(quantileDiscretizerModelInfo.getSelectedColsInModel().length, 1);
			}
		});

		BatchOperator.execute();
	}
}