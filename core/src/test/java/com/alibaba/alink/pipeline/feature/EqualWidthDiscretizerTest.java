package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerModelInfoBatchOp;
import com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.EqualWidthDiscretizerPredictStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Pipeline test for EqualWidthDiscretizer.
 */

public class EqualWidthDiscretizerTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void test() throws Exception {
		try {
			NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 10, "col0");

			Pipeline pipeline = new Pipeline()
				.add(new EqualWidthDiscretizer()
					.setNumBuckets(3)
					.enableLazyPrintModelInfo()
					.setSelectedCols("col0"));
			numSeqSourceBatchOp.lazyPrint(-1);
			pipeline.fit(numSeqSourceBatchOp).transform(numSeqSourceBatchOp).print();
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Should not throw exception here.");
		}
	}

	@Test
	public void testException() throws Exception {
		thrown.expect(RuntimeException.class);
		NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 10, "col0");

		EqualWidthDiscretizerTrainBatchOp op = new EqualWidthDiscretizerTrainBatchOp()
			.setNumBuckets(5)
			.setNumBucketsArray(5, 4)
			.setSelectedCols("col0")
			.linkFrom(numSeqSourceBatchOp);
		op.print();

		op = new EqualWidthDiscretizerTrainBatchOp()
			.setNumBucketsArray(5)
			.setSelectedCols("col0")
			.linkFrom(numSeqSourceBatchOp);
		op.lazyCollect(new Consumer <List <Row>>() {
			@Override
			public void accept(List <Row> rows) {
				System.out.println(Arrays.toString(
					new EqualWidthDiscretizerModelInfoBatchOp.EqualWidthDiscretizerModelInfo(rows)
						.getCutsArray("col0")));
			}
		});

	}

	@Test
	public void testInitializer() {
		EqualWidthDiscretizerModel model = new EqualWidthDiscretizerModel();
		Assert.assertEquals(model.getParams().size(), 0);
		EqualWidthDiscretizer op = new EqualWidthDiscretizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new EqualWidthDiscretizerTrainBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new EqualWidthDiscretizerTrainBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		b = new EqualWidthDiscretizerPredictBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new EqualWidthDiscretizerPredictBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new EqualWidthDiscretizerPredictStreamOp(b);
		Assert.assertEquals(s.getParams().size(), 0);
		s = new EqualWidthDiscretizerPredictStreamOp(b, new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}