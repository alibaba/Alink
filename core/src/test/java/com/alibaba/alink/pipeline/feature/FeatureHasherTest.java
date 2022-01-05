package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.FeatureHasherBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.FeatureHasherStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for FeatureHasher.
 */

public class FeatureHasherTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, 1.1, true, "2", "A"),
			Row.of(2, 1.1, false, "2", "B"),
			Row.of(3, 1.1, true, "1", "B"),
			Row.of(4, 2.2, true, "1", "A"),
		};

		List <Row> expectedRows = Arrays.asList(
			Row.of(1, new SparseVector(100, new int[] {9, 38, 45, 95}, new double[] {1.0, 1.1, 1.0, 1.0})),
			Row.of(2, new SparseVector(100, new int[] {9, 30, 38, 76}, new double[] {1.0, 1.0, 1.1, 1.0})),
			Row.of(3, new SparseVector(100, new int[] {11, 38, 76, 95}, new double[] {1.0, 1.1, 1.0, 1.0})),
			Row.of(4, new SparseVector(100, new int[] {11, 38, 45, 95}, new double[] {1.0, 2.2, 1.0, 1.0}))
			);

		BatchOperator <?> data = new MemSourceBatchOp(rows,
			new String[] {"id", "double", "bool", "number", "str"});
		StreamOperator <?> dataStream = new MemSourceStreamOp(rows,
			new String[] {"id", "double", "bool", "number", "str"});

		FeatureHasher op = new FeatureHasher()
			.setSelectedCols(new String[] {"double", "bool", "number", "str"})
			.setNumFeatures(100)
			.setOutputCol("features");

		List <Row> list = op.transform(data)
			.select("id, features")
			.collect();

		assertListRowEqual(expectedRows, list, 0);

		CollectSinkStreamOp resStream = op
			.transform(dataStream)
			.select("id, features")
			.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		assertListRowEqual(expectedRows, resStream.getAndRemoveValues(), 0);
	}

	@Test
	public void testInitializer() {
		FeatureHasher op = new FeatureHasher(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator<?> b = new FeatureHasherBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new FeatureHasherBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator<?> s = new FeatureHasherStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new FeatureHasherStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
