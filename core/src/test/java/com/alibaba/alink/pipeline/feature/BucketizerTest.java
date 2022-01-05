package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BucketizerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.BucketizerStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for Bucketizer.
 */

public class BucketizerTest extends AlinkTestBase {

	@Test
	public void testBucketizer() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, -999.9, -999.9),
			Row.of(2, -0.5, -0.2),
			Row.of(3, -0.3, -0.1),
			Row.of(4, 0.0, 0.0),
			Row.of(5, 0.2, 0.4),
			Row.of(6, 999.9, 999.9)
		};

		double[][] cutsArray = new double[][] {{-0.5, 0.0, 0.5}, {-0.3, 0.0, 0.3, 0.4}};

		List <Row> expectedRows = Arrays.asList(
			Row.of(1, 0L),
			Row.of(2, 0L),
			Row.of(3, 1L),
			Row.of(4, 1L),
			Row.of(5, 2L),
			Row.of(6, 3L)
		);

		BatchOperator <?> data = new MemSourceBatchOp(rows, new String[] {"id", "features1", "features2"});
		StreamOperator <?> dataStream = new MemSourceStreamOp(rows, new String[] {"id", "features1", "features2"});

		Bucketizer op = new Bucketizer()
			.setSelectedCols(new String[] {"features1", "features2"})
			.setOutputCols(new String[] {"bucket1", "bucket2"})
			.setCutsArray(cutsArray);

		List <Row> list = op
			.transform(data)
			.select("id, bucket1")
			.collect();

		assertListRowEqual(expectedRows, list, 0);

		CollectSinkStreamOp resS = op
			.transform(dataStream)
			.select("id, bucket1")
			.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		assertListRowEqual(expectedRows, resS.getAndRemoveValues(), 0);
	}

	@Test
	public void testInitializer() {
		Bucketizer op = new Bucketizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator<?> b = new BucketizerBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new BucketizerBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator<?> s = new BucketizerStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new BucketizerStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
