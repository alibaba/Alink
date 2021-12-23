package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BucketizerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.BucketizerStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Test for Bucketizer.
 */

public class BucketizerTest extends AlinkTestBase {

	private Row[] rows = new Row[] {
		Row.of(1, -999.9, -999.9),
		Row.of(2, -0.5, -0.2),
		Row.of(3, -0.3, -0.1),
		Row.of(4, 0.0, 0.0),
		Row.of(5, 0.2, 0.4),
		Row.of(6, 999.9, 999.9)
	};
	private static double[][] cutsArray = new double[][] {{-0.5, 0.0, 0.5}, {-0.3, 0.0, 0.3, 0.4}};

	@Test
	public void testBucketizer() throws Exception {
		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"id","features1", "features2"});
		StreamOperator dataStream = new MemSourceStreamOp(rows, new String[] {"id", "features1", "features2"});

		Bucketizer op = new Bucketizer()
			.setSelectedCols(new String[] {"features1", "features2"})
			.setOutputCols(new String[] {"bucket1", "bucket2"})
			.setCutsArray(cutsArray);

		BatchOperator res = op.transform(data);

		List <Row> list = res.select
				("id, bucket1").collect();

		// for stability in multi-thread case
		Collections.sort(list, new Comparator<Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				return Double.compare((int) o1.getField(0), (int) o2.getField(0));
			}
		});

		Assert.assertEquals(list.get(0).getField(1), 0L);
		Assert.assertEquals(list.get(1).getField(1), 0L);
		Assert.assertEquals(list.get(2).getField(1), 1L);
		Assert.assertEquals(list.get(3).getField(1), 1L);
		Assert.assertEquals(list.get(4).getField(1), 2L);
		Assert.assertEquals(list.get(5).getField(1), 3L);

		StreamOperator resS = op.transform(dataStream);

		resS.print();

		StreamOperator.execute();
	}

	@Test
	public void testInitializer() {
		Bucketizer op = new Bucketizer(new Params());
		Assert.assertEquals(op.getParams().size(), 0);

		BatchOperator b = new BucketizerBatchOp();
		Assert.assertEquals(b.getParams().size(), 0);
		b = new BucketizerBatchOp(new Params());
		Assert.assertEquals(b.getParams().size(), 0);

		StreamOperator s = new BucketizerStreamOp();
		Assert.assertEquals(s.getParams().size(), 0);
		s = new BucketizerStreamOp(new Params());
		Assert.assertEquals(s.getParams().size(), 0);
	}
}
