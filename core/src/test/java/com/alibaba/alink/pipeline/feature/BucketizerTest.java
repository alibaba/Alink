package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BucketizerBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.BucketizerStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for Bucketizer.
 */
public class BucketizerTest extends AlinkTestBase {

	private Row[] rows = new Row[] {
		Row.of(-999.9, -999.9),
		Row.of(-0.5, -0.2),
		Row.of(-0.3, -0.1),
		Row.of(0.0, 0.0),
		Row.of(0.2, 0.4),
		Row.of(999.9, 999.9)
	};
	private static double[][] cutsArray = new double[][] {{-0.5, 0.0, 0.5}, {-0.3, 0.0, 0.3, 0.4}};

	@Test
	public void testBucketizer() throws Exception {
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"features1", "features2"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows,
			new String[] {"features1", "features2"});

		Bucketizer op = new Bucketizer()
			.setSelectedCols(new String[] {"features1", "features2"})
			.setOutputCols(new String[] {"bucket1", "bucket2"})
			.setCutsArray(cutsArray);

		Table res = op.transform(data);

		List <Long> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select
				("bucket1"),
			Long.class).collect();
		Assert.assertArrayEquals(list.toArray(new Long[0]), new Long[] {0L, 0L, 1L, 1L, 2L, 3L});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
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
