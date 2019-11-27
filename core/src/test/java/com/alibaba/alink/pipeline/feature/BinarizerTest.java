package com.alibaba.alink.pipeline.feature;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for Binarizer.
 */
public class BinarizerTest {
	Row[] rows = new Row[] {
		Row.of(1.218, 16.0, "1.560 -0.605"),
		Row.of(2.949, 4.0, "0.346 2.158"),
		Row.of(3.627, 2.0, "1.380 0.231"),
		Row.of(0.273, 15.0, "0.520 1.151"),
		Row.of(4.199, 7.0, "0.795 -0.226")
	};

	Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"label", "censor", "features"});
	Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"label", "censor", "features"});

	@Test
	public void test() throws Exception {
		Binarizer op = new Binarizer()
			.setSelectedCol("censor")
			.setThreshold(8.0);

		Table res = op.transform(data);

		List <Double> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("censor"), Double.class)
			.collect();

		Assert.assertEquals(list.toArray(new Double[0]), new Double[] {1.0, 0.0, 0.0, 1.0, 0.0});

		res = op.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}
}
