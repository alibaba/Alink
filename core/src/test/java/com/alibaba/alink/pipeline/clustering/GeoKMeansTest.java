package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class GeoKMeansTest extends AlinkTestBase {

	@Test
	public void linkPipeline() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, 0),
			Row.of(8, 8),
			Row.of(1, 2),
			Row.of(9, 10),
			Row.of(3, 1),
			Row.of(10, 7)
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"f0", "f1"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"f0", "f1"});

		GeoKMeans kMeans = new GeoKMeans()
			.setLatitudeCol("f0")
			.setLongitudeCol("f1")
			.setPredictionCol("pred")
			.setPredictionDistanceCol("distance")
			.setK(2);

		PipelineModel model = new Pipeline().add(kMeans).fit(data);

		Table res = model.transform(data);

		List <Double> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(
			res.select("distance"), Double.class)
			.collect();

		double[] actual = new double[] {185.31, 117.08, 117.18, 183.04, 185.32, 183.70};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals(list.get(i), actual[i], 0.01);
		}

		res = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

}