package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
			Row.of(0, 0, 0),
			Row.of(1, 8, 8),
			Row.of(2, 1, 2),
			Row.of(3, 9, 10),
			Row.of(4, 3, 1),
			Row.of(5, 10, 7)
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "f0", "f1"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "f0", "f1"});

		GeoKMeans kMeans = new GeoKMeans()
			.setLatitudeCol("f0")
			.setLongitudeCol("f1")
			.setPredictionCol("pred")
			.setPredictionDistanceCol("distance")
			.setK(2);

		PipelineModel model = new Pipeline().add(kMeans).fit(data);

		Table res = model.transform(data);

		List <Row> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(
			res, Row.class)
			.collect();

		double[] actual = new double[] {185.31, 117.08, 117.18, 183.04, 185.32, 183.70};
		for (int i = 0; i < actual.length; i++) {
			Row tmpData = list.get(i);
			Assert.assertEquals((double) tmpData.getField(4), actual[(int) tmpData.getField(0)], 0.01);
		}

		res = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).addSink(
			new SinkFunction <Row>() {
				@Override
				public void invoke(Row value) throws Exception {
					Assert.assertEquals((double) value.getField(4), actual[(int) value.getField(0)], 0.01);
				}
			});

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

}