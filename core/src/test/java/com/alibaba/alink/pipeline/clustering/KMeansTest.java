package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.clustering.KMeansPredictStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class KMeansTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1 0.1 0.1"),
			Row.of(2, "0.2 0.2 0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "vector"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "vector"});

		KMeans kMeans = new KMeans()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setPredictionDistanceCol("distance")
			.setK(2)
			.enableLazyPrintModelInfo();

		PipelineModel model = new Pipeline().add(kMeans).fit(data);

		BatchOperator <?> res = model.transform(new TableSourceBatchOp(data));

		List <Row> list = res.select(new String[] {"id", "distance"})
			.collect();
		double[] actual = new double[] {0.173, 0, 0.173, 0.173, 0, 0.173};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals((Double) list.get(i).getField(1), actual[(int) list.get(i).getField(0)], 0.01);
		}

		Table resTable = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, resTable).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testInitializer() {
		KMeansModel model = new KMeansModel();
		Assert.assertEquals(model.getParams().size(), 0);
		KMeans kMeans = new KMeans(new Params());
		Assert.assertEquals(kMeans.getParams().size(), 0);

		KMeansTrainBatchOp op = new KMeansTrainBatchOp();
		Assert.assertEquals(op.getParams().size(), 0);

		KMeansPredictBatchOp predict = new KMeansPredictBatchOp(new Params());
		Assert.assertEquals(predict.getParams().size(), 0);
		predict = new KMeansPredictBatchOp();
		Assert.assertEquals(predict.getParams().size(), 0);

		KMeansPredictStreamOp predictStream = new KMeansPredictStreamOp(op, new Params());
		Assert.assertEquals(predictStream.getParams().size(), 0);
		predictStream = new KMeansPredictStreamOp(predict);
		Assert.assertEquals(predictStream.getParams().size(), 0);
	}

}