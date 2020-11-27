package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.BisectingKMeansPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.BisectingKMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.clustering.BisectingKMeansPredictStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class BisectingKMeansTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "0  0  0"),
			Row.of(1, "0.1  0.1  0.1"),
			Row.of(2, "0.2  0.2  0.2"),
			Row.of(3, "9  9  9"),
			Row.of(4, "9.1  9.1  9.1"),
			Row.of(5, "9.2  9.2  9.2"),
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "vector"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "vector"});

		BisectingKMeans bisectingKMeans = new BisectingKMeans()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setK(3)
			.setMaxIter(10)
			.enableLazyPrintModelInfo();

		PipelineModel model = new Pipeline().add(bisectingKMeans).fit(data);

		BatchOperator <?> res = model.transform(new TableSourceBatchOp(data));

		List <Row> list = res.select(new String[] {"id", "pred"}).collect();
		Long[] actual = new Long[] {0L, 0L, 0L, 1L, 2L, 2L};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals(list.get(i).getField(1), actual[(int) list.get(i).getField(0)]);
		}

		Table resStream = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, resStream).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

	@Test
	public void testInitializer() {
		BisectingKMeansModel model = new BisectingKMeansModel();
		Assert.assertEquals(model.getParams().size(), 0);
		BisectingKMeans bisectingKMeans = new BisectingKMeans(new Params());
		Assert.assertEquals(bisectingKMeans.getParams().size(), 0);

		BisectingKMeansTrainBatchOp op = new BisectingKMeansTrainBatchOp();
		Assert.assertEquals(op.getParams().size(), 0);

		BisectingKMeansPredictBatchOp predict = new BisectingKMeansPredictBatchOp(new Params());
		Assert.assertEquals(predict.getParams().size(), 0);
		predict = new BisectingKMeansPredictBatchOp();
		Assert.assertEquals(predict.getParams().size(), 0);

		BisectingKMeansPredictStreamOp predictStream = new BisectingKMeansPredictStreamOp(op, new Params());
		Assert.assertEquals(predictStream.getParams().size(), 0);
		predictStream = new BisectingKMeansPredictStreamOp(predict);
		Assert.assertEquals(predictStream.getParams().size(), 0);

	}
}
