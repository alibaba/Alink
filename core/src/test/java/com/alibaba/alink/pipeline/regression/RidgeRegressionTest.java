package com.alibaba.alink.pipeline.regression;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class RidgeRegressionTest extends AlinkTestBase {
	Row[] vecrows = new Row[] {
		Row.of("$3$0:1.0 1:7.0 2:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 16.8),
		Row.of("$3$0:1.0 1:3.0 2:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 6.7),
		Row.of("$3$0:1.0 1:2.0 2:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 6.9),
		Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 3.0 4.0", 1.0, 3.0, 4.0, 8.0)
	};
	String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "label"};

	@Test
	public void regressionPipelineTest() throws Exception {
		BatchOperator <?> vecdata = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);
		StreamOperator <?> svecdata = new MemSourceStreamOp(Arrays.asList(vecrows), veccolNames);

		String[] xVars = new String[] {"f0", "f1", "f2"};
		String yVar = "label";
		String vec = "vec";
		String svec = "svec";
		RidgeRegression ridge = new RidgeRegression()
			.setLabelCol(yVar)
			.setFeatureCols(xVars)
			.setLambda(0.01)
			.setMaxIter(10)
			.setPredictionCol("linpred");

		RidgeRegression vridge = new RidgeRegression()
			.setLabelCol(yVar)
			.setVectorCol(vec)
			.setLambda(0.01)
			.setMaxIter(10)
			.setOptimMethod("newton")
			.setPredictionCol("vlinpred");

		RidgeRegression svridge = new RidgeRegression()
			.setLabelCol(yVar)
			.setVectorCol(svec)
			.setLambda(0.01)
			.setMaxIter(10)
			.setPredictionCol("svlinpred");

		Pipeline pl = new Pipeline().add(ridge).add(vridge).add(svridge);
		PipelineModel model = pl.fit(vecdata);

		BatchOperator <?> result = model.transform(vecdata).select(
			new String[] {"label", "linpred", "vlinpred", "svlinpred"});

		List <Row> data = result.collect();
		for (Row row : data) {
			if ((double) row.getField(0) == 16.8000) {
				Assert.assertEquals((double) row.getField(1), 16.77322547668301, 0.01);
				Assert.assertEquals((double) row.getField(2), 16.620448399254673, 0.01);
				Assert.assertEquals((double) row.getField(3), 16.384437074591887, 0.01);
			} else if ((double) row.getField(0) == 6.7000) {
				Assert.assertEquals((double) row.getField(1), 6.932628087721653, 0.01);
				Assert.assertEquals((double) row.getField(2), 6.775060404865803, 0.01);
				Assert.assertEquals((double) row.getField(3), 7.425378715755974, 0.01);
			}
		}
		// below is stream test code
		CollectSinkStreamOp sop = model.transform(svecdata).select(
			new String[] {"label", "linpred", "vlinpred", "svlinpred"})
			.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List <Row> rows = sop.getAndRemoveValues();
		for (Row row : rows) {
			if ((double) row.getField(0) == 16.8000) {
				Assert.assertEquals((double) row.getField(1), 16.77322547668301, 0.01);
				Assert.assertEquals((double) row.getField(2), 16.620448399254673, 0.01);
				Assert.assertEquals((double) row.getField(3), 16.384437074591887, 0.01);
			} else if ((double) row.getField(0) == 6.7000) {
				Assert.assertEquals((double) row.getField(1), 6.932628087721653, 0.01);
				Assert.assertEquals((double) row.getField(2), 6.775060404865803, 0.01);
				Assert.assertEquals((double) row.getField(3), 7.425378715755974, 0.01);
			}
		}
	}
}
