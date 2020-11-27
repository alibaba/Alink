package com.alibaba.alink.pipeline.regression;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for IsotonicRegression.
 */
public class IsotonicRegressionTest extends AlinkTestBase {
	private Row[] rows = new Row[] {
		Row.of(0, 0.35, 1),
		Row.of(1, 0.6, 1),
		Row.of(2, 0.55, 1),
		Row.of(3, 0.5, 1),
		Row.of(4, 0.18, 0),
		Row.of(5, 0.1, 1),
		Row.of(6, 0.8, 1),
		Row.of(7, 0.45, 0),
		Row.of(8, 0.4, 1),
		Row.of(9, 0.7, 0),
		Row.of(10, 0.02, 1),
		Row.of(11, 0.3, 0),
		Row.of(12, 0.27, 1),
		Row.of(13, 0.2, 0),
		Row.of(14, 0.9, 1)
	};

	@Test
	public void testIsotonicReg() throws Exception {
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "feature", "label"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows,
			new String[] {"id", "feature", "label"});

		IsotonicRegression op = new IsotonicRegression()
			.setFeatureCol("feature")
			.setLabelCol("label")
			.setPredictionCol("result");

		PipelineModel model = new Pipeline().add(op).fit(data);

		BatchOperator <?> res = model.transform(new TableSourceBatchOp(data));

		List <Row> list = res.select(new String[] {"id", "result"}).collect();

		double[] actual = new double[] {0.66, 0.75, 0.75, 0.75, 0.5, 0.5, 0.75, 0.66, 0.66, 0.75, 0.5, 0.5, 0.5, 0.5,
			0.75};
		for (int i = 0; i < actual.length; i++) {
			Assert.assertEquals((Double) list.get(i).getField(1), actual[(int) list.get(i).getField(0)], 0.01);
		}

		//StreamOperator<?> resStream = model.transform(new TableSourceStreamOp(dataStream));

		//resStream.print();

		//MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}

}
