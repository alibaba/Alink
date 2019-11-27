package com.alibaba.alink.operator.common.ml;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LinearModelMapperTest {
	Row[] rows = new Row[] {
		Row.of(0L, "{\"hasInterceptItem\":\"true\",\"modelName\":\"\\\"Logistic Regression\\\"\",\"labelType\":\"4\","
			+ "\"modelSchema\":\"\\\"model_id bigint,model_info string,label_type int\\\"\","
			+ "\"isNewFormat\":\"true\",\"linearModelType\":\"\\\"LR\\\"\"}", null),
		Row.of(1048576L, "{\"featureColNames\":[\"f0\",\"f1\",\"f2\",\"f3\"],"
			+ "\"coefVector\":{\"data\":[-9.634910228989458,45.508924427487486,-22.06146649207175,"
			+ "-20.926964828123506,45.508924427487486]}}", null),
		Row.of(Integer.MAX_VALUE * 1048576L + 0L, null, 1),
		Row.of(Integer.MAX_VALUE * 1048576L + 1L, null, 0),
	};

	List <Row> model = Arrays.asList(rows);
	TableSchema modelSchema = new TableSchema(new String[] {"model_id", "model_info", "label_type"},
		new TypeInformation[] {Types.LONG(), Types.STRING(), Types.INT()});

	@Test
	public void test() throws Exception {
		System.out.println(Params.fromJson(rows[0].getField(1).toString()));
	}

	@Test
	public void test1() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation <?>[] {Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()}
		);
		Params params = new Params()
			.set(LogisticRegressionPredictParams.PREDICTION_COL, "pred")
			.set(LogisticRegressionPredictParams.RESERVED_COLS, new String[] {});

		LinearModelMapper mapper = new LinearModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of(1.0, 1.0, 0.0, 1.0)).getField(0), 1);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"pred"},
			new TypeInformation <?>[] {Types.INT()}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation <?>[] {Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()}
		);
		Params params = new Params()
			.set(LogisticRegressionPredictParams.PREDICTION_COL, "pred");

		LinearModelMapper mapper = new LinearModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of(1.0, 1.0, 0.0, 1.0)).getField(4), 1);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"f0", "f1", "f2", "f3", "pred"},
			new TypeInformation <?>[] {Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT()}));
	}
}
