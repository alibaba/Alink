package com.alibaba.alink.operator.common.regression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.regression.AftRegPredictParams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AFTRegressionMapperTest {

	Row[] rows = new Row[] {
		Row.of(0L, "{\"hasInterceptItem\":\"true\",\"vectorColName\":\"\\\"features\\\"\","
			+ "\"modelName\":\"\\\"AFTSurvivalRegTrainBatchOp\\\"\",\"labelType\":\"8\","
			+ "\"modelSchema\":\"\\\"model_id bigint,model_info string,label_type double\\\"\","
			+ "\"isNewFormat\":\"true\",\"linearModelType\":\"\\\"AFT\\\"\",\"vectorSize\":\"2\"}", null),
		Row.of(1048576L, "{\"coefVector\":{\"data\":[2.6380946835087933,-0.49631115827728234,"
			+ "0.19844422562555475,1.5472345338855131]}}", null)
	};
	List <Row> model = Arrays.asList(rows);
	TableSchema modelSchema = new TableSchema(new String[] {"model_id", "model_info", "label_type"},
		new TypeInformation[] {Types.LONG, Types.STRING, Types.DOUBLE});

	@Test
	public void testAftReg() throws Exception {
		//it seems the predict column should be put behind if the input don't have predict col.
		TableSchema dataSchema = new TableSchema(new String[] {"vector", "pred"},
			new TypeInformation <?>[] {Types.STRING, Types.DOUBLE});

		Params params = new Params()
			.set(AftRegPredictParams.VECTOR_COL, "vector")
			.set(AftRegPredictParams.PREDICTION_COL, "pred");

		AFTModelMapper mapper = new AFTModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(Double.valueOf(mapper.map(Row.of("1.560 -0.605")).getField(1).toString()), 5.71, 0.01);
		assertEquals(mapper.getOutputSchema(), dataSchema);
	}
}
