package com.alibaba.alink.operator.common.regression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.IsotonicRegPredictParams;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;

public class IsotonicRegressionModelMapperTest {
	@Test
	public void testIsoReg() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0L, "{\"vectorColName\":null,\"modelName\":\"\\\"IsotonicRegressionModel\\\"\","
				+ "\"featureColName\":\"\\\"feature\\\"\",\"featureIndex\":\"0\",\"modelSchema\":\"\\\"model_id "
				+ "bigint,"
				+ "model_info string\\\"\",\"isNewFormat\":\"true\"}"),
			Row.of(1048576L, "[0.02,0.1,0.2,0.27,0.3,0.35,0.45,0.5,0.7,0.8,0.9]"),
			Row.of(2097152L,
				"[0.0,0.3333333333333333,0.3333333333333333,0.5,0.5,0.6666666666666666,0.6666666666666666,0.75,0.75,"
					+ "1.0,1.0]")
		};
		List <Row> model = Arrays.asList(rows);
		TableSchema modelSchema = new TableSchema(new String[] {"model_id", "model_info"},
			new TypeInformation[] {Types.LONG, Types.STRING});

		TableSchema dataSchema = new TableSchema(new String[] {"feature"}, new TypeInformation <?>[] {Types.DOUBLE});

		Params params = new Params()
			.set(IsotonicRegPredictParams.PREDICTION_COL, "pred");

		IsotonicRegressionModelMapper mapper = new IsotonicRegressionModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(Double.parseDouble(mapper.map(Row.of(0.35)).getField(1).toString()), 0.66, 0.01);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"feature", "pred"},
			new TypeInformation <?>[] {Types.DOUBLE, Types.DOUBLE}));
	}

	@Test
	public void testRowData() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0L, "{\"vectorColName\":\"\\\"vector\\\"\",\"modelName\":\"\\\"IsotonicRegressionModel\\\"\","
				+ "\"featureColName\":null,\"featureIndex\":\"0\",\"modelSchema\":\"\\\"model_id bigint,model_info "
				+ "string\\\"\",\"isNewFormat\":\"true\"}\n"),
			Row.of(1048576L, "[0.02,0.1,0.2,0.27,0.3,0.35,0.45,0.5,0.7,0.8,0.9]"),
			Row.of(2097152L,
				"[0.0,0.3333333333333333,0.3333333333333333,0.5,0.5,0.6666666666666666,0.6666666666666666,0.75,0.75,"
					+ "1.0,1.0]")
		};
		List <Row> model = Arrays.asList(rows);
		TableSchema modelSchema = new TableSchema(new String[] {"model_id", "model_info"},
			new TypeInformation[] {Types.LONG, Types.STRING});

		TableSchema dataSchema = new TableSchema(new String[] {"vector"}, new TypeInformation <?>[] {Types.DOUBLE});

		Params params = new Params()
			.set(IsotonicRegPredictParams.PREDICTION_COL, "pred");

		IsotonicRegressionModelMapper mapper = new IsotonicRegressionModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(Double.parseDouble(mapper.map(Row.of("0.81, 0.35")).getField(1).toString()), 1.0, 0.01);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"vector", "pred"},
			new TypeInformation <?>[] {Types.DOUBLE, Types.DOUBLE}));
	}
}