package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SoftmaxModelMapperTest {
	Row[] rows = new Row[] {
		Row.of(0L, "{\"hasInterceptItem\":\"true\",\"modelName\":\"\\\"softmax\\\"\",\"labelType\":\"4\","
				+ "\"modelSchema\":\"\\\"model_id bigint,model_info string,label_type int\\\"\","
				+ "\"isNewFormat\":\"true\"}",
			null),
		Row.of(1048576L, "{\"featureColNames\":[\"f0\",\"f1\",\"f2\"],\"coefVector\":{\"data\":[172.15928828045577,"
			+ "-18.99714734506609,-93.78617647691524,27.419236408736307,-825.863312143001,-47.67468533510818,"
			+ "97.56887933300092,87.33950982847793]}}", null),
		Row.of(2097152L, null, null),
		Row.of(Integer.MAX_VALUE * 1048576L, null, 1),
		Row.of(Integer.MAX_VALUE * 1048576L + 1L, null, 2),
		Row.of(Integer.MAX_VALUE * 1048576L + 2L, null, 3),
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
			new String[] {"f0", "f1", "f2"},
			new TypeInformation <?>[] {Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()}
		);
		Params params = new Params()
			.set(SoftmaxPredictParams.PREDICTION_COL, "pred")
			.set(SoftmaxPredictParams.RESERVED_COLS, new String[] {});

		SoftmaxModelMapper mapper = new SoftmaxModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of(1.0, 7.0, 9.0)).getField(0), 2);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"pred"},
			new TypeInformation <?>[] {Types.INT()}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"f0", "f1", "f2"},
			new TypeInformation <?>[] {Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()}
		);
		Params params = new Params()
			.set(SoftmaxPredictParams.PREDICTION_COL, "pred");

		SoftmaxModelMapper mapper = new SoftmaxModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of(1.0, 7.0, 9.0)).getField(3), 2);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"f0", "f1", "f2", "pred"},
			new TypeInformation <?>[] {Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT()}));
	}
}
