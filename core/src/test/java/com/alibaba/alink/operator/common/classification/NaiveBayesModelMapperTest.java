package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.classification.NaiveBayesPredictParams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class NaiveBayesModelMapperTest {
	Row[] rows = new Row[] {
		Row.of(0L, "{\"labelType\":\"4\",\"modelType\":\"\\\"BERNOULLI\\\"\",\"labelTypeName\":\"\\\"INTEGER\\\"\","
				+ "\"modelSchema\":\"\\\"model_id bigint,model_info string,label_type int\\\"\","
				+ "\"isNewFormat\":\"true\",\"featureColNames\":\"[\\\"f0\\\",\\\"f1\\\",\\\"f2\\\",\\\"f3\\\"]\"}",
			null),
		Row.of(1048576L, "{\"piArray\":[-0.6931471805599454,-0.6931471805599454],\"theta\":{\"m\":2,\"n\":4,"
			+ "\"data\":[-2.3025850929940455,-0.10536051565782611,-0.10536051565782611,-0.6931471805599452,"
			+ "-0.10536051565782611,-0.3566749439387322,-2.3025850929940455,-0.10536051565782611]}}", null),
		Row.of(Integer.MAX_VALUE * 1048576L, null, 0),
		Row.of(Integer.MAX_VALUE * 1048576L + 1L, null, 1),
	};

	List <Row> model = Arrays.asList(rows);
	TableSchema modelSchema = new TableSchema(new String[] {"model_id", "model_info", "label_type"},
		new TypeInformation[] {Types.LONG, Types.STRING, Types.INT});

	@Test
	public void testParams() throws Exception {
		System.out.println(Params.fromJson(rows[0].getField(1).toString()));
	}

	@Test
	public void testPredictCol() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation <?>[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE}
		);
		Params params = new Params()
			.set(NaiveBayesPredictParams.PREDICTION_COL, "pred")
			.set(NaiveBayesPredictParams.RESERVED_COLS, new String[] {});

		NaiveBayesModelMapper mapper = new NaiveBayesModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of(1.0, 1.0, 0.0, 1.0)).getField(0), 1);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"pred"},
			new TypeInformation <?>[] {Types.INT}));
	}

	@Test
	public void testPredictReservedCol() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"f0", "f1", "f2", "f3"},
			new TypeInformation <?>[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE}
		);
		Params params = new Params()
			.set(NaiveBayesPredictParams.PREDICTION_COL, "pred");

		NaiveBayesModelMapper mapper = new NaiveBayesModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of(1.0, 1.0, 0.0, 1.0)).getField(4), 1);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"f0", "f1", "f2", "f3", "pred"},
			new TypeInformation <?>[] {Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.INT}));
	}
}

