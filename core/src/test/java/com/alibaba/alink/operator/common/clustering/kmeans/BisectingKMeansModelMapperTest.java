package com.alibaba.alink.operator.common.clustering.kmeans;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelDataConverter;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelMapper;
import com.alibaba.alink.params.clustering.KMeansPredictParams;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for KMeansModelMapper.
 */
public class BisectingKMeansModelMapperTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private Row[] rows = new Row[] {
		Row.of(0L, "{\"vectorCol\":\"\\\"Y\\\"\",\"distanceType\":\"\\\"EUCLIDEAN\\\"\",\"k\":\"3\","
			+ "\"vectorSize\":\"3\"}"),
		Row.of(1048576L,
			"{\"clusterId\":1,\"size\":6,\"center\":{\"data\":[4.6,4.6,4.6]},\"cost\":364.61999999999995}"),
		Row.of(2097152L, "{\"clusterId\":2,\"size\":3,\"center\":{\"data\":[0.1,0.1,0.1]},\"cost\":0.06}"),
		Row.of(3145728L,
			"{\"clusterId\":3,\"size\":3,\"center\":{\"data\":[9.1,9.1,9.1]},\"cost\":0.06000000000005912}"),
		Row.of(4194304L, "{\"clusterId\":6,\"size\":1,\"center\":{\"data\":[9.0,9.0,9.0]},\"cost\":0.0}"),
		Row.of(5242880L, "{\"clusterId\":7,\"size\":2,\"center\":{\"data\":[9.149999999999999,9.149999999999999,"
			+ "9.149999999999999]},\"cost\":0.015000000000100044}")
	};

	private List <Row> model = Arrays.asList(rows);
	private TableSchema modelSchema = new BisectingKMeansModelDataConverter().getModelSchema();

	@Test
	public void testDefault() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"Y"}, new TypeInformation <?>[] {Types.STRING}
		);
		Params params = new Params()
			.set(KMeansPredictParams.PREDICTION_COL, "pred");

		BisectingKMeansModelMapper mapper = new BisectingKMeansModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("0 0 0")).getField(1), 0L);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"Y", "pred"},
			new TypeInformation <?>[] {Types.STRING, Types.LONG}));
	}

	@Test
	public void testDetailOutput() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"Y"}, new TypeInformation <?>[] {Types.STRING}
		);
		Params params = new Params()
			.set(KMeansPredictParams.PREDICTION_COL, "pred")
			.set(KMeansPredictParams.PREDICTION_DETAIL_COL, "detail");

		BisectingKMeansModelMapper mapper = new BisectingKMeansModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("0 0 0")).getField(2), "0.5 0.25 0.25");
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"Y", "pred", "detail"},
			new TypeInformation <?>[] {Types.STRING, Types.LONG, Types.STRING}));
	}

	@Test
	public void testCosine() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0L, "{\"vectorCol\":\"\\\"Y\\\"\",\"distanceType\":\"\\\"COSINE\\\"\",\"k\":\"2\","
				+ "\"vectorSize\":\"3\"}"),
			Row.of(1048576L, "{\"clusterId\":1,\"size\":6,\"center\":{\"data\":[0.38404277482376237,"
				+ "0.5455711027027044,0.7448914813591441]},\"cost\":0.225825649929436}\n"),
			Row.of(2097152L, "{\"clusterId\":2,\"size\":3,\"center\":{\"data\":[0.16321889668192696,"
				+ "0.47679175258685436,0.8637298283786952]},\"cost\":0.025594817648504797}\n"),
			Row.of(3145728L, "{\"clusterId\":3,\"size\":3,\"center\":{\"data\":[0.5773502691896257,"
				+ "0.5773502691896257,0.5773502691896257]},\"cost\":4.440892098500626E-16}\n")
		};

		List <Row> model = Arrays.asList(rows);

		TableSchema dataSchema = new TableSchema(
			new String[] {"Y"}, new TypeInformation <?>[] {Types.STRING}
		);
		Params params = new Params()
			.set(KMeansPredictParams.PREDICTION_COL, "pred")
			.set(KMeansPredictParams.PREDICTION_DETAIL_COL, "detail");

		BisectingKMeansModelMapper mapper = new BisectingKMeansModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("0 0.1 0.2")).getField(1), 0L);
	}

	@Test
	public void testErrorInput() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"Y"}, new TypeInformation <?>[] {Types.STRING}
		);
		Params params = new Params()
			.set(KMeansPredictParams.PREDICTION_COL, "pred")
			.set(KMeansPredictParams.PREDICTION_DETAIL_COL, "detail");

		BisectingKMeansModelMapper mapper = new BisectingKMeansModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Dim of predict data not equal to vectorSize of training data: 3");
		mapper.map(Row.of("0 0 0 0"));
	}
}
