package com.alibaba.alink.operator.common.clustering.kmeans;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.clustering.KMeansPredictParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for KMeansModelMapper.
 */

public class KMeansModelMapperTest extends AlinkTestBase {
	private Row[] rows = new Row[] {
		Row.of(0L, "{\"vectorCol\":\"\\\"Y\\\"\",\"latitudeCol\":null,\"longitudeCol\":null,"
			+ "\"distanceType\":\"\\\"EUCLIDEAN\\\"\",\"k\":\"2\",\"vectorSize\":\"3\"}"),
		Row.of(1048576L, "{\"clusterId\":0,\"weight\":3.0,\"vec\":{\"data\":[9.1,9.1,9.1]}}"),
		Row.of(2097152L, "{\"clusterId\":1,\"weight\":3.0,\"vec\":{\"data\":[0.1,0.1,0.1]}}")
	};

	private List <Row> model = Arrays.asList(rows);
	private TableSchema modelSchema = new KMeansModelDataConverter().getModelSchema();

	@Test
	public void testDefault() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"Y", "id"}, new TypeInformation <?>[] {Types.STRING, Types.INT}
		);
		Params params = new Params()
			.set(KMeansPredictParams.PREDICTION_COL, "pred");

		KMeansModelMapper mapper = new KMeansModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("0 0 0", 1)).getField(2), 1L);
		assertEquals(mapper.map(Row.of(null, 2)).getField(2), null);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"Y", "id", "pred"},
			new TypeInformation <?>[] {Types.STRING, Types.INT, Types.LONG}));
	}

	@Test
	public void testDetailOutput() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"Y", "id"}, new TypeInformation <?>[] {Types.STRING, Types.INT}
		);
		Params params = new Params()
			.set(KMeansPredictParams.PREDICTION_COL, "pred")
			.set(KMeansPredictParams.PREDICTION_DETAIL_COL, "detail");

		KMeansModelMapper mapper = new KMeansModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("0 0 0", 1)).getField(3), "DenseVector(size = 2) 0.010869565217391353 0.9891304347826086");
		assertEquals(mapper.map(Row.of(null, 2)).getField(3), null);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"Y", "id", "pred", "detail"},
			new TypeInformation <?>[] {Types.STRING, Types.INT, Types.LONG, Types.STRING}));
	}

	@Test
	public void testDistanceOutput() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"Y", "id"}, new TypeInformation <?>[] {Types.STRING, Types.INT}
		);
		Params params = new Params()
			.set(KMeansPredictParams.PREDICTION_COL, "pred")
			.set(KMeansPredictParams.PREDICTION_DISTANCE_COL, "distance");

		KMeansModelMapper mapper = new KMeansModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals((double) mapper.map(Row.of("0 0 0", 1)).getField(3), 0.173, 0.001);
		assertEquals(mapper.map(Row.of(null, 2)).getField(3), null);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"Y", "id", "pred", "distance"},
			new TypeInformation <?>[] {Types.STRING, Types.INT, Types.LONG, Types.DOUBLE}));
	}

	@Test
	public void testDetailDistanceOutput() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"Y", "id"}, new TypeInformation <?>[] {Types.STRING, Types.INT}
		);
		Params params = new Params()
			.set(KMeansPredictParams.PREDICTION_COL, "pred")
			.set(KMeansPredictParams.PREDICTION_DETAIL_COL, "detail")
			.set(KMeansPredictParams.PREDICTION_DISTANCE_COL, "distance");

		KMeansModelMapper mapper = new KMeansModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		Row res = mapper.map(Row.of("0 0 0", 1));
		assertEquals(res.getField(3), "DenseVector(size = 2) 0.010869565217391353 0.9891304347826086");
		assertEquals((double) res.getField(4), 0.173, 0.001);
		assertEquals(mapper.map(Row.of(null, 2)).getField(3), null);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"Y", "id", "pred", "detail", "distance"},
			new TypeInformation <?>[] {Types.STRING, Types.INT, Types.LONG, Types.STRING, Types.DOUBLE}));
	}
}
