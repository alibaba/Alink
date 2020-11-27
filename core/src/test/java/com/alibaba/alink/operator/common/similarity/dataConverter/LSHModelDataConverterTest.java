package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.recommendation.KObjectUtil;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LSHModelDataConverterTest extends AlinkTestBase {
	private TableSchema modelSchema;
	private TableSchema dataSchema = new TableSchema(new String[] {"id", "str"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING});

	static TableSchema expectSchema = new TableSchema(new String[] {"id", "str", "topN"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING, Types.STRING});

	public static Double[] extractScore(String res) {
		List <Object> list = KObjectUtil.deserializeKObject(
			res, new String[] {"METRIC"}, new Type[] {Double.class}
		).get("METRIC");
		if (null != list) {
			return list.toArray(new Double[0]);
		} else {
			return new Double[0];
		}
	}

	@Test
	public void testKDTree() throws Exception {
		KDTreeModelDataConverter dataConverter = new KDTreeModelDataConverter();
		dataConverter.setIdType(Types.LONG);
		modelSchema = dataConverter.getModelSchema();
		Row[] model = new Row[] {
			Row.of(null, null, null, null,
				"{\"selectedCol\":\"\\\"vec\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"APPROX_VECTOR\\\"\",\"distanceType\":\"\\\"EUCLIDEAN\\\"\","
					+ "\"vectorSize\":\"3\",\"solver\":\"\\\"KDTREE\\\"\"}", null),
			Row.of(0L, 0L, "{\"vector\":\"\\\"0.0 0.0 0.0\\\"\",\"label\":\"{\\\"data\\\":[0.0]}\","
				+ "\"rows\":\"{\\\"fields\\\":[\\\"dict1\\\"]}\"}", null, null, null),
			Row.of(0L, 1L, "{\"vector\":\"\\\"0.1 0.1 0.1\\\"\","
				+ "\"label\":\"{\\\"data\\\":[0.030000000000000006]}\","
				+ "\"rows\":\"{\\\"fields\\\":[\\\"dict2\\\"]}\"}", null, null, null),
			Row.of(0L, 2L, "{\"vector\":\"\\\"0.2 0.2 0.2\\\"\","
					+ "\"label\":\"{\\\"data\\\":[0.12000000000000002]}\","
					+ "\"rows\":\"{\\\"fields\\\":[\\\"dict3\\\"]}\"}",
				null, null, null),
			Row.of(0L, 3L, "{\"vector\":\"\\\"9.0 9.0 9.0\\\"\",\"label\":\"{\\\"data\\\":[243.0]}\","
				+ "\"rows\":\"{\\\"fields\\\":[\\\"dict4\\\"]}\"}", null, null, null),
			Row.of(0L, 4L, "{\"vector\":\"\\\"9.1 9.1 9.1\\\"\","
					+ "\"label\":\"{\\\"data\\\":[248.42999999999995]}\","
					+ "\"rows\":\"{\\\"fields\\\":[\\\"dict5\\\"]}\"}",
				null, null, null),
			Row.of(0L, 5L, "{\"vector\":\"\\\"9.2 9.2 9.2\\\"\","
					+ "\"label\":\"{\\\"data\\\":[253.91999999999996]}\","
					+ "\"rows\":\"{\\\"fields\\\":[\\\"dict6\\\"]}\"}",
				null, null, null),
			Row.of(0L, null, null, "{\"nodeIndex\":2,\"startIndex\":0,\"endIndex\":6,\"splitDim\":0,"
				+ "\"left\":{\"nodeIndex\":0,\"startIndex\":0,\"endIndex\":2,\"splitDim\":0,\"left\":null,"
				+ "\"right\":{\"nodeIndex\":1,\"startIndex\":1,\"endIndex\":2,\"splitDim\":0,\"left\":null,"
				+ "\"right\":null,\"downThre\":[0.1,0.1,0.1],\"upThre\":[0.1,0.1,0.1]},\"downThre\":[0.0,0.0,0.0],"
				+ "\"upThre\":[0.1,0.1,0.1]},\"right\":{\"nodeIndex\":4,\"startIndex\":3,\"endIndex\":6,"
				+ "\"splitDim\":0,\"left\":{\"nodeIndex\":3,\"startIndex\":3,\"endIndex\":4,\"splitDim\":0,"
				+ "\"left\":null,\"right\":null,\"downThre\":[9.0,9.0,9.0],\"upThre\":[9.0,9.0,9.0]},"
				+ "\"right\":{\"nodeIndex\":5,\"startIndex\":5,\"endIndex\":6,\"splitDim\":0,\"left\":null,"
				+ "\"right\":null,\"downThre\":[9.2,9.2,9.2],\"upThre\":[9.2,9.2,9.2]},\"downThre\":[9.0,9.0,9.0],"
				+ "\"upThre\":[9.2,9.2,9.2]},\"downThre\":[0.0,0.0,0.0],\"upThre\":[9.2,9.2,9.2]}\n", null, null)
		};

		Params params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3);

		NearestNeighborsMapper mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		Row row = mapper.map(Row.of(1L, "0 0 0"));
		Assert.assertEquals(mapper.getOutputSchema(), expectSchema);
		Tuple2 <List <Object>, List <Object>> t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict2", "dict3"});
		double[] expect = new double[] {0.0, 0.173, 0.346};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.RADIUS, 20.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "0 0 0"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]),
			new String[] {"dict1", "dict2", "dict3", "dict4", "dict5", "dict6"});
		expect = new double[] {0.0, 0.173, 0.346, 15.588, 15.761, 15.934};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 4)
			.set(NearestNeighborPredictParams.RADIUS, 20.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "0 0 0"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict2", "dict3", "dict4"});
		expect = new double[] {0.0, 0.173, 0.346, 15.588};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

	@Test
	public void testEuclidean() throws Exception {
		LSHModelDataConverter dataConverter = new LSHModelDataConverter();
		dataConverter.setIdType(Types.LONG);
		modelSchema = dataConverter.getModelSchema();
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"vec\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"projectionWidth\":\"1.0\",\"trainType\":\"\\\"APPROX_VECTOR\\\"\","
					+ "\"metric\":\"\\\"EUCLIDEAN\\\"\",\"randVectors\":\"[[{\\\"data\\\":[0.3336056067134845,"
					+ "-0.37476440958760915,0.8650196162375619]}]]\",\"randNumber\":\"[[0.5975452777972018]]\","
					+ "\"solver\":\"\\\"LSH\\\"\"}", null),
			Row.of(null, "{\"f0\":\"dict1\",\"f1\":\"0.0 0.0 0.0\"}", null, null),
			Row.of(null, "{\"f0\":\"dict2\",\"f1\":\"0.1 0.1 0.1\"}", null, null),
			Row.of(null, "{\"f0\":\"dict3\",\"f1\":\"0.2 0.2 0.2\"}", null, null),
			Row.of(null, "{\"f0\":\"dict4\",\"f1\":\"9.0 9.0 9.0\"}", null, null),
			Row.of(null, "{\"f0\":\"dict5\",\"f1\":\"9.1 9.1 9.1\"}", null, null),
			Row.of(null, "{\"f0\":\"dict6\",\"f1\":\"9.2 9.2 9.2\"}", null, null),
			Row.of("{\"f0\":-1152336412,\"f1\":[\"dict4\",\"dict5\",\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":593689054,\"f1\":[\"dict1\",\"dict2\",\"dict3\"]}", null, null, null)
		};

		Params params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3);

		NearestNeighborsMapper mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		Row row = mapper.map(Row.of(1L, "0 0 0"));
		Assert.assertEquals(mapper.getOutputSchema(), expectSchema);
		Tuple2 <List <Object>, List <Object>> t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict2", "dict3"});
		double[] expect = new double[] {0.0, 0.173, 0.346};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.RADIUS, 20.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "0 0 0"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict2", "dict3"});
		expect = new double[] {0.0, 0.173, 0.346};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 4)
			.set(NearestNeighborPredictParams.RADIUS, 20.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "0 0 0"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict2", "dict3"});
		expect = new double[] {0.0, 0.173, 0.346};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

	@Test
	public void testJaccard() throws Exception {
		LSHModelDataConverter dataConverter = new LSHModelDataConverter();
		dataConverter.setIdType(Types.LONG);
		modelSchema = dataConverter.getModelSchema();
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"vec\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"randCoefficientsB\":\"[[1785505948]]\",\"trainType\":\"\\\"APPROX_VECTOR\\\"\","
					+ "\"randCoefficientsA\":\"[[1569741361]]\",\"metric\":\"\\\"JACCARD\\\"\","
					+ "\"solver\":\"\\\"LSH\\\"\"}", null),
			Row.of(null, "{\"f0\":\"dict1\",\"f1\":\"$10$\"}", null, null),
			Row.of(null, "{\"f0\":\"dict2\",\"f1\":\"$10$1:1.0 5:2.0\"}", null, null),
			Row.of(null, "{\"f0\":\"dict3\",\"f1\":\"$10$4:1.0 6:2.0\"}", null, null),
			Row.of(null, "{\"f0\":\"dict4\",\"f1\":\"$10$2:1.0 7:2.0\"}", null, null),
			Row.of(null, "{\"f0\":\"dict5\",\"f1\":\"$10$3:1.0 5:2.0\"}", null, null),
			Row.of(null, "{\"f0\":\"dict6\",\"f1\":\"$10$4:1.0 7:2.0\"}", null, null),
			Row.of("{\"f0\":-1391219424,\"f1\":[\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":-1303151775,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":-964991960,\"f1\":[\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":-133771986,\"f1\":[\"dict2\",\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":805379812,\"f1\":[\"dict3\"]}", null, null, null)
		};

		Params params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3);

		NearestNeighborsMapper mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		Row row = mapper.map(Row.of(1L, "$10$1:1.0 2:2.0"));
		Assert.assertEquals(mapper.getOutputSchema(), expectSchema);
		Tuple2 <List <Object>, List <Object>> t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict4"});
		double[] expect = new double[] {0.6666666666666667};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.RADIUS, 0.9);
		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "$10$1:1.0 2:2.0"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict4"});
		expect = new double[] {0.6666666666666667};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 4)
			.set(NearestNeighborPredictParams.RADIUS, 1.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "$10$1:1.0 2:2.0"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict4"});
		expect = new double[] {0.6666666666666667};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

	}

}