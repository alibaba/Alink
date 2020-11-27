package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit test for MinHashModelDataConverter.
 */
public class MinHashModelDataConverterTest extends AlinkTestBase {
	private TableSchema modelSchema;
	private TableSchema dataSchema = new TableSchema(new String[] {"id", "str"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING});

	static TableSchema expectSchema = new TableSchema(new String[] {"id", "str", "topN"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING, Types.STRING});

	@Before
	public void init() {
		MinHashModelDataConverter dataConverter = new MinHashModelDataConverter();
		dataConverter.setIdType(Types.STRING);
		modelSchema = dataConverter.getModelSchema();
	}

	@Test
	public void testMinHash() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"str\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"paiMetric\":\"\\\"MINHASH_JACCARD_SIM\\\"\",\"trainType\":\"\\\"APPROX_STRING\\\"\","
					+ "\"metric\":\"\\\"MINHASH_JACCARD_SIM\\\"\",\"text\":\"false\"}", null),
			Row.of(null, "{\"f0\":\"dict1\",\"f1\":[323473912,873416864,68799388,15948568,52222499,378075082,"
				+ "152565331,10654137,211003671,524719659]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict2\",\"f1\":[498045464,548250736,68799388,15948568,785685081,378075082,"
				+ "1170820564,10654137,307903457,1029385287]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict3\",\"f1\":[323473912,1035999928,1111607376,449199238,942844777,"
				+ "378075082,152565331,10654137,211003671,1281718101]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict4\",\"f1\":[657439112,159679194,118585646,219446469,52222499,378075082,"
				+ "820722876,10654137,729910799,524719659]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict5\",\"f1\":[1987495240,159679194,1488480878,1946806881,1780752357,"
				+ "379555913,833652071,205969452,826810585,777052473]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict6\",\"f1\":[3497096,873416864,68799388,15948568,52222499,378075082,"
				+ "152565331,10654137,211003671,272386845]}\n", null, null),
			Row.of("{\"f0\":3497127,\"f1\":[\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":10654168,\"f1\":[\"dict3\",\"dict6\",\"dict4\",\"dict1\",\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":15948599,\"f1\":[\"dict2\",\"dict1\",\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":52222530,\"f1\":[\"dict4\",\"dict1\",\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":68799419,\"f1\":[\"dict6\",\"dict1\",\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":118585677,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":152565362,\"f1\":[\"dict6\",\"dict1\",\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":159679225,\"f1\":[\"dict5\",\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":205969483,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":211003702,\"f1\":[\"dict6\",\"dict3\",\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":219446500,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":272386876,\"f1\":[\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":307903488,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":323473943,\"f1\":[\"dict1\",\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":378075113,\"f1\":[\"dict3\",\"dict1\",\"dict4\",\"dict6\",\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":379555944,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":449199269,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":498045495,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":524719690,\"f1\":[\"dict1\",\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":548250767,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":657439143,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":729910830,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":777052504,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":785685112,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":820722907,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":826810616,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":833652102,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":873416895,\"f1\":[\"dict6\",\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":942844808,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":1029385318,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":1035999959,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":1111607407,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":1170820595,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":1281718132,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":1488480909,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":1780752388,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":1946806912,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":1987495271,\"f1\":[\"dict5\"]}", null, null, null)
		};

		Params params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3);

		NearestNeighborsMapper mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		Row row = mapper.map(Row.of(1L, "ABC"));
		Assert.assertEquals(mapper.getOutputSchema(), expectSchema);

		Tuple2 <List <Object>, List <Object>> t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict2", "dict6"});
		double[] expect = new double[] {0.7, 0.6, 0.6};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.RADIUS, 0.5);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict2", "dict3", "dict6"});
		expect = new double[] {0.7, 0.6, 0.6, 0.6};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3)
			.set(NearestNeighborPredictParams.RADIUS, 0.5);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict2", "dict6"});
		expect = new double[] {0.7, 0.6, 0.6};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

	@Test
	public void testJaccard() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"str\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"paiMetric\":\"\\\"JACCARD_SIM\\\"\",\"trainType\":\"\\\"APPROX_STRING\\\"\","
					+ "\"metric\":\"\\\"JACCARD_SIM\\\"\",\"text\":\"false\"}", null),
			Row.of(null, "{\"f0\":\"dict1\",\"f1\":[65,66,67,69]}n"),
			Row.of(null, "{\"f0\":\"dict2\",\"f1\":[65,67,77]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict3\",\"f1\":[65,66]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict4\",\"f1\":[65,68,69,78]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict5\",\"f1\":[68]}\n", null, null),
			Row.of(null, "{\"f0\":\"dict6\",\"f1\":[65,65,66,67,69,70]}\n", null, null),
			Row.of("{\"f0\":3497127,\"f1\":[\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":10654168,\"f1\":[\"dict3\",\"dict6\",\"dict4\",\"dict1\",\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":15948599,\"f1\":[\"dict2\",\"dict1\",\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":52222530,\"f1\":[\"dict4\",\"dict1\",\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":68799419,\"f1\":[\"dict6\",\"dict1\",\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":118585677,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":152565362,\"f1\":[\"dict6\",\"dict1\",\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":159679225,\"f1\":[\"dict5\",\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":205969483,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":211003702,\"f1\":[\"dict6\",\"dict3\",\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":219446500,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":272386876,\"f1\":[\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":307903488,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":323473943,\"f1\":[\"dict1\",\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":378075113,\"f1\":[\"dict3\",\"dict1\",\"dict4\",\"dict6\",\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":379555944,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":449199269,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":498045495,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":524719690,\"f1\":[\"dict1\",\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":548250767,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":657439143,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":729910830,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":777052504,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":785685112,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":820722907,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":826810616,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":833652102,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":873416895,\"f1\":[\"dict6\",\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":942844808,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":1029385318,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":1035999959,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":1111607407,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":1170820595,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":1281718132,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":1488480909,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":1780752388,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":1946806912,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":1987495271,\"f1\":[\"dict5\"]}", null, null, null)
		};

		Params params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3);

		NearestNeighborsMapper mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		Row row = mapper.map(Row.of(1L, "ABC"));
		Assert.assertEquals(mapper.getOutputSchema(), expectSchema);

		Tuple2 <List <Object>, List <Object>> t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict3", "dict2"});
		double[] expect = new double[] {0.75, 0.666, 0.5};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.RADIUS, 0.3);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict3", "dict2", "dict6"});
		expect = new double[] {0.75, 0.6666666666666666, 0.5, 0.5};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3)
			.set(NearestNeighborPredictParams.RADIUS, 0.3);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict3", "dict2"});
		expect = new double[] {0.75, 0.6666666666666666, 0.5};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

}