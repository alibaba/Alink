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
 * Unit test for VectorModelDataConverter.
 */

public class VectorModelDataConverterTest extends AlinkTestBase {
	private TableSchema modelSchema;
	private TableSchema dataSchema = new TableSchema(new String[] {"id", "str"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING});

	private static TableSchema expectSchema = new TableSchema(new String[] {"id", "str", "topN"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING, Types.STRING});

	@Before
	public void init() {
		VectorModelDataConverter dataConverter = new VectorModelDataConverter();
		dataConverter.setIdType(Types.STRING);
		modelSchema = dataConverter.getModelSchema();
	}

	@Test
	public void testDense() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"vec\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"VECTOR\\\"\",\"metric\":\"\\\"EUCLIDEAN\\\"\"}", null),
			Row.of(1L, "{\"vectors\":\"{\\\"m\\\":3,\\\"n\\\":6,\\\"data\\\":[0.0,0.0,0.0,0.1,0.1,0.1,"
				+ "0.2,0.2,0.2,9.0,9.0,9.0,9.1,9.1,9.1,9.2,9.2,9.2]}\",\"label\":\"{\\\"m\\\":1,\\\"n\\\":6,"
				+ "\\\"data\\\":[0.0,0.030000000000000006,0.12000000000000002,243.0,248.42999999999995,"
				+ "253.91999999999996]}\",\"rows\":\"[{\\\"fields\\\":[\\\"dict1\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict2\\\"]},{\\\"fields\\\":[\\\"dict3\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict4\\\"]},{\\\"fields\\\":[\\\"dict5\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict6\\\"]}]\"}\n", null, null)
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
		double[] expect = new double[] {0.0, 0.17320508075688776, 0.3464101615137755};
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
		expect = new double[] {0.0, 0.173, 0.346, 15.58, 15.76, 15.93};
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
		expect = new double[] {0.0, 0.173, 0.346, 15.58};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		Assert.assertNull(mapper.map(Row.of(1L, null)).getField(2));
	}

	@Test
	public void testMultiMatrix() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"vec\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"VECTOR\\\"\",\"metric\":\"\\\"EUCLIDEAN\\\"\"}", null),
			Row.of(1L, "{\"vectors\":\"{\\\"m\\\":3,\\\"n\\\":6,\\\"data\\\":[0.0,0.0,0.0,0.1,0.1,0.1,"
				+ "0.2,0.2,0.2,9.0,9.0,9.0,9.1,9.1,9.1,9.2,9.2,9.2]}\",\"label\":\"{\\\"m\\\":1,\\\"n\\\":6,"
				+ "\\\"data\\\":[0.0,0.030000000000000006,0.12000000000000002,243.0,248.42999999999995,"
				+ "253.91999999999996]}\",\"rows\":\"[{\\\"fields\\\":[\\\"dict1\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict2\\\"]},{\\\"fields\\\":[\\\"dict3\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict4\\\"]},{\\\"fields\\\":[\\\"dict5\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict6\\\"]}]\"}\n", null, null),
			Row.of(1L, "{\"vectors\":\"{\\\"m\\\":3,\\\"n\\\":6,\\\"data\\\":[0.0,0.0,0.0,0.1,0.1,0.1,"
				+ "0.2,0.2,0.2,9.0,9.0,9.0,9.1,9.1,9.1,9.2,9.2,9.2]}\",\"label\":\"{\\\"m\\\":1,\\\"n\\\":6,"
				+ "\\\"data\\\":[0.0,0.030000000000000006,0.12000000000000002,243.0,248.42999999999995,"
				+ "253.91999999999996]}\",\"rows\":\"[{\\\"fields\\\":[\\\"dict7\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict8\\\"]},{\\\"fields\\\":[\\\"dict9\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict10\\\"]},{\\\"fields\\\":[\\\"dict11\\\"]},"
				+ "{\\\"fields\\\":[\\\"dict12\\\"]}]\"}\n", null, null)
		};

		Params params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3);

		NearestNeighborsMapper mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		Row row = mapper.map(Row.of(1L, "9.2 9.2 9.2"));
		Assert.assertEquals(mapper.getOutputSchema(), expectSchema);
		Tuple2 <List <Object>, List <Object>> t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict6", "dict12", "dict11"});
		double[] expect = new double[] {0.0, 0.0, 0.17};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

	@Test
	public void testEmpty() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"vec\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"VECTOR\\\"\",\"metric\":\"\\\"EUCLIDEAN\\\"\"}", null),
		};

		Params params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3);

		NearestNeighborsMapper mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		Row row = mapper.map(Row.of(1L, "9.2 9.2 9.2"));
		Assert.assertEquals(mapper.getOutputSchema(), expectSchema);
		Tuple2 <List <Object>, List <Object>> t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertEquals(t.f0.size(), 0);
	}

	@Test
	public void testSparse() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"vec\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"VECTOR\\\"\",\"metric\":\"\\\"JACCARD\\\"\"}", null),
			Row.of(3L, "{\"indices\":\"[null,[1],[3],[4],[2,5],[1,4],[2],[3,5],null,null]\",\"values\":\"[null,"
					+ "[1.0],[1.0],[1.0],[1.0,1.0],[2.0,2.0],[2.0],[2.0,2.0],null,null]\",\"label\":\"{\\\"m\\\":1,"
					+ "\\\"n\\\":6,\\\"data\\\":[0.0,2.0,2.0,2.0,2.0,2.0]}\","
					+ "\"rows\":\"[{\\\"fields\\\":[\\\"dict1\\\"]},{\\\"fields\\\":[\\\"dict2\\\"]},"
					+ "{\\\"fields\\\":[\\\"dict3\\\"]},{\\\"fields\\\":[\\\"dict4\\\"]},"
					+ "{\\\"fields\\\":[\\\"dict5\\\"]},{\\\"fields\\\":[\\\"dict6\\\"]}]\",\"vectorNum\":\"6\"}\n",
				null,
				null)
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
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict2", "dict4", "dict3"});
		double[] expect = new double[] {0.67, 0.67, 1.0};
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
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict4", "dict2"});
		expect = new double[] {0.6666666666666667, 0.6666666666666667};
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
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict2", "dict4", "dict3", "dict1"});
		expect = new double[] {0.67, 0.67, 1.0, 1.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

	}
}