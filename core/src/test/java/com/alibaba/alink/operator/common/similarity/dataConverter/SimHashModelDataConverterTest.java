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
 * Unit Test for SimHashModelDataConverter.
 */
public class SimHashModelDataConverterTest extends AlinkTestBase {
	private TableSchema modelSchema;
	private TableSchema dataSchema = new TableSchema(new String[] {"id", "str"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING});

	static TableSchema expectSchema = new TableSchema(new String[] {"id", "str", "topN"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING, Types.STRING});

	@Before
	public void init() {
		SimHashModelDataConverter dataConverter = new SimHashModelDataConverter();
		dataConverter.setIdType(Types.STRING);
		modelSchema = dataConverter.getModelSchema();
	}

	@Test
	public void testSimilarity() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"str\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"APPROX_STRING\\\"\",\"metric\":\"\\\"SIMHASH_HAMMING_SIM\\\"\","
					+ "\"text\":\"false\"}", null),
			Row.of(null, "{\"f0\":\"dict1\",\"f1\":8588593090}\n", null, null),
			Row.of(null, "{\"f0\":\"dict2\",\"f1\":8311767488}"),
			Row.of(null, "{\"f0\":\"dict3\",\"f1\":8588600259}\n", null, null),
			Row.of(null, "{\"f0\":\"dict4\",\"f1\":9930844101}", null, null),
			Row.of(null, "{\"f0\":\"dict5\",\"f1\":8704026181}\n", null, null),
			Row.of(null, "{\"f0\":\"dict6\",\"f1\":8588591554}\n", null, null),
			Row.of("{\"f0\":-535319171,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":-1968527525,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":-757438290,\"f1\":[\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":72493399,\"f1\":[\"dict6\",\"dict3\",\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":82511433,\"f1\":[\"dict4\",\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":585897852,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":770281886,\"f1\":[\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":1045056634,\"f1\":[\"dict2\",\"dict3\",\"dict1\",\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":1082018723,\"f1\":[\"dict1\",\"dict2\",\"dict3\",\"dict5\",\"dict6\",\"dict4\"]}", null,
				null, null),
			Row.of("{\"f0\":1305834434,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":1605593907,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":1961002042,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":2138849511,\"f1\":[\"dict4\"]}", null, null, null)
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
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict6", "dict1", "dict2"});
		double[] expect = new double[] {0.984, 0.9531, 0.937};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.RADIUS, 0.9);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict6", "dict1", "dict3", "dict2"});
		expect = new double[] {0.984, 0.9531, 0.937, 0.937};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3)
			.set(NearestNeighborPredictParams.RADIUS, 0.9);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict6", "dict1", "dict2"});
		expect = new double[] {0.984, 0.9531, 0.937};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

	@Test
	public void testDistance() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null,
				"{\"selectedCol\":\"\\\"str\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"APPROX_STRING\\\"\",\"metric\":\"\\\"SIMHASH_HAMMING\\\"\","
					+ "\"text\":\"false\"}", null),
			Row.of(null, "{\"f0\":\"dict1\",\"f1\":8588593090}\n", null, null),
			Row.of(null, "{\"f0\":\"dict2\",\"f1\":8311767488}"),
			Row.of(null, "{\"f0\":\"dict3\",\"f1\":8588600259}\n", null, null),
			Row.of(null, "{\"f0\":\"dict4\",\"f1\":9930844101}", null, null),
			Row.of(null, "{\"f0\":\"dict5\",\"f1\":8704026181}\n", null, null),
			Row.of(null, "{\"f0\":\"dict6\",\"f1\":8588591554}\n", null, null),
			Row.of("{\"f0\":-535319171,\"f1\":[\"dict4\"]}", null, null, null),
			Row.of("{\"f0\":-1968527525,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":-757438290,\"f1\":[\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":72493399,\"f1\":[\"dict6\",\"dict3\",\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":82511433,\"f1\":[\"dict4\",\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":585897852,\"f1\":[\"dict5\"]}", null, null, null),
			Row.of("{\"f0\":770281886,\"f1\":[\"dict1\"]}", null, null, null),
			Row.of("{\"f0\":1045056634,\"f1\":[\"dict2\",\"dict3\",\"dict1\",\"dict6\"]}", null, null, null),
			Row.of("{\"f0\":1082018723,\"f1\":[\"dict1\",\"dict2\",\"dict3\",\"dict5\",\"dict6\",\"dict4\"]}", null,
				null, null),
			Row.of("{\"f0\":1305834434,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":1605593907,\"f1\":[\"dict2\"]}", null, null, null),
			Row.of("{\"f0\":1961002042,\"f1\":[\"dict3\"]}", null, null, null),
			Row.of("{\"f0\":2138849511,\"f1\":[\"dict4\"]}", null, null, null)
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
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict6", "dict1", "dict2"});
		double[] expect = new double[] {1.0, 3.0, 4.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.RADIUS, 4.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict6", "dict1", "dict3", "dict2"});
		expect = new double[] {1.0, 3.0, 4.0, 4.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 3)
			.set(NearestNeighborPredictParams.RADIUS, 4.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict6", "dict1", "dict2"});
		expect = new double[] {1.0, 3.0, 4.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

}