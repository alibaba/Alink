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
 * Unit test for StringModelDataConverter.
 */
public class StringModelDataConverterTest extends AlinkTestBase {
	private TableSchema modelSchema;
	private TableSchema dataSchema = new TableSchema(new String[] {"id", "str"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING});

	static TableSchema expectSchema = new TableSchema(new String[] {"id", "str", "topN"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING, Types.STRING});

	@Before
	public void init() {
		StringModelDataConverter dataConverter = new StringModelDataConverter();
		dataConverter.setIdType(Types.STRING);
		modelSchema = dataConverter.getModelSchema();
	}

	@Test
	public void testSimilarity() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null, null,
				"{\"selectedCol\":\"\\\"str\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"STRING\\\"\",\"metric\":\"\\\"COSINE\\\"\",\"text\":\"false\"}", null),
			Row.of("dict1", "ABCE", "3.0", null, null),
			Row.of("dict2", "ACM", "2.0", null, null),
			Row.of("dict3", "AB", "1.0", null, null),
			Row.of("dict4", "DEAN", "3.0", null, null),
			Row.of("dict5", "D", "1.0", null, null),
			Row.of("dict6", "AFECBAk", "5.0", null, null)
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
		double[] expect = new double[] {0.816, 0.707, 0.0};
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
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1", "dict3"});
		expect = new double[] {0.816, 0.707};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 1)
			.set(NearestNeighborPredictParams.RADIUS, 0.5);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1"});
		expect = new double[] {0.816};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

	@Test
	public void testDistance() throws Exception {
		Row[] model = new Row[] {
			Row.of(null, null, null,
				"{\"selectedCol\":\"\\\"str\\\"\",\"idCol\":\"\\\"id\\\"\",\"idType\":\"\\\"VARCHAR\\\"\","
					+ "\"trainType\":\"\\\"STRING\\\"\",\"metric\":\"\\\"LEVENSHTEIN\\\"\",\"text\":\"false\"}", null),
			Row.of("dict1", "ABCE", null, null, null),
			Row.of("dict2", "ACM", null, null, null),
			Row.of("dict3", "AB", null, null, null),
			Row.of("dict4", "DEAN", null, null, null),
			Row.of("dict5", "D", null, null, null),
			Row.of("dict6", "AFECBAk", null, null, null)
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
		double[] expect = new double[] {1.0, 1.0, 2.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.RADIUS, 1.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict3", "dict1"});
		expect = new double[] {1.0, 1.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}

		params = new Params()
			.set(NearestNeighborPredictParams.SELECTED_COL, "str")
			.set(NearestNeighborPredictParams.OUTPUT_COL, "topN")
			.set(NearestNeighborPredictParams.TOP_N, 1)
			.set(NearestNeighborPredictParams.RADIUS, 1.0);

		mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		mapper.loadModel(new ArrayList <Row>(Arrays.asList(model)));

		row = mapper.map(Row.of(1L, "ABC"));
		t = NearestNeighborsMapper.extractKObject((String) row.getField(2));
		Assert.assertArrayEquals(t.f0.toArray(new String[0]), new String[] {"dict1"});
		expect = new double[] {1.0};
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], (double) t.f1.get(i), 0.01);
		}
	}

}