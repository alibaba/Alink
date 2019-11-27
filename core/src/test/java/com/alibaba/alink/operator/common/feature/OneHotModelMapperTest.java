package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OneHotModelMapperTest {
	Row[] rows = new Row[] {
		Row.of(0L, "{\"modelName\":\"\\\"OneHot\\\"\",\"modelSchema\":\"\\\"model_id bigint,model_info string\\\"\","
			+ "\"isNewFormat\":\"true\",\"vectorSize\":\"18\"}"),
		Row.of(1048576L, "[\"docid@ # %null@ # %0\",\"docid@ # %doc2@ # %1\",\"docid@ # %doc1@ # %2\",\"docid@ # "
			+ "%doc0@ # %3\",\"word@ # %地@ # %4\",\"word@ # %一@ # %5\",\"word@ # %色@ # %6\",\"word@ # %null@ # %7\","
			+ "\"word@ # %清@ # %8\",\"word@ # %合@ # %9\",\"word@ # %天@ # %10\",\"word@ # %人@ # %11\",\"cnt@ # %2@ # "
			+ "%12\",\"cnt@ # %3@ # %13\",\"cnt@ # %4@ # %14\",\"cnt@ # %5@ # %15\",\"cnt@ # %1@ # %16\"]")
	};

	List <Row> model = Arrays.asList(rows);
	TableSchema modelSchema = new TableSchema(new String[] {"model_id", "model_info"},
		new TypeInformation[] {Types.LONG, Types.STRING});

	@Test
	public void testParams() throws Exception {
		System.out.println(Params.fromJson(rows[0].getField(1).toString()));
	}

	@Test
	public void testOneHot() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"docid", "word", "cnt"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG}
		);
		Params params = new Params()
			.set(OneHotPredictParams.OUTPUT_COL, "pred")
			.set(OneHotPredictParams.RESERVED_COLS, new String[] {});

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("doc0", "天", 4L)).getField(0).toString(), "$18$3:1.0 10:1.0 14:1.0");
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"pred"},
			new TypeInformation <?>[] {VectorTypes.VECTOR}));
	}

	@Test
	public void testReserved() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"docid", "word", "cnt"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG}
		);
		Params params = new Params()
			.set(OneHotPredictParams.OUTPUT_COL, "pred");

		OneHotModelMapper mapper = new OneHotModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("doc0", "天", 4L)).getField(3).toString(), "$18$3:1.0 10:1.0 14:1.0");
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"docid", "word", "cnt", "pred"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.LONG, VectorTypes.VECTOR}));
	}
}
