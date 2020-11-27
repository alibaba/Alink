package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.dataproc.JsonValueParams;
import org.junit.Assert;
import org.junit.Test;

public class JsonPathMapperTest {

	@Test
	public void testStringType() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"content"},
			new TypeInformation <?>[] {Types.STRING}
		);

		Params params = new Params()
			.set(JsonValueParams.JSON_PATHS, new String[] {"$.key"})
			.set(JsonValueParams.SELECTED_COL, "content")
			.set(JsonValueParams.OUTPUT_COLS, new String[] {"parsed_content"});

		JsonPathMapper jsonPathMapper = new JsonPathMapper(dataSchema, params);

		RowCollector rowCollector = new RowCollector();

		jsonPathMapper.flatMap(Row.of("{\"key\": [\"value\"]}"), rowCollector);

		Assert.assertArrayEquals(
			new Row[] {Row.of("{\"key\": [\"value\"]}", "[\"value\"]")},
			rowCollector.getRows().toArray(new Row[0])
		);
	}

	@Test
	public void testLongType() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"content"},
			new TypeInformation <?>[] {Types.STRING}
		);

		Params params = new Params()
			.set(JsonValueParams.JSON_PATHS, new String[] {"$.key"})
			.set(JsonValueParams.SELECTED_COL, "content")
			.set(JsonValueParams.OUTPUT_COLS, new String[] {"parsed_content"})
			.set(JsonValueParams.OUTPUT_COL_TYPES, new String[] {"bigint"});

		JsonPathMapper jsonPathMapper = new JsonPathMapper(dataSchema, params);

		RowCollector rowCollector = new RowCollector();

		jsonPathMapper.flatMap(Row.of("{\"key\": 123}"), rowCollector);

		Assert.assertArrayEquals(
			new Row[] {Row.of("{\"key\": 123}", 123L)},
			rowCollector.getRows().toArray(new Row[0])
		);
	}
}