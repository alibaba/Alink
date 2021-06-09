package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.dataproc.JsonValueParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class JsonPathMapperTest extends AlinkTestBase {

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

		jsonPathMapper.open();

		Row ret = jsonPathMapper.map(Row.of("{\"key\": [\"value\"]}"));

		Assert.assertEquals(Row.of("{\"key\": [\"value\"]}", "[\"value\"]"), ret);

		jsonPathMapper.close();
	}


	@Test
	public void testMultiColumn() throws Exception {
		TableSchema dataSchema = new TableSchema(
			new String[] {"content", "content1"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING}
		);

		Params params = new Params()
			.set(JsonValueParams.JSON_PATHS, new String[] {"$.key"})
			.set(JsonValueParams.SELECTED_COL, "content1")
			.set(JsonValueParams.OUTPUT_COLS, new String[] {"parsed_content"});

		JsonPathMapper jsonPathMapper = new JsonPathMapper(dataSchema, params);

		jsonPathMapper.open();

		Row ret = jsonPathMapper.map(Row.of("{\"key\": [\"value\"]}", "{\"key\": [\"value\"]}"));

		Assert.assertEquals(Row.of("{\"key\": [\"value\"]}", "{\"key\": [\"value\"]}", "[\"value\"]"), ret);

		jsonPathMapper.close();
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

		jsonPathMapper.open();

		Row ret =  jsonPathMapper.map(Row.of("{\"key\": 123}"));

		Assert.assertEquals(Row.of("{\"key\": 123}", 123L), ret);

		jsonPathMapper.close();
	}
}