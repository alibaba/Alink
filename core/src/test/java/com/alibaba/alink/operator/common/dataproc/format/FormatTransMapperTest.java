package com.alibaba.alink.operator.common.dataproc.format;

import com.alibaba.alink.params.dataproc.format.*;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import org.junit.Assert;
import org.junit.Test;

public class FormatTransMapperTest {

	@Test
	public void testSparse() throws Exception {
		//String vecStr = "1.1 2,2 3.3 4.4 5.5";

		//String vecStr = "1.1 2.2 3.3 4.4 5.5";
		String vecStr = "$5$0:1.1 2:3.3 3:4.4";

		String kvStr = "0:1.1,2:3.3,3:4.4";

		//Vector vec = new DenseVector(new double[]{1.1, 2.2, 3.3, 4.4, 5.5});

		Row columns = Row.of(1.1, 2.2, 3.3, 4.4, 5.5);

		String schemaStr = "f1 double, f2 double, f3 double, f4 double, f5 double";

		FormatTransMapper transVectorToColumns = new FormatTransMapper(
			CsvUtil.schemaStr2Schema("vec string"),
			//new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.})
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.VECTOR)
				.set(FormatTransParams.TO_FORMAT, FormatType.COLUMNS)
				.set(VectorToColumnsParams.VECTOR_COL, "vec")
				.set(VectorToColumnsParams.SCHEMA_STR, schemaStr)
		);
		transVectorToColumns.open();

		Row resultVectorToColumns = transVectorToColumns.map(Row.of(vecStr));

		System.out.println(resultVectorToColumns);
		Assert.assertEquals(
			"1.1",
			resultVectorToColumns.getField(resultVectorToColumns.getArity() - 5).toString()
		);

		FormatTransMapper transColumnsToVector = new FormatTransMapper(
			CsvUtil.schemaStr2Schema(schemaStr),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.COLUMNS)
				.set(FormatTransParams.TO_FORMAT, FormatType.VECTOR)
				.set(ColumnsToVectorParams.SELECTED_COLS, new String[] {"f1", "f2", "f3", "f4", "f5"})
				.set(ColumnsToVectorParams.VECTOR_COL, "vec")
		);
		transColumnsToVector.open();

		Row resultColumnsToVector = transColumnsToVector.map(columns);

		System.out.println(resultColumnsToVector);
		Assert.assertEquals(
			"1.1 2.2 3.3 4.4 5.5",
			resultColumnsToVector.getField(resultColumnsToVector.getArity() - 1).toString()
		);

		FormatTransMapper transKvToVector = new FormatTransMapper(
			CsvUtil.schemaStr2Schema("kv string"),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.KV)
				.set(FormatTransParams.TO_FORMAT, FormatType.VECTOR)
				.set(KvToVectorParams.KV_COL, "kv")
				.set(KvToVectorParams.KV_VAL_DELIMITER, ":")
				.set(KvToVectorParams.KV_COL_DELIMITER, ",")
				.set(KvToVectorParams.VECTOR_COL, "vec")
				.set(KvToVectorParams.VECTOR_SIZE, 5L)
		);
		transKvToVector.open();

		Row resultKvToVector = transKvToVector.map(Row.of(kvStr));

		System.out.println(resultKvToVector);
		Assert.assertEquals(
			vecStr.length(),
			resultKvToVector.getField(resultKvToVector.getArity() - 1).toString().length()
		);

		FormatTransMapper transVectorToKv = new FormatTransMapper(
			CsvUtil.schemaStr2Schema("vec string"),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.VECTOR)
				.set(FormatTransParams.TO_FORMAT, FormatType.KV)
				.set(VectorToKvParams.VECTOR_COL, "vec")
				.set(VectorToKvParams.KV_COL, "kv")
				.set(VectorToKvParams.KV_VAL_DELIMITER, ":")
				.set(VectorToKvParams.KV_COL_DELIMITER, ",")
		);
		transVectorToKv.open();

		Row resultVectorToKv = transVectorToKv.map(Row.of(vecStr));

		System.out.println(resultVectorToKv);
		Assert.assertEquals(
			kvStr.length(),
			resultVectorToKv.getField(resultVectorToKv.getArity() - 1).toString().length()
		);

	}

	@Test
	public void testDense() throws Exception {
		String csvStr = "1$2.0$false$val$2018-09-10$14:22:20$2018-09-10 14:22:20";
		String kvStr = "f1=1,f2=2.0,f3=false,f4=val,f5=2018-09-10,f6=14:22:20,f7=2018-09-10 14:22:20";
		String jsonStr = "{\"f6\":\"14:22:20\",\"f7\":\"2018-09-10 14:22:20\",\"f1\":\"1\",\"f2\":\"2.0\","
			+ "\"f3\":\"false\",\"f4\":\"val\",\"f5\":\"2018-09-10\"}";

		String schemaStr = "f1 bigint, f2 double, f3 boolean, f4 string, f5 date, f6 time, f7 timestamp";

		FormatTransMapper transJsonToColumns = new FormatTransMapper(
			CsvUtil.schemaStr2Schema("json string"),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.JSON)
				.set(FormatTransParams.TO_FORMAT, FormatType.COLUMNS)
				.set(JsonToColumnsParams.JSON_COL, "json")
				.set(JsonToColumnsParams.SCHEMA_STR, schemaStr)
		);
		transJsonToColumns.open();

		Row resultJsonToColumns = transJsonToColumns.map(Row.of(jsonStr));

		System.out.println(resultJsonToColumns);
		Assert.assertEquals(
			"1",
			resultJsonToColumns.getField(resultJsonToColumns.getArity() - 7).toString()
		);

		FormatTransMapper transKvToColumns = new FormatTransMapper(
			CsvUtil.schemaStr2Schema("kv string"),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.KV)
				.set(FormatTransParams.TO_FORMAT, FormatType.COLUMNS)
				.set(KvToColumnsParams.KV_COL, "kv")
				.set(KvToColumnsParams.KV_COL_DELIMITER, ",")
				.set(KvToColumnsParams.KV_VAL_DELIMITER, "=")
				.set(KvToColumnsParams.SCHEMA_STR, schemaStr)
		);
		transKvToColumns.open();

		Row resultKvToColumns = transKvToColumns.map(Row.of(kvStr));

		System.out.println(resultKvToColumns);
		Assert.assertEquals(
			"1",
			resultKvToColumns.getField(resultKvToColumns.getArity() - 7).toString()
		);

		FormatTransMapper transKvToCsv = new FormatTransMapper(
			CsvUtil.schemaStr2Schema("kv string"),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.KV)
				.set(FormatTransParams.TO_FORMAT, FormatType.CSV)
				.set(KvToCsvParams.KV_COL, "kv")
				.set(KvToCsvParams.KV_COL_DELIMITER, ",")
				.set(KvToCsvParams.KV_VAL_DELIMITER, "=")
				.set(KvToCsvParams.SCHEMA_STR, schemaStr)
				.set(KvToCsvParams.CSV_COL, "csv")
				.set(KvToCsvParams.CSV_FIELD_DELIMITER, "$")
		);
		transKvToCsv.open();

		Row resultKvToCsv = transKvToCsv.map(Row.of(kvStr));

		System.out.println(resultKvToCsv);
		Assert.assertEquals(csvStr, resultKvToCsv.getField(resultKvToCsv.getArity() - 1));

		FormatTransMapper transCsvToKv = new FormatTransMapper(
			CsvUtil.schemaStr2Schema("csv string"),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.CSV)
				.set(FormatTransParams.TO_FORMAT, FormatType.KV)
				.set(CsvToKvParams.SCHEMA_STR, schemaStr)
				.set(CsvToKvParams.CSV_COL, "csv")
				.set(CsvToKvParams.CSV_FIELD_DELIMITER, "$")
				.set(CsvToKvParams.KV_COL, "kv")
				.set(CsvToKvParams.KV_COL_DELIMITER, ",")
				.set(CsvToKvParams.KV_VAL_DELIMITER, "=")
		);
		transCsvToKv.open();

		Row resultCsvToKv = transCsvToKv.map(Row.of(csvStr));

		System.out.println(resultCsvToKv);
		Assert.assertEquals(
			kvStr.length() + 2,
			resultCsvToKv.getField(resultCsvToKv.getArity() - 1).toString().length()
		);

		FormatTransMapper transKvToJson = new FormatTransMapper(
			CsvUtil.schemaStr2Schema("kv string"),
			new Params()
				.set(FormatTransParams.FROM_FORMAT, FormatType.KV)
				.set(FormatTransParams.TO_FORMAT, FormatType.JSON)
				.set(KvToJsonParams.KV_COL, "kv")
				.set(KvToJsonParams.KV_COL_DELIMITER, ",")
				.set(KvToJsonParams.KV_VAL_DELIMITER, "=")
				.set(KvToJsonParams.JSON_COL, "json")
		);
		transKvToJson.open();

		Row resultKvToJson = transKvToJson.map(Row.of(kvStr));

		System.out.println(resultKvToJson);
		Assert.assertEquals(jsonStr.length(), resultKvToJson.getField(resultKvToJson.getArity() - 1).toString()
			.length());

	}

}