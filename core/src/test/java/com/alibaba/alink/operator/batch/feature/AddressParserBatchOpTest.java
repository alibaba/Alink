package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.JsonToColumnsBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dataproc.format.HasHandleInvalidDefaultAsError.HandleInvalid;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class AddressParserBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("1", "成都市高新区天府软件园B区科技大楼"),
				Row.of("2", "双流县郑通路社保局区52050号"),
				Row.of("3", "city_walk")
			};

		String[] colNames = new String[] {"id", "address"};

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		AddressParserBatchOp parser = new AddressParserBatchOp()
			.setSelectedCol("address")
			.setOutputCol("address_parse");

		JsonToColumnsBatchOp jsonToColumns = new JsonToColumnsBatchOp()
			.setJsonCol("address_parse")
			.setSchemaStr("prov string, city string, district string, street string")
			.setHandleInvalid(HandleInvalid.SKIP);

		String selectSql = "*, "
			+ "case "
			+ "		when prov is not null and city is not null and district is not null then true "
			+ "		else false "
			+ "end  is_address,"
			+ "case "
			+ "		when prov is not null then true "
			+ "		else false "
			+ "end  is_prov";

		BatchOperator <?> result = data
			.link(parser)
			.link(jsonToColumns)
			.select(selectSql);

		result.getOutputTable().printSchema();

		result.print();
	}

}