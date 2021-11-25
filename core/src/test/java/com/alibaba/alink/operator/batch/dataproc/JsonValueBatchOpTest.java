package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.dataproc.JsonValue;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JsonValueBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("{\"ak\":'asdfa',\"ck\":2}"),
			};
		String[] colnames = new String[] {"jsoncol"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
		List <Row> result = inOp.link(new JsonValueBatchOp()
			.setSkipFailed(true)
			.setSelectedCol("jsoncol").setOutputCols(new String[] {"jsonval", "jv"})
			.setJsonPath("$.ak", "$.ck")).collect();

		Assert.assertEquals(result.get(0).getField(1).toString(), "asdfa");
		Assert.assertEquals(result.get(0).getField(2).toString(), "2");
	}

	@Test
	public void test1() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("{\"ak\":1,\"dd\":1}"),
			};
		String[] colnames = new String[] {"jsoncol"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
		List <Row> result = inOp.link(new JsonValueBatchOp()
			.setSkipFailed(true).setOutputColTypes(new String[] {"string", "string"})
			.setSelectedCol("jsoncol").setOutputCols(new String[] {"jsonval", "jv"})
			.setJsonPath("$.ak", "$.ck")).collect();
		Assert.assertNull(result.get(0).getField(2));
	}

	@Test
	public void test3() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("{\"ak\":1,\"dd\":1}"),
			};
		String[] colnames = new String[] {"jsoncol"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
		List <Row> result = (new Pipeline().add(new JsonValue()
			.setSkipFailed(true).setOutputColTypes(new String[] {"string", "string"})
			.setSelectedCol("jsoncol").setOutputCols(new String[] {"jsonval", "jv"})
			.setJsonPath("$.ak", "$.ck"))).fit(inOp).transform(inOp).collect();
		Assert.assertNull(result.get(0).getField(2));
	}
}