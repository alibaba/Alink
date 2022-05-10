package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class ToMTableTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		final String mTableStr = "{\"data\":{\"col0\":[1],\"col1\":[\"2\"],\"label\":[0],\"ts\":[\"2603-10-12 04:13:52"
			+ ".012\"],\"d_vec\":[null],\"s_vec\":[\"$3$1:2.0\"],\"tensor\":[\"FLOAT#1#3.0 \"]},\"schema\":\"col0 INT,"
			+ "col1 VARCHAR,label INT,ts TIMESTAMP,d_vec DENSE_VECTOR,s_vec VECTOR,tensor FLOAT_TENSOR\"}";
		final MTable expect = MTable.fromJson(mTableStr);

		Row[] rows = new Row[] {
			Row.of(mTableStr)
		};

		MemSourceBatchOp data = new MemSourceBatchOp(
			rows, new String[] {"m_table"}
		);

		new ToMTable().setSelectedCol("m_table").transform(data)
			.lazyCollect(rows1 -> Assert.assertEquals(expect.toString(), rows1.get(0).getField(0).toString()));

		BatchOperator.execute();
	}
}