package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorPolynomialExpandStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorPolynomialExpand;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class VectorPolynomialExpandTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of("3.0, 4.0")
		};
		BatchOperator batchData = new MemSourceBatchOp(rows, new String[] {"vec"});
		StreamOperator streamData = new MemSourceStreamOp(rows, new String[] {"vec"});
		new VectorPolynomialExpandBatchOp()
			.setDegree(2)
			.setOutputCol("outv")
			.setSelectedCol("vec")
			.linkFrom(batchData);
		new VectorPolynomialExpandStreamOp()
			.setDegree(2)
			.setOutputCol("outv")
			.setSelectedCol("vec")
			.linkFrom(streamData);
		VectorPolynomialExpand pipeline = new VectorPolynomialExpand()
			.setDegree(2)
			.setOutputCol("outv")
			.setSelectedCol("vec");
		pipeline.transform(batchData).collect();
		pipeline.transform(streamData).print();
		StreamOperator.execute();
	}
}
