package com.alibaba.alink.operator.batch.dataproc.vector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorElementwiseProductStreamOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorEleWiseProdTest;
import com.alibaba.alink.pipeline.dataproc.vector.VectorElementwiseProduct;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class VectorElementwiseProductTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator batchData = (BatchOperator) VectorEleWiseProdTest.getData(true);
		StreamOperator streamData = (StreamOperator) VectorEleWiseProdTest.getData(false);
		VectorElementwiseProduct model = new VectorElementwiseProduct()
			.setSelectedCol("c1").setScalingVector("3.0 2.0 3.0")
			.setOutputCol("product_result");
		model.transform(batchData).lazyCollect();
		model.transform(streamData).print();

		new VectorElementwiseProductBatchOp()
			.setSelectedCol("c1").setScalingVector("3.0 2.0 3.0")
			.setOutputCol("product_result")
			.linkFrom(batchData).collect();
		new VectorElementwiseProductStreamOp()
			.setSelectedCol("c1").setScalingVector("3.0 2.0 3.0")
			.setOutputCol("product_result")
			.linkFrom(streamData).print();
		StreamOperator.execute();
	}

}
