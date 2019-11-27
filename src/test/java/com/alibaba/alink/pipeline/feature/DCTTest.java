package com.alibaba.alink.pipeline.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class DCTTest {

	@Test
	public void test() throws Exception {
		Random generator = new Random(1234);

		int data_num = 10;
		int col_num = 31;
		Row[] data = new Row[data_num];
		for (int index = 0; index < data_num; index++) {
			double[] cur_double = new double[col_num];
			for (int index2 = 0; index2 < col_num; index2++) {
				cur_double[index2] = ((int) (generator.nextDouble() * 512) - 256) * 1.0;
			}
			data[index] = Row.of(VectorUtil.toString(new DenseVector(cur_double)));
		}

		String[] colNames = new String[] {"feature"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(data), colNames);

		DCT dct = new DCT()
			.setSelectedCol("feature")
			.setOutputCol("result")
			.setInverse(false);

		Assert.assertEquals(dct.transform(memSourceBatchOp).count(), data_num);
	}

}