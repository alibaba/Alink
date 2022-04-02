package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorType;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

public class ToVectorTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		final String vecStr = "1 0 3 4";
		final DenseVector expect = VectorUtil.parseDense("1 0 3 4");
		final SparseVector expect1 = VectorUtil.parseSparse("$4$0:1 2:3 3:4");

		Row[] rows = new Row[] {
			Row.of(vecStr)
		};

		MemSourceBatchOp data = new MemSourceBatchOp(
			rows, new String[] {"vec"}
		);

		new ToVector().setSelectedCol("vec").setVectorType(VectorType.DENSE).transform(data)
			.lazyCollect(rows1 -> Assert.assertEquals(expect, rows1.get(0).getField(0)));
		new ToVector().setSelectedCol("vec").setVectorType(VectorType.SPARSE).transform(data)
			.lazyCollect(rows1 -> Assert.assertEquals(expect1, rows1.get(0).getField(0)));

		BatchOperator.execute();
	}
}