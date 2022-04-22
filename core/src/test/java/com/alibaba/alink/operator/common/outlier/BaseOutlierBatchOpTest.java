package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.outlier.HasMaxSampleNumPerGroup;
import com.alibaba.alink.params.outlier.OutlierParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BaseOutlierBatchOpTest extends AlinkTestBase {

	@Test
	public void testGroup2MTables() {
		Params params1 = new Params()
			.set(HasMaxSampleNumPerGroup.MAX_SAMPLE_NUM_PER_GROUP, 3)
			.set(OutlierParams.GROUP_COLS, new String[] {"f0"});

		BatchOperator <?> out = BaseOutlierBatchOp.group2MTables(getSource(), params1);

		Assert.assertEquals(9, out.collect().size());
	}

	@Test
	public void testGroup2MTables2() throws Exception{
		Params params1 = new Params()
			.set(HasMaxSampleNumPerGroup.MAX_SAMPLE_NUM_PER_GROUP, 3);

		BatchOperator <?> out = BaseOutlierBatchOp.group2MTables(getSource(), params1);

		Assert.assertEquals(7, out.collect().size());
	}

	@Test
	public void testGroup2MTables3() {
		Params params1 = new Params();

		BatchOperator <?> out = BaseOutlierBatchOp.group2MTables(getSource(), params1);

		Assert.assertEquals(1, out.collect().size());
	}

	@Test
	public void testGroup2MTable4() {
		Params params1 = new Params()
			.set(OutlierParams.GROUP_COLS, new String[] {"f0"});

		BatchOperator <?> out = BaseOutlierBatchOp.group2MTables(getSource(), params1);

		Assert.assertEquals(5, out.collect().size());
	}

	private BatchOperator <?> getSource() {
		List <Row> testArray =
			Arrays.asList(
				Row.of("a", 1, 2.0),// a 2
				Row.of("a", 2, -3.0),
				Row.of("a", 2, 4.0),
				Row.of("a", 2, 5.0),
				Row.of("a", 2, 6.0),
				Row.of("a", 2, 7.0),
				Row.of("b", 2, 5.6), // b 1
				Row.of("b", 2, 6.1),
				Row.of("c", 2, 6.0), // c 1
				Row.of("d", 2, 6.0), //d 2
				Row.of("d", 2, 6.0),
				Row.of("d", 2, 6.0),
				Row.of("d", 2, 6.0),
				Row.of("e", 1, 2.0), //e 3
				Row.of("e", 2, -3.0),
				Row.of("e", 2, 4.0),
				Row.of("e", 2, 5.0),
				Row.of("e", 2, 6.0),
				Row.of("e", 2, 7.0),
				Row.of("e", 1, 8.0)
			);

		String[] colNames = new String[] {"f0", "f1", "f2"};

		return new MemSourceBatchOp(testArray, colNames);

	}

}