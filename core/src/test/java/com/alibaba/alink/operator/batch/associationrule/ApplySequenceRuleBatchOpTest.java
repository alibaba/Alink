package com.alibaba.alink.operator.batch.associationrule;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class ApplySequenceRuleBatchOpTest extends AlinkTestBase {
	@Test
	public void testPrefixSpan() throws Exception {
		Row[] rows = new Row[] {
			Row.of("a;a,b,c;a,c;d;c,f"),
			Row.of("a,d;c;b,c;a,e"),
			Row.of("e,f;a,b;d,f;c;b"),
			Row.of("e;g;a,f;c;b;c"),
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"sequence"});

		PrefixSpanBatchOp prefixSpan = new PrefixSpanBatchOp()
			.setItemsCol("sequence")
			.setMinSupportCount(3);

		prefixSpan.linkFrom(BatchOperator.fromTable(data));
		Assert.assertEquals(prefixSpan.count(), 14);
		Assert.assertEquals(prefixSpan.getSideOutputAssociationRules().count(), 8);

		ApplySequenceRuleBatchOp op = new ApplySequenceRuleBatchOp()
			.setSelectedCol("sequence")
			.setOutputCol("result")
			.linkFrom(prefixSpan.getSideOutputAssociationRules(), BatchOperator.fromTable(data));
		Assert.assertEquals(op.count(), 4);
	}

}