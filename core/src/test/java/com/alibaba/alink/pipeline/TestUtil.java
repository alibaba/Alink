package com.alibaba.alink.pipeline;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;

import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;

public class TestUtil extends AlinkTestBase {
	public static void printTable(Table table) throws Exception {
		TableImpl tableImpl = (TableImpl) table;
		if (tableImpl.getTableEnvironment() instanceof StreamTableEnvironment) {
			new TableSourceStreamOp(table).print();
			StreamOperator.execute();
		} else {
			new TableSourceBatchOp(table).print();
		}
	}
}
