//
// Copyright (c) 2014, Alibaba Inc.
// All rights reserved.
//
// Author: Yan Huang <allison.hy@alibaba-inc.com>
// Created: 4/8/18
// Description:
//

package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RankingListBatchOpTest extends AlinkTestBase {

	@Test
	public void test3() {
		TableSchema schema = new TableSchema(
			new String[] {"col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"},
			new TypeInformation <?>[] {AlinkTypes.STRING, AlinkTypes.STRING, AlinkTypes.INT, AlinkTypes.DOUBLE,
				AlinkTypes.BOOLEAN, AlinkTypes.INT, AlinkTypes.DOUBLE, AlinkTypes.DOUBLE, AlinkTypes.STRING}
		);
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("a", "a", 1, 1.1, true, 2, 2.1, 2.0, "a"));
		rows.add(Row.of("a", "a", 1, 1.1, true, 3, 3.1, null, null));

		MemSourceBatchOp source = new MemSourceBatchOp(rows, schema);

		RankingListBatchOp rankingList = new RankingListBatchOp()
			.setGroupCol("col2")
			.setGroupValues(new String[] {"a", "b", "c", "d"})
			.setObjectCol("col1")
			.setStatCol("col3")
			.setStatType("sum")
			.setTopN(3)
			.setIsDescending(true)
			.setAddedCols(new String[] {"col6", "col7", "col8", "col9"})
			.setAddedStatTypes(new String[] {"min", "max", "counttotal", "count"});

		BatchOperator op = source.link(rankingList);
		op.collect();
	}
}
