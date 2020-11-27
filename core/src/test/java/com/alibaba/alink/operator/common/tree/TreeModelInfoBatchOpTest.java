package com.alibaba.alink.operator.common.tree;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TreeModelInfoBatchOpTest extends AlinkTestBase {

	@Test
	public void createModelInfo() {
		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			new ArrayList <>(
				Arrays.asList(
					Row.of(0.1, "1", 0),
					Row.of(0.2, "2", 1),
					Row.of(null, "3", 0)
				)
			),
			new String[] {"f0", "f1", "label"}
		);

		new RandomForestTrainBatchOp()
			.setNumTrees(1)
			.setFeatureCols("f0", "f1")
			.setCategoricalCols("f1")
			.setLabelCol("label")
			.linkFrom(memSourceBatchOp)
			.lazyCollectModelInfo(
				treeModelSummary -> {
					assertArrayEquals(new String[] {"f0", "f1"}, treeModelSummary.getFeatures());
					assertArrayEquals(new String[] {"f1"}, treeModelSummary.getCategoricalFeatures());
					assertEquals(3, treeModelSummary.getCategoricalValues("f1").size());
					assertEquals(1, treeModelSummary.getNumTrees());
					assertArrayEquals(new Object[] {0, 1}, treeModelSummary.getLabels());
					assertEquals(2, treeModelSummary.getFeatureImportance().size());
					assertTrue(treeModelSummary.getCaseWhenRuleFromTreeId(0).length() != 0);
					assertTrue(treeModelSummary.toString().length() != 0);
				}
			)
			.collect();
	}
}