package com.alibaba.alink.operator.common.tree.viz;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;

public class TreeModelVizTest extends AlinkTestBase {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void toImage() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0.8),
				Row.of(1, 2, 0.7),
				Row.of(0, 3, 0.4),
				Row.of(0, 2, 0.4),
				Row.of(1, 3, 0.6),
				Row.of(4, 3, 0.2),
				Row.of(4, 4, 0.3)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		RandomForestTrainBatchOp rfOp = new RandomForestTrainBatchOp()
			.setLabelCol(colNames[2])
			.setFeatureCols(colNames[0], colNames[1])
			.setMaxDepth(10)
			.setNumTrees(3)
			.setMinSamplesPerLeaf(1)
			.setCategoricalCols(colNames[1]);

		rfOp.linkFrom(memSourceBatchOp).lazyCollectModelInfo(treeModelSummary -> {
			try {
				Assert.assertNotNull(treeModelSummary.getCaseWhenRule(0));
				treeModelSummary.saveTreeAsImage(folder.getRoot().toPath() + "tree_model_image.png", 0, true);
				Assert.assertNotNull(treeModelSummary.toString());
			} catch (Exception e) {
				Assert.fail(e.toString());
			}
		});

		BatchOperator.execute();
	}
}