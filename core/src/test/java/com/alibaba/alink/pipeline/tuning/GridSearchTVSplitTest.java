package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningBinaryClassMetric;
import com.alibaba.alink.operator.common.evaluation.TuningClusterMetric;
import com.alibaba.alink.operator.common.evaluation.TuningMultiClassMetric;
import com.alibaba.alink.operator.common.evaluation.TuningRegressionMetric;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.dataproc.format.ColumnsToVector;
import com.alibaba.alink.pipeline.regression.GbdtRegressor;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType.COSINE;
import static com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType.EUCLIDEAN;

public class GridSearchTVSplitTest extends AlinkTestBase {
	private Row[] testArray;
	private MemSourceBatchOp memSourceBatchOp;
	private String[] colNames;

	@Before
	public void setUp() throws Exception {
		testArray =
			new Row[] {
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1),
				Row.of(5, 3, 0),
				Row.of(5, 4, 0),
				Row.of(5, 2, 1)
			};

		colNames = new String[] {"col0", "col1", "label"};
		memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);
	}

	@Test
	public void findBest() {
		GbdtClassifier gbdtClassifier = new GbdtClassifier()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMinSamplesPerLeaf(1)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		ParamGrid grid = new ParamGrid()
			.addGrid(gbdtClassifier, GbdtClassifier.NUM_TREES, new Integer[] {1, 2});

		GridSearchTVSplit gridSearchTVSplit = new GridSearchTVSplit()
			.setEstimator(gbdtClassifier)
			.setParamGrid(grid)
			.setTuningEvaluator(
				new BinaryClassificationTuningEvaluator()
					.setTuningBinaryClassMetric(TuningBinaryClassMetric.ACCURACY)
					.setLabelCol(colNames[2])
					.setPositiveLabelValueString("1")
					.setPredictionDetailCol("pred_detail")
			);

		GridSearchTVSplitModel model = gridSearchTVSplit.fit(memSourceBatchOp);

		Assert.assertEquals(
			testArray.length,
			model.transform(memSourceBatchOp).collect().size()
		);
	}

	@Test
	public void findBestMulti() {
		GbdtClassifier gbdtClassifier = new GbdtClassifier()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMinSamplesPerLeaf(1)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		ParamGrid grid = new ParamGrid()
			.addGrid(gbdtClassifier, GbdtClassifier.NUM_TREES, new Integer[] {1, 2});

		GridSearchTVSplit gridSearchTVSplit = new GridSearchTVSplit()
			.setEstimator(gbdtClassifier)
			.setParamGrid(grid)
			.setTuningEvaluator(
				new MultiClassClassificationTuningEvaluator()
					.setTuningMultiClassMetric(TuningMultiClassMetric.ACCURACY)
					.setLabelCol(colNames[2])
					.setPredictionDetailCol("pred_detail")
			);

		GridSearchTVSplitModel model = gridSearchTVSplit.fit(memSourceBatchOp);

		Assert.assertEquals(
			testArray.length,
			model.transform(memSourceBatchOp).collect().size()
		);
	}

	@Test
	public void findBestReg() {
		GbdtRegressor gbdtClassifier = new GbdtRegressor()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMinSamplesPerLeaf(1)
			.setPredictionCol("pred");

		ParamGrid grid = new ParamGrid()
			.addGrid(gbdtClassifier, GbdtClassifier.NUM_TREES, new Integer[] {1, 2});

		GridSearchTVSplit gridSearchTVSplit = new GridSearchTVSplit()
			.setEstimator(gbdtClassifier)
			.setParamGrid(grid)
			.setTuningEvaluator(
				new RegressionTuningEvaluator()
					.setTuningRegressionMetric(TuningRegressionMetric.RMSE)
					.setLabelCol(colNames[2])
					.setPredictionCol("pred")
			);

		GridSearchTVSplitModel model = gridSearchTVSplit.fit(memSourceBatchOp);

		Assert.assertEquals(
			testArray.length,
			model.transform(memSourceBatchOp).collect().size()
		);
	}

	@Test
	public void findBestCluster() throws Exception {
		ColumnsToVector columnsToVector = new ColumnsToVector()
			.setSelectedCols(colNames[0], colNames[1])
			.setVectorCol("vector");

		KMeans kMeans = new KMeans()
			.setVectorCol("vector")
			.setPredictionCol("pred");

		ParamGrid grid = new ParamGrid()
			.addGrid(kMeans, "distanceType", new HasKMeansDistanceType.DistanceType[] {EUCLIDEAN, COSINE});

		Pipeline pipeline = new Pipeline()
			.add(columnsToVector)
			.add(kMeans);

		GridSearchTVSplit gridSearchTVSplit = new GridSearchTVSplit()
			.setEstimator(pipeline)
			.setParamGrid(grid)
			.setTrainRatio(0.5)
			.setTuningEvaluator(
				new ClusterTuningEvaluator()
					.setTuningClusterMetric(TuningClusterMetric.RI)
					.setPredictionCol("pred")
					.setVectorCol("vector")
					.setLabelCol("label")
			);

		GridSearchTVSplitModel model = gridSearchTVSplit.fit(memSourceBatchOp);

		Assert.assertEquals(
			testArray.length,
			model.transform(memSourceBatchOp).collect().size()
		);
	}
}