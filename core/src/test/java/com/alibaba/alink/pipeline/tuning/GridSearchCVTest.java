package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningBinaryClassMetric;
import com.alibaba.alink.operator.common.evaluation.TuningClusterMetric;
import com.alibaba.alink.operator.common.evaluation.TuningMultiClassMetric;
import com.alibaba.alink.operator.common.evaluation.TuningRegressionMetric;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.classification.RandomForestClassifier;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.dataproc.format.ColumnsToVector;
import com.alibaba.alink.pipeline.regression.GbdtRegressor;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType.COSINE;
import static com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType.EUCLIDEAN;

public class GridSearchCVTest extends AlinkTestBase {
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
	public void testSplit() throws Exception {
		List <Row> rows = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1),
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1),
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1));

		String[] colNames = new String[] {"f0", "f1", "f2", "f3", "label"};
		MemSourceBatchOp data = new MemSourceBatchOp(rows, colNames);
		String[] featureColNames = new String[] {colNames[0], colNames[1], colNames[2], colNames[3]};
		String[] categoricalColNames = new String[] {colNames[1]};
		String labelColName = colNames[4];

		RandomForestClassifier rf = new RandomForestClassifier()
			.setFeatureCols(featureColNames)
			.setCategoricalCols(categoricalColNames)
			.setLabelCol(labelColName)
			.setPredictionCol("pred_result")
			.setPredictionDetailCol("pred_detail")
			.setSubsamplingRatio(1.0);

		Pipeline pipeline = new Pipeline(rf);

		ParamGrid paramGrid = new ParamGrid()
			.addGrid(rf, "SUBSAMPLING_RATIO", new Double[] {1.0})
			.addGrid(rf, "NUM_TREES", new Integer[] {3});

		BinaryClassificationTuningEvaluator tuning_evaluator = new BinaryClassificationTuningEvaluator()
			.setLabelCol(labelColName)
			.setPredictionDetailCol("pred_detail")
			.setTuningBinaryClassMetric("Accuracy");

		GridSearchTVSplit cv = new GridSearchTVSplit()
			.setEstimator(pipeline)
			.setParamGrid(paramGrid)
			.setTuningEvaluator(tuning_evaluator)
			.setTrainRatio(0.8);

		ModelBase cvModel = cv.fit(data);
		cvModel.transform(data).print();
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
			.addGrid(gbdtClassifier, GbdtClassifier.NUM_TREES, new Integer[] {1, 2})
			.addGrid(gbdtClassifier, GbdtClassifier.MAX_DEPTH, new Integer[] {3, -1});

		GridSearchCV gridSearchCV = new GridSearchCV()
			.setEstimator(gbdtClassifier)
			.setParamGrid(grid)
			.setNumFolds(2)
			.enableLazyPrintTrainInfo()
			.setTuningEvaluator(
				new BinaryClassificationTuningEvaluator()
					.setTuningBinaryClassMetric(TuningBinaryClassMetric.ACCURACY)
					.setLabelCol(colNames[2])
					.setPositiveLabelValueString("1")
					.setPredictionDetailCol("pred_detail")
			);

		GridSearchCVModel model = gridSearchCV.fit(memSourceBatchOp);

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

		GridSearchCV gridSearchCV = new GridSearchCV()
			.setEstimator(gbdtClassifier)
			.setParamGrid(grid)
			.setNumFolds(2)
			.setTuningEvaluator(
				new MultiClassClassificationTuningEvaluator()
					.setTuningMultiClassMetric(TuningMultiClassMetric.ACCURACY)
					.setLabelCol(colNames[2])
					.setPredictionDetailCol("pred_detail")
			);

		GridSearchCVModel model = gridSearchCV.fit(memSourceBatchOp);

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

		GridSearchCV gridSearchCV = new GridSearchCV()
			.setEstimator(gbdtClassifier)
			.setParamGrid(grid)
			.setNumFolds(2)
			.setTuningEvaluator(
				new RegressionTuningEvaluator()
					.setTuningRegressionMetric(TuningRegressionMetric.RMSE)
					.setLabelCol(colNames[2])
					.setPredictionCol("pred")
			);

		GridSearchCVModel model = gridSearchCV.fit(memSourceBatchOp);

		Assert.assertEquals(
			testArray.length,
			model.transform(memSourceBatchOp).collect().size()
		);
	}

	@Test
	public void findBestCluster() {
		ColumnsToVector columnsToVector = new ColumnsToVector()
			.setSelectedCols(colNames[0], colNames[1])
			.setVectorCol("vector");

		KMeans kMeans = new KMeans()
			.setVectorCol("vector")
			.setPredictionCol("pred");

		ParamGrid grid = new ParamGrid()
			.addGrid(kMeans, KMeans.DISTANCE_TYPE, new HasKMeansDistanceType.DistanceType[] {EUCLIDEAN, COSINE});

		Pipeline pipeline = new Pipeline()
			.add(columnsToVector)
			.add(kMeans);

		GridSearchCV gridSearchCV = new GridSearchCV()
			.setEstimator(pipeline)
			.setParamGrid(grid)
			.setNumFolds(2)
			.setTuningEvaluator(
				new ClusterTuningEvaluator()
					.setTuningClusterMetric(TuningClusterMetric.RI)
					.setPredictionCol("pred")
					.setVectorCol("vector")
					.setLabelCol("label")
			);

		GridSearchCVModel model = gridSearchCV.fit(memSourceBatchOp);

		Assert.assertEquals(
			testArray.length,
			model.transform(memSourceBatchOp).collect().size()
		);
	}
}