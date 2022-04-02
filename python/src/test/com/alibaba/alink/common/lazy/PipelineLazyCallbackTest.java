package com.alibaba.alink.common.lazy;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.fake_lazy_operators.FakeModel;
import com.alibaba.alink.common.lazy.fake_lazy_operators.FakeTrainer;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningMultiClassMetric;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.tuning.GridSearchCV;
import com.alibaba.alink.pipeline.tuning.GridSearchCVModel;
import com.alibaba.alink.pipeline.tuning.MultiClassClassificationTuningEvaluator;
import com.alibaba.alink.pipeline.tuning.ParamGrid;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PipelineLazyCallbackTest extends BaseLazyTest {

	@Test
	public void testTrainer() throws Exception {
		MemSourceBatchOp source
			= new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[] {"label", "u", "i", "r"});

		FakeTrainer trainer = new FakeTrainer()
			.enableLazyPrintTrainInfo()
			.enableLazyPrintModelInfo()
			.enableLazyPrintTransformData(1)
			.enableLazyPrintTransformStat();

		PipelineLazyCallbackUtils.callbackForTrainerLazyModelInfo(trainer, Arrays.asList(d -> {
			System.out.println("===== MODEL INFO CALLBACK =====");
			System.out.println(d);
		}));

		PipelineLazyCallbackUtils.callbackForTrainerLazyTrainInfo(trainer, Arrays.asList(d -> {
			System.out.println("===== TRAIN INFO CALLBACK =====");
			System.out.println(d);
		}));

		PipelineLazyCallbackUtils.callbackForTrainerLazyTransformResult(trainer, Arrays.asList(d -> {
			d.lazyPrint(5, "===== TRAINER TRANSFORM DATA CALLBACK =====");
		}, d -> {
			d.lazyPrintStatistics("===== TRAINER TRANSFORM STAT CALLBACK =====");
		}));

		FakeModel model = trainer.fit(source);

		BatchOperator output = model.transform(source);
		output.firstN(5).print();

		String content = outContent.toString();
		Assert.assertTrue(content.contains("===== MODEL INFO CALLBACK ====="));
		Assert.assertTrue(content.contains("===== TRAIN INFO CALLBACK ====="));
		Assert.assertTrue(content.contains("===== TRAINER TRANSFORM DATA CALLBACK ====="));
		Assert.assertTrue(content.contains("===== TRAINER TRANSFORM STAT CALLBACK ====="));
	}

	@Test
	public void testTransformer() throws Exception {
		MemSourceBatchOp source
			= new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[] {"label", "u", "i", "r"});

		FakeTrainer trainer = new FakeTrainer();
		FakeModel model = trainer.fit(source)
			.enableLazyPrintTransformData(1)
			.enableLazyPrintTransformStat();

		PipelineLazyCallbackUtils.callbackForTransformerLazyTransformResult(model, Arrays.asList(d -> {
			d.lazyPrint(5, "===== TRANSFORM DATA CALLBACK =====");
		}, d -> {
			d.lazyPrintStatistics("===== TRANSFORM STAT CALLBACK =====");
		}));

		BatchOperator output = model.transform(source);
		output.firstN(5).print();

		String content = outContent.toString();
		Assert.assertTrue(content.contains("===== TRANSFORM DATA CALLBACK ====="));
		Assert.assertTrue(content.contains("===== TRANSFORM STAT CALLBACK ====="));
	}

	@Test
	public void testTuning() throws Exception {
		Row[] testArray =
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

		String[] colNames = new String[] {"col0", "col1", "label"};
		BatchOperator<?> memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

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
			)
			.enableLazyPrintTrainInfo();
		PipelineLazyCallbackUtils.callbackForTuningLazyReport(gridSearchCV, Arrays.asList(
			d -> {
				System.out.println("===== TUNING TRAIN INFO CALLBACK =====");
				System.out.println(d.toString());
			}
		));

		GridSearchCVModel model = gridSearchCV.fit(memSourceBatchOp);

		String content = outContent.toString();
		Assert.assertTrue(content.contains("===== TUNING TRAIN INFO CALLBACK ====="));
	}
}
