package com.alibaba.alink.common.lazy;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.Trainer;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.tuning.BaseTuning;
import com.alibaba.alink.pipeline.tuning.Report;

import java.util.List;
import java.util.function.Consumer;

/**
 * Provides utility to add callbacks for lazyCollect of ModelInfo, TrainInfo, TransformData and TransformStat.
 * <p>
 * Mainly for PyAlink, as callbacks are not supported in {@link HasLazyPrintModelInfo}, {@link HasLazyPrintTrainInfo},
 * and {@link HasLazyPrintTransformInfo}.
 */
public class PipelineLazyCallbackUtils {

	@SuppressWarnings("unchecked")
	public static <S> void callbackForTrainerLazyTrainInfo(Trainer <?, ?> trainer, List <Consumer <S>> callbacks) {
		LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(trainer);
		LazyEvaluation <BatchOperator <?>> lazyTrainOp = lazyObjectsManager.genLazyTrainOp(trainer);
		lazyTrainOp.addCallback(d -> {
			((WithTrainInfo <S, ?>) d).lazyCollectTrainInfo(callbacks);
		});
	}

	@SuppressWarnings("unchecked")
	public static <S> void callbackForTrainerLazyModelInfo(Trainer <?, ?> trainer, List <Consumer <S>> callbacks) {
		LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(trainer);
		LazyEvaluation <BatchOperator <?>> lazyTrainOp = lazyObjectsManager.genLazyTrainOp(trainer);
		lazyTrainOp.addCallback(d -> {
			((WithModelInfoBatchOp <S, ?, ?>) d).lazyCollectModelInfo(callbacks);
		});
	}

	public static void callbackForTrainerLazyTransformResult(Trainer <?, ?> trainer,
															 List <Consumer <BatchOperator <?>>> callbacks) {
		LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(trainer);
		LazyEvaluation <ModelBase <?>> lazyModel = lazyObjectsManager.genLazyModel(trainer);
		lazyModel.addCallback(model -> {
			LazyEvaluation <BatchOperator <?>> lazyTransformResult = lazyObjectsManager.genLazyTransformResult(model);
			lazyTransformResult.addCallbacks(callbacks);
		});
	}

	public static void callbackForTransformerLazyTransformResult(TransformerBase <?> transformer,
																 List <Consumer <BatchOperator <?>>> callbacks) {
		MLEnvironment mlEnv = MLEnvironmentFactory.get(transformer.getMLEnvironmentId());
		LazyObjectsManager lazyObjectsManager = mlEnv.getLazyObjectsManager();
		LazyEvaluation <BatchOperator <?>> lazyTransformResult = lazyObjectsManager.genLazyTransformResult
			(transformer);
		lazyTransformResult.addCallbacks(callbacks);
	}

	public static void callbackForTuningLazyReport(BaseTuning <?, ?> tuning,
												   List <Consumer <Report>> callbacks) {
		MLEnvironment mlEnv = MLEnvironmentFactory.get(tuning.getMLEnvironmentId());
		LazyObjectsManager lazyObjectsManager = mlEnv.getLazyObjectsManager();
		LazyEvaluation <Report> lazyReport = lazyObjectsManager.genLazyReport(tuning);
		lazyReport.addCallbacks(callbacks);
	}
}
