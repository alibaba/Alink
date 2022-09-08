package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo;
import com.alibaba.alink.common.lazy.LazyObjectsManager;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.lazy.WithTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.LocalLazyObjectsManager;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.ModelStreamScanParams;

import java.lang.reflect.ParameterizedType;

import static com.alibaba.alink.common.lazy.HasLazyPrintModelInfo.LAZY_PRINT_MODEL_INFO_ENABLED;
import static com.alibaba.alink.common.lazy.HasLazyPrintModelInfo.LAZY_PRINT_MODEL_INFO_TITLE;
import static com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo.LAZY_PRINT_TRAIN_INFO_ENABLED;
import static com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo.LAZY_PRINT_TRAIN_INFO_TITLE;

/**
 * Abstract class for a trainer that train a machine learning model.
 * <p>
 * The different between {@link EstimatorBase} and {@link Trainer} is that
 * some of {@link EstimatorBase} have its own feature such as some ensemble algorithms,
 * some frequent item set mining algorithms, auto tuning, etc.
 *
 * @param <T> The class type of the {@link Trainer} implementation itself
 * @param <M> class type of the {@link ModelBase} this Trainer produces.
 */
public abstract class Trainer<T extends Trainer <T, M>, M extends MapModel <M>>
	extends EstimatorBase <T, M>
	implements ModelStreamScanParams <T>, HasLazyPrintTransformInfo <T> {

	private static final long serialVersionUID = -3065228676122699535L;

	public Trainer() {
		super();
	}

	public Trainer(Params params) {
		super(params);
	}

	@Override
	public M fit(BatchOperator <?> input) {
		BatchOperator <?> trainer = postProcessTrainOp(train(input));
		M model = createModel(trainer);
		checkModelValidity(model, input);
		return postProcessModel(model);
	}

	@Override
	public M fit(LocalOperator <?> input) {
		LocalOperator <?> trainer = postProcessTrainOp(train(input));
		M model = createModel(trainer);
		checkModelValidity(model, input);
		return postProcessModel(model);
	}

	protected BatchOperator <?> postProcessTrainOp(BatchOperator <?> trainOp) {
		LazyObjectsManager lazyObjectsManager = MLEnvironmentFactory.get(trainOp.getMLEnvironmentId())
			.getLazyObjectsManager();
		lazyObjectsManager.genLazyTrainOp(this).addValue(trainOp);
		if (this instanceof HasLazyPrintTrainInfo) {
			if (get(LAZY_PRINT_TRAIN_INFO_ENABLED)) {
				((WithTrainInfo <?, ?>) trainOp).lazyPrintTrainInfo(get(LAZY_PRINT_TRAIN_INFO_TITLE));
			}
		}
		if (this instanceof HasLazyPrintModelInfo) {
			if (get(LAZY_PRINT_MODEL_INFO_ENABLED)) {
				((WithModelInfoBatchOp <?, ?, ?>) trainOp).lazyPrintModelInfo(get(LAZY_PRINT_MODEL_INFO_TITLE));
			}
		}
		return trainOp;
	}

	protected LocalOperator <?> postProcessTrainOp(LocalOperator <?> trainOp) {
		LocalLazyObjectsManager lazyObjectsManager = LocalMLEnvironment.getInstance().getLazyObjectsManager();
		lazyObjectsManager.genLazyTrainOp(this).addValue(trainOp);
		if (this instanceof HasLazyPrintTrainInfo) {
			if (get(LAZY_PRINT_TRAIN_INFO_ENABLED)) {
				((WithTrainInfo <?, ?>) trainOp).lazyPrintTrainInfo(get(LAZY_PRINT_TRAIN_INFO_TITLE));
			}
		}
		if (this instanceof HasLazyPrintModelInfo) {
			if (get(LAZY_PRINT_MODEL_INFO_ENABLED)) {
				((WithModelInfoBatchOp <?, ?, ?>) trainOp).lazyPrintModelInfo(get(LAZY_PRINT_MODEL_INFO_TITLE));
			}
		}
		return trainOp;
	}

	protected void checkModelValidity(M model, BatchOperator <?> input) {
		if (model instanceof MapModel) {
			((MapModel <?>) model).validate(model.getModelData().getSchema(), input.getSchema());
		}
	}

	protected void checkModelValidity(M model, LocalOperator <?> input) {
		if (model instanceof MapModel) {
			((MapModel <?>) model).validate(model.getModelDataLocal().getSchema(), input.getSchema());
		}
	}

	protected M postProcessModel(M model) {
		LazyObjectsManager lazyObjectsManager = MLEnvironmentFactory.get(model.getMLEnvironmentId())
			.getLazyObjectsManager();
		lazyObjectsManager.genLazyModel(this).addValue(model);
		if (this instanceof HasLazyPrintTransformInfo) {
			if (get(LAZY_PRINT_TRANSFORM_DATA_ENABLED)) {
				model.enableLazyPrintTransformData(
					get(LAZY_PRINT_TRANSFORM_DATA_NUM),
					get(LAZY_PRINT_TRANSFORM_DATA_TITLE));
			}
			if (get(LAZY_PRINT_TRANSFORM_STAT_ENABLED)) {
				model.enableLazyPrintTransformStat(get(LAZY_PRINT_TRANSFORM_STAT_TITLE));
			}
		}
		return model;
	}

	@Override
	public M fit(StreamOperator <?> input) {
		throw new AkUnsupportedOperationException("Only support batch fit!");
	}

	private M createModel(BatchOperator <?> model) {
		try {
			ParameterizedType pt =
				(ParameterizedType) this.getClass().getGenericSuperclass();

			Class <M> classM = (Class <M>) pt.getActualTypeArguments()[1];

			return (M) classM.getConstructor(Params.class)
				.newInstance(getParams())
				.setModelData(model);

		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}
	}

	private M createModel(LocalOperator <?> model) {
		try {
			ParameterizedType pt =
				(ParameterizedType) this.getClass().getGenericSuperclass();

			Class <M> classM = (Class <M>) pt.getActualTypeArguments()[1];

			return (M) classM.getConstructor(Params.class)
				.newInstance(getParams())
				.setModelData(model);

		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}
	}

	protected abstract BatchOperator <?> train(BatchOperator <?> in);

	protected StreamOperator <?> train(StreamOperator <?> in) {
		throw new AkUnsupportedOperationException("Only support batch fit!");
	}

	protected LocalOperator <?> train(LocalOperator <?> in) {
		throw new AkUnsupportedOperationException("Not supported yet!");
	}
}
