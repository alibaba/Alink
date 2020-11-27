package com.alibaba.alink.operator.common.fm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.linear.FeatureLabelUtil;
import com.alibaba.alink.operator.common.optim.FmOptimizer;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;

import java.util.List;

/**
 * Fm mapper maps one sample to a sample with a predicted label.
 */
public class FmModelMapper extends RichModelMapper {

	private static final long serialVersionUID = -6348182372481296494L;
	private int vectorColIndex = -1;
	private int[] featIdx = null;
	private int featLen = -1;
	private FmModelData model;
	private int[] dim;

	public FmModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		if (null != params) {
			String vectorColName = params.get(SoftmaxPredictParams.VECTOR_COL);
			if (null != vectorColName && vectorColName.length() != 0) {
				this.vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), vectorColName);
			}
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		FmModelDataConverter fmModelDataConverter
			= new FmModelDataConverter(FmModelDataConverter.extractLabelType(super.getModelSchema()));
		this.model = fmModelDataConverter.load(modelRows);
		this.dim = model.dim;

		if (vectorColIndex == -1) {
			TableSchema dataSchema = getDataSchema();
			if (this.model.featureColNames != null) {
				this.featIdx = new int[this.model.featureColNames.length];
				featLen = featIdx.length;
				String[] predictTableColNames = dataSchema.getFieldNames();
				for (int i = 0; i < this.featIdx.length; i++) {
					this.featIdx[i] = TableUtil.findColIndexWithAssert(predictTableColNames,
						this.model.featureColNames[i]);
				}
			} else {
				vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), model.vectorColName);
			}
		}
	}

	@Override
	protected Object predictResult(Row row) throws Exception {

		Vector vec = FeatureLabelUtil.getFeatureVector(row, false, featLen,
			this.featIdx, this.vectorColIndex, model.vectorSize);
		double y = FmOptimizer.calcY(vec, model.fmModel, dim).f0;
		;

		if (model.task.equals(Task.REGRESSION)) {
			return y;
		} else if (model.task.equals(Task.BINARY_CLASSIFICATION)) {
			y = logit(y);
			return (y <= 0.5 ? model.labelValues[1] : model.labelValues[0]);
		} else {
			throw new RuntimeException("task not support yet");
		}
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(Row row) throws Exception {
		Vector vec = FeatureLabelUtil.getFeatureVector(row, false, featLen,
			featIdx, this.vectorColIndex, model.vectorSize);
		double y = FmOptimizer.calcY(vec, model.fmModel, dim).f0;

		if (model.task.equals(Task.REGRESSION)) {
			String detail = String.format("{\"%s\":%f}", "label", y);
			return Tuple2.of(y, detail);
		} else if (model.task.equals(Task.BINARY_CLASSIFICATION)) {
			y = logit(y);
			Object label = (y <= 0.5 ? model.labelValues[1] : model.labelValues[0]);

			String detail = String.format("{\"0\":%f,\"1\":%f}", 1 - y, y);
			return Tuple2.of(label, detail);

		} else {
			throw new RuntimeException("task not support yet");
		}
	}

	public double getY(SparseVector feature, boolean isBinCls) {
		double y = FmOptimizer.calcY(feature, model.fmModel, dim).f0;
		if (isBinCls) {
			y = logit(y);
		}
		return y;
	}

	private static double logit(double x) {
		return 1. / (1. + Math.exp(-x));
	}

	public FmModelData getModel() {
		return model;
	}
}
