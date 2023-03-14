package com.alibaba.alink.operator.common.fm;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.linear.FeatureLabelUtil;
import com.alibaba.alink.operator.common.optim.FmOptimizer;
import com.alibaba.alink.params.recommendation.FmPredictParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fm mapper maps one sample to a sample with a predicted label.
 */
public class FmModelMapper extends RichModelMapper {

	private static final long serialVersionUID = -6348182372481296494L;
	private int vectorColIndex = -1;
	private FmModelData model;
	private int[] dim;
	private int[] featureIdx;
	private final TypeInformation <?> labelType;
	private transient ThreadLocal <DenseVector> threadLocalVec;

	public FmModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		if (null != params) {
			String vectorColName = params.get(FmPredictParams.VECTOR_COL);
			if (null != vectorColName && vectorColName.length() != 0) {
				this.vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), vectorColName);
			}
		}
		labelType = modelSchema.getFieldTypes()[2];
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		FmModelDataConverter fmModelDataConverter
			= new FmModelDataConverter(FmModelDataConverter.extractLabelType(super.getModelSchema()));
		this.model = fmModelDataConverter.load(modelRows);
		this.dim = model.dim;
		if (labelType.equals(Types.INT)) {
			this.model.labelValues[0] = Double.valueOf(model.labelValues[0].toString()).intValue();
			this.model.labelValues[1] = Double.valueOf(model.labelValues[1].toString()).intValue();
		} else if (labelType.equals(Types.LONG)) {
			this.model.labelValues[0] = Double.valueOf(model.labelValues[0].toString()).longValue();
			this.model.labelValues[1] = Double.valueOf(model.labelValues[1].toString()).longValue();
		}

		if (vectorColIndex == -1) {
			TableSchema dataSchema = getDataSchema();
			if (this.model.featureColNames != null) {
				int featLen = this.model.featureColNames.length;
				this.featureIdx = new int[featLen];
				String[] predictTableColNames = dataSchema.getFieldNames();
				for (int i = 0; i < featLen; i++) {
					this.featureIdx[i] = TableUtil.findColIndexWithAssert(predictTableColNames,
						this.model.featureColNames[i]);
				}
				threadLocalVec = ThreadLocal.withInitial(() -> new DenseVector(featLen));
			} else {
				vectorColIndex = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), model.vectorColName);
			}
		}
	}

	public double getY(SparseVector feature, boolean isBinCls) {
		double y = FmOptimizer.calcY(feature, model.fmModel, dim).f0;
		if (isBinCls) {
			y = logit(y);
		}
		return y;
	}

	private static double logit(double y) {
		if (y < -37) {
			return 0.0;
		} else if (y > 34) {
			return 1.0;
		} else {
			return 1.0 / (1.0 + Math.exp(-y));
		}
	}

	public FmModelData getModel() {
		return model;
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) {
		Vector vec;
		if (vectorColIndex != -1) {
			vec = FeatureLabelUtil.getVectorFeature(selection.get(vectorColIndex), false, model.vectorSize);
		} else {
			vec = threadLocalVec.get();
			selection.fillDenseVector((DenseVector) vec, false, featureIdx);
		}

		double y = FmOptimizer.calcY(vec, model.fmModel, dim).f0;

		if (model.task.equals(Task.REGRESSION)) {
			String detail = String.format("{\"%s\":%f}", "label", y);
			return Tuple2.of(y, detail);
		} else if (model.task.equals(Task.BINARY_CLASSIFICATION)) {
			y = logit(y);
			Object label = (y <= 0.5 ? model.labelValues[1] : model.labelValues[0]);
			Map <String, String> detail = new HashMap <>(0);
			detail.put(model.labelValues[1].toString(), Double.valueOf(1 - y).toString());
			detail.put(model.labelValues[0].toString(), Double.valueOf(y).toString());
			String jsonDetail = JsonConverter.toJson(detail);
			return Tuple2.of(label, jsonDetail);
		} else {
			throw new AkUnsupportedOperationException("task not support yet");
		}
	}
}
