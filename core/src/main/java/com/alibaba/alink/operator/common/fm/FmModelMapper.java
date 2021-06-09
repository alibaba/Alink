package com.alibaba.alink.operator.common.fm;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.linear.FeatureLabelUtil;
import com.alibaba.alink.operator.common.optim.FmOptimizer;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * Fm mapper maps one sample to a sample with a predicted label.
 */
public class FmModelMapper extends RichModelMapper {

	private static final long serialVersionUID = -6348182372481296494L;
	private int vectorColIndex = -1;
	private FmModelData model;
	private int[] dim;
	private int[] featureIdx;
	private long lastModelVersion = Long.MAX_VALUE;
	private final TypeInformation <?> labelType;
	private transient ThreadLocal <DenseVector> threadLocalVec;

	public FmModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		if (null != params) {
			String vectorColName = params.get(SoftmaxPredictParams.VECTOR_COL);
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
		if (labelType == Types.INT) {
			this.model.labelValues[0] = Double.valueOf(model.labelValues[0].toString()).intValue();
			this.model.labelValues[1] = Double.valueOf(model.labelValues[1].toString()).intValue();
		} else if (labelType == Types.LONG) {
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

	@Override
	public ModelMapper createNew(List <Row> modelRows) {
		FmModelMapper fmModelMapper = new FmModelMapper(getModelSchema(), getDataSchema(), params.clone());
		FmModelData modelData = new FmModelData();
		modelData.vectorColName = this.model.vectorColName;
		modelData.featureColNames = this.model.featureColNames;
		modelData.labelColName = this.model.labelColName;
		modelData.task = this.model.task;
		modelData.dim = this.model.dim;
		modelData.vectorSize = this.model.vectorSize;

		modelData.labelValues = this.model.labelValues;

		modelData.fmModel = new FmDataFormat();
		modelData.fmModel.factors = new double[modelData.vectorSize][];
		modelData.fmModel.dim = modelData.dim;

		int featureSize = model.fmModel.factors.length;
		int cnt = 0;
		for (int i = 0; i < featureSize; ++i) {
			if (model.fmModel.factors[i] != null) {
				modelData.fmModel.factors[i] = model.fmModel.factors[i];
			}
		}

		Params meta = null;
		for (Row row : modelRows) {
			Long featureId = (Long) row.getField(0);
			if (featureId == null && row.getField(1) != null) {
				String metaStr = (String) row.getField(1);
				meta = Params.fromJson(metaStr);
				break;
			}
		}

		if (this.lastModelVersion == meta.get("lastModelVersion", Long.TYPE)
			|| meta.get("isFullModel", Boolean.TYPE)) {
			cnt = 0;
			this.lastModelVersion = meta.get("modelVersion", Long.TYPE);
			for (Row row : modelRows) {
				Long featureId = (Long) row.getField(0);
				if (featureId >= 0) {
					cnt++;
					double[] factor = gson.fromJson((String) row.getField(1), double[].class);
					modelData.fmModel.factors[featureId.intValue()] = factor;
				} else if (featureId == -1) {
					double[] factor = gson.fromJson((String) row.getField(1), double[].class);
					model.fmModel.bias = factor[0];
				}
			}
		}
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			if (meta.get("isFullModel", Boolean.TYPE)) {
				System.out.println("load fm full model ok, model size : " + cnt);
			} else {
				System.out.println("load fm increment model ok, model size : " + cnt);
			}
		}
		fmModelMapper.model = modelData;
		return this;
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

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
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
			throw new RuntimeException("task not support yet");
		}
	}
}
