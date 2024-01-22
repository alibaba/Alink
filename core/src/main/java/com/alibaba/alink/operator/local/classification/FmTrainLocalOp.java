package com.alibaba.alink.operator.local.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.fm.FmModelData;
import com.alibaba.alink.operator.common.fm.FmModelDataConverter;
import com.alibaba.alink.operator.common.optim.LocalFmOptimizer;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.recommendation.FmTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Local FM model training.
 */
@Internal
public class FmTrainLocalOp<T extends FmTrainLocalOp <T>> extends LocalOperator <T> {

	private static final long serialVersionUID = -3985394692858121356L;

	/**
	 * construct function.
	 *
	 * @param params parameters needed by training process.
	 * @param task   Fm task: maybe "binary_classification" or "regression".
	 */
	public FmTrainLocalOp(Params params, Task task) {
		super(params.set(ModelParamName.TASK, task));
	}

	/**
	 * do the operation of this op.
	 *
	 * @param inputs the linked inputs
	 * @return this class.
	 */
	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		// Get parameters of this algorithm.
		Params params = getParams();
		if (params.contains(HasFeatureCols.FEATURE_COLS) && params.contains(HasVectorCol.VECTOR_COL)) {
			throw new AkIllegalArgumentException("featureCols and vectorCol cannot be set at the same time.");
		}
		int[] dim = new int[3];
		dim[0] = params.get(FmTrainParams.WITH_INTERCEPT) ? 1 : 0;
		dim[1] = params.get(FmTrainParams.WITH_LINEAR_ITEM) ? 1 : 0;
		dim[2] = params.get(FmTrainParams.NUM_FACTOR);

		boolean isRegProc = params.get(ModelParamName.TASK).equals(Task.REGRESSION);
		TypeInformation <?> labelType = isRegProc ? Types.DOUBLE : in.getColTypes()[TableUtil
			.findColIndex(in.getColNames(), params.get(FmTrainParams.LABEL_COL))];

		// Transform data to Tuple3 format <weight, label, feature vector>.
		List <Tuple3 <Double, Object, Vector>> initData = transform(in, params, isRegProc);

		// Get some util info, such as featureSize and labelValues.
		Tuple2 <Object[], Integer> labelsAndFeatureSize = getLabelsAndFeatureSize(initData, isRegProc);

		List <Tuple3 <Double, Double, Vector>>
			trainData = transferLabel(initData, isRegProc, labelsAndFeatureSize.f0);

		Tuple2 <FmDataFormat, double[]> model = optimize(trainData, labelsAndFeatureSize.f1, params, dim);

		List <Row> modelRows = transformModel(model, labelsAndFeatureSize.f0,
			labelsAndFeatureSize.f1, params, dim, isRegProc, labelType);

		this.setOutputTable(new MTable(modelRows, new FmModelDataConverter(labelType).getModelSchema()));
	}

	/**
	 * optimize function.
	 *
	 * @param trainData training Data.
	 * @param vecSize   vector size.
	 * @param params    parameters.
	 * @param dim       dimension.
	 * @return result.
	 */
	protected Tuple2 <FmDataFormat, double[]> optimize(List <Tuple3 <Double, Double, Vector>> trainData,
													   int vecSize,
													   final Params params,
													   final int[] dim) {
		final double initStdev = getParams().get(FmTrainParams.INIT_STDEV);
		FmDataFormat initFactors = new FmDataFormat(vecSize, dim, initStdev);

		LocalFmOptimizer optimizer = new LocalFmOptimizer(trainData, params);
		optimizer.setWithInitFactors(initFactors);
		return optimizer.optimize();
	}

	/**
	 * The api for transforming model format.
	 *
	 * @param model       model with fm data format.
	 * @param labelValues label values.
	 * @param vecSize     vector size.
	 * @param params      parameters.
	 * @param dim         dimension.
	 * @param isRegProc   is regression process.
	 * @param labelType   label type.
	 * @return model rows.
	 */
	private List <Row> transformModel(Tuple2 <FmDataFormat, double[]> model,
									  Object[] labelValues,
									  Integer vecSize,
									  Params params,
									  int[] dim,
									  boolean isRegProc,
									  TypeInformation <?> labelType) {
		FmModelData modelData = new FmModelData();
		modelData.fmModel = model.f0;
		modelData.vectorColName = params.get(FmTrainParams.VECTOR_COL);
		modelData.featureColNames = params.get(FmTrainParams.FEATURE_COLS);
		modelData.dim = dim;
		modelData.regular = new double[] {params.get(FmTrainParams.LAMBDA_0),
			params.get(FmTrainParams.LAMBDA_1),
			params.get(FmTrainParams.LAMBDA_2)};
		modelData.labelColName = params.get(FmTrainParams.LABEL_COL);
		modelData.task = params.get(ModelParamName.TASK);
		modelData.vectorSize = vecSize;
		if (!isRegProc) {
			modelData.labelValues = labelValues;
		} else {
			modelData.labelValues = new Object[] {0.0};
		}
		RowCollector rowCollector = new RowCollector();
		new FmModelDataConverter(labelType).save(modelData, rowCollector);
		return rowCollector.getRows();
	}

	/**
	 * Do transform for train data.
	 *
	 * @param initData    initial data.
	 * @param isRegProc   train process is regression or classification.
	 * @param labelValues label values.
	 * @return data for fm training.
	 */
	private List <Tuple3 <Double, Double, Vector>> transferLabel(
		List <Tuple3 <Double, Object, Vector>> initData,
		final boolean isRegProc,
		Object[] labelValues) {
		List <Tuple3 <Double, Double, Vector>> retList = new ArrayList <>(initData.size());
		for (Tuple3 <Double, Object, Vector> value : initData) {
			if (value.f0 > 0) {
				Double label = isRegProc ? Double.parseDouble(value.f1.toString())
					: (value.f1.equals(labelValues[0]) ? 1.0 : 0.0);
				retList.add(Tuple3.of(value.f0, label, value.f2));
			}
		}
		return retList;
	}

	/**
	 * @param initData  get some useful info from initial data.
	 * @param isRegProc train process is regression or classification.
	 * @return useful data, including label values and vector size.
	 */
	private Tuple2 <Object[], Integer> getLabelsAndFeatureSize(
		List <Tuple3 <Double, Object, Vector>> initData,
		boolean isRegProc) {
		int size = -1;
		Set <Object> labelValues = new HashSet <>();
		for (Tuple3 <Double, Object, Vector> t3 : initData) {
			if (t3.f0 < 0) {
				Tuple2 <Integer, Object[]>
					labelVals = (Tuple2 <Integer, Object[]>) t3.f1;
				Collections.addAll(labelValues, labelVals.f1);
				size = Math.max(size, labelVals.f0);
			}
		}
		Object[] labelsSort = isRegProc ? labelValues.toArray() : orderLabels(labelValues);
		return Tuple2.of(labelsSort, size);
	}

	/**
	 * order by the dictionary order, only classification problem need do this process.
	 *
	 * @param unorderedLabelRows unordered label rows
	 * @return Ordered labels.
	 */
	private Object[] orderLabels(Iterable <Object> unorderedLabelRows) {
		List <Object> tmpArr = new ArrayList <>();
		for (Object row : unorderedLabelRows) {
			tmpArr.add(row);
		}
		Object[] labels = tmpArr.toArray(new Object[0]);
		AkPreconditions.checkState((labels.length == 2),
			new AkIllegalDataException("labels count should be 2 in 2 classification algo."));
		if (labels[0] instanceof Number) {
			if (((Number) labels[0]).doubleValue() + ((Number) labels[1]).doubleValue() == 1.0) {
				if (((Number) labels[0]).doubleValue() == 0.0) {
					Object t = labels[0];
					labels[0] = labels[1];
					labels[1] = t;
				}
			}
		} else {
			String str0 = labels[0].toString();
			String str1 = labels[1].toString();
			String positiveLabelValueString = (str1.compareTo(str0) > 0) ? str1 : str0;

			if (labels[1].toString().equals(positiveLabelValueString)) {
				Object t = labels[0];
				labels[0] = labels[1];
				labels[1] = t;
			}
		}
		return labels;
	}

	/**
	 * Transform train data to Tuple3 format.
	 *
	 * @param in     train data in row format.
	 * @param params train parameters.
	 * @return Tuple3 format train data <weight, label, vector></>.
	 */
	private List <Tuple3 <Double, Object, Vector>> transform(LocalOperator <?> in,
															 Params params,
															 boolean isRegProc) {
		String[] featureColNames = params.get(FmTrainParams.FEATURE_COLS);
		String labelName = params.get(FmTrainParams.LABEL_COL);
		String weightColName = params.get(FmTrainParams.WEIGHT_COL);
		String vectorColName = params.get(FmTrainParams.VECTOR_COL);
		TableSchema dataSchema = in.getSchema();
		if (null == featureColNames && null == vectorColName) {
			featureColNames = TableUtil.getNumericCols(dataSchema, new String[] {labelName});
			params.set(FmTrainParams.FEATURE_COLS, featureColNames);
		}
		int[] featureIndices = null;
		int labelIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), labelName);
		if (featureColNames != null) {
			featureIndices = new int[featureColNames.length];
			for (int i = 0; i < featureColNames.length; ++i) {
				int idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), featureColNames[i]);
				featureIndices[i] = idx;
			}
		}
		int weightIdx = weightColName != null ? TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
			weightColName)
			: -1;
		int vecIdx = vectorColName != null ? TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName)
			: -1;

		MTable mt = in.getOutputTable();

		return preprocess(mt, isRegProc, weightIdx, vecIdx, featureIndices, labelIdx);
	}

	/**
	 * Transform the train data to Tuple3 format: Tuple3<weightValue, labelValue, featureSparseVector>
	 */
	private List <Tuple3 <Double, Object, Vector>> preprocess(MTable mt, boolean isRegProc, int weightIdx, int vecIdx,
															  int[] featureIndices, int labelIdx) {
		Set <Object> labelValues = new HashSet <>();
		List <Tuple3 <Double, Object, Vector>> listRet = new ArrayList <>();
		int size = -1;
		if (featureIndices != null) {
			size = featureIndices.length;
		}
		for (Row row : mt.getRows()) {
			Double weight = weightIdx == -1 ? 1.0 : ((Number) row.getField(weightIdx)).doubleValue();
			Object label = row.getField(labelIdx);

			if (!isRegProc) {
				labelValues.add(label);
			} else {
				labelValues.add(0.0);
			}

			Vector vec;
			if (featureIndices != null) {
				vec = new DenseVector(featureIndices.length);
				for (int i = 0; i < featureIndices.length; ++i) {
					vec.set(i, ((Number) row.getField(featureIndices[i])).doubleValue());
				}
			} else {
				vec = VectorUtil.getVector(row.getField(vecIdx));
				if (vec instanceof SparseVector) {
					int[] indices = ((SparseVector) vec).getIndices();
					for (int index : indices) {
						size = (vec.size() > 0) ? vec.size() : Math.max(size, index + 1);
					}
				} else {
					size = ((DenseVector) vec).getData().length;
				}
			}
			listRet.add(Tuple3.of(weight, label, vec));
		}
		listRet.add(
			Tuple3.of(-1.0, Tuple2.of(size, labelValues.toArray()), new DenseVector(0)));
		return listRet;
	}
}


