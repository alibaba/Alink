package com.alibaba.alink.operator.local.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.lazy.WithTrainInfoLocalOp;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelTrainInfo;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.optim.LocalOptimizer;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.regression.LassoRegTrainParams;
import com.alibaba.alink.params.regression.LinearSvrTrainParams;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Base class of linear model training. Linear binary classification and linear regression algorithms should inherit
 * this class. Then it only need to write the code of loss function and regular item.
 *
 * @param <T> parameter of this class. Maybe the Svm, linearRegression or Lr parameter.
 */

@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(value = PortType.MODEL, isOptional = true)
})
@OutputPorts(values = {
	@PortSpec(PortType.MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.MODEL_INFO),
	@PortSpec(value = PortType.DATA, desc = PortDesc.FEATURE_IMPORTANCE),
	@PortSpec(value = PortType.DATA, desc = PortDesc.MODEL_WEIGHT)
})

@ParamSelectColumnSpec(name = "featureCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "weightCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@FeatureColsVectorColMutexRule

@Internal
public abstract class BaseLinearModelTrainLocalOp<T extends BaseLinearModelTrainLocalOp <T>> extends LocalOperator <T>
	implements WithTrainInfoLocalOp <LinearModelTrainInfo, T> {
	final static int MAX_LABELS = 1000;
	final static double LABEL_RATIO = 0.5;

	private final String modelName;
	private final LinearModelType linearModelType;

	/**
	 * @param params    parameters needed by training process.
	 * @param modelType model type: LR, SVR, SVM, Ridge ...
	 * @param modelName name of model.
	 */
	public BaseLinearModelTrainLocalOp(Params params, LinearModelType modelType, String modelName) {
		super(params);
		this.modelName = modelName;
		this.linearModelType = modelType;
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in;
		LocalOperator <?> initModel = null;
		if (inputs.length == 1) {
			in = checkAndGetFirst(inputs);
		} else {
			in = inputs[0];
			initModel = inputs[1];
		}
		/* Get parameters of this algorithm. */
		final Params params = getParams();
		if (params.contains(HasFeatureCols.FEATURE_COLS) && params.contains(HasVectorCol.VECTOR_COL)) {
			throw new AkIllegalArgumentException("FeatureCols and vectorCol cannot be set at the same time.");
		}

		try {
			MTable mt = in.getOutputTable();

			/* Get type of processing: regression or not */
			final boolean isRegProc = getIsRegProc(params, linearModelType, modelName);
			final boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
			final TypeInformation <?> labelType = isRegProc ? Types.DOUBLE : mt.getColTypes()[TableUtil
				.findColIndexWithAssertAndHint(mt.getColNames(), params.get(LinearTrainParams.LABEL_COL))];

			// Transform data to Tuple3 format <weight, label, feature vector>. meanAndVar, dim, labelsSort
			Tuple4 <List <Tuple3 <Double, Double, Vector>>, DenseVector[], Integer, Object[]> t4
				= preprocess(mt, params, isRegProc, linearModelType);
			List <Tuple3 <Double, Double, Vector>> trainData = t4.f0;
			DenseVector[] meanVar = t4.f1;
			Integer featSize = t4.f2;
			Object[] labelValues = t4.f3;

			DenseVector initModelCoefs = (null == initModel) ?
				DenseVector.zeros(featSize * (isRegProc ? 1 : (labelValues.length - 1))) :
				initializeModelCoefs(initModel.getOutputTable().getRows(), featSize, meanVar, params, linearModelType);

			if (LinearModelType.Softmax == linearModelType) {
				params.set(ModelParamName.NUM_CLASSES, labelValues.length);
			}
			Tuple2 <DenseVector, double[]> modelCoefs
				= LocalOptimizer.optimize(OptimObjFunc.getObjFunction(linearModelType, params), trainData,
				initModelCoefs, params);

			String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);

			Params meta = new Params();
			meta.set(ModelParamName.MODEL_NAME, this.modelName);
			meta.set(ModelParamName.LINEAR_MODEL_TYPE, this.linearModelType);
			meta.set(ModelParamName.HAS_INTERCEPT_ITEM, hasIntercept);
			meta.set(ModelParamName.VECTOR_COL_NAME, params.get(LinearTrainParams.VECTOR_COL));
			meta.set(LinearTrainParams.LABEL_COL, params.get(LinearTrainParams.LABEL_COL));
			meta.set(ModelParamName.FEATURE_TYPES, getFeatureTypes(mt.getSchema(), featureColNames));
			if (LinearModelType.LinearReg != linearModelType && LinearModelType.SVR != linearModelType
				&& LinearModelType.AFT != linearModelType) {
				meta.set(ModelParamName.LABEL_VALUES, labelValues);
			}
			if (LinearModelType.Softmax == linearModelType) {
				meta.set(ModelParamName.NUM_CLASSES, labelValues.length);
			}

			if (LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE))) {
				meanVar = null;
			}
			LinearModelData modelData = buildLinearModelData(meta, featureColNames, labelType, meanVar,
				hasIntercept,
				params.get(LinearTrainParams.STANDARDIZATION),
				modelCoefs);

			LinearModelDataConverter linearModelDataConverter = new LinearModelDataConverter(labelType);
			RowCollector rowCollector = new RowCollector();
			linearModelDataConverter.save(modelData, rowCollector);
			List <Row> modelRows = rowCollector.getRows();
			this.setOutputTable(new MTable(modelRows, linearModelDataConverter.getModelSchema()));

			this.setSideOutputTables(getSideTablesOfCoefficient(modelRows, modelCoefs.f1, trainData, featSize,
				params.get(LinearTrainParams.FEATURE_COLS),
				params.get(LinearTrainParams.WITH_INTERCEPT))
			);
		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}

	}

	public static DenseVector initializeModelCoefs(List <Row> modelRows,
												   Integer featSize,
												   DenseVector[] meanVar,
												   Params params,
												   final LinearModelType localLinearModelType) {

		LinearModelData model = new LinearModelDataConverter().load(modelRows);

		if (!(model.hasInterceptItem == params.get(HasWithIntercept.WITH_INTERCEPT))) {
			throw new AkIllegalArgumentException("Initial linear model is not compatible with parameter setting."
				+ "InterceptItem parameter setting error.");
		}
		if (!(model.linearModelType == localLinearModelType)) {
			throw new AkIllegalArgumentException("Initial linear model is not compatible with parameter setting."
				+ "linearModelType setting error.");
		}
		if (!(model.vectorSize + (model.hasInterceptItem ? 1 : 0) == featSize)) {
			throw new AkIllegalDataException("Initial linear model is not compatible with training data. "
				+ " vector size not equal, vector size in init model is : " + model.vectorSize +
				" and vector size of train data is : " + featSize);
		}
		if (null != meanVar[0]) {
			int n = meanVar[0].size();
			if (!LinearModelType.Softmax.equals(localLinearModelType)) {

				if (model.hasInterceptItem) {
					double sum = 0.0;
					for (int i = 1; i < n; ++i) {
						sum += model.coefVector.get(i) * meanVar[0].get(i);
						model.coefVector.set(i, model.coefVector.get(i) * meanVar[1].get(i));
					}
					model.coefVector.set(0, model.coefVector.get(0) + sum);
				} else {
					for (int i = 0; i < n; ++i) {
						model.coefVector.set(i, model.coefVector.get(i) * meanVar[1].get(i));
					}
				}
			} else {
				if (model.hasInterceptItem) {
					for (int i = 0; i < 10 - 1; ++i) {
						double sum = 0.0;
						for (int j = 1; j < n; ++j) {
							int idx = i * n + j;
							sum += model.coefVector.get(idx) * meanVar[0].get(j);
							model.coefVector.set(idx, model.coefVector.get(idx) * meanVar[1].get(j));
						}
						model.coefVector.set(i * n, model.coefVector.get(i * n) + sum);
					}
				} else {
					for (int i = 0; i < model.coefVector.size(); ++i) {
						int idx = i % meanVar[1].size();
						model.coefVector.set(i, model.coefVector.get(i) * meanVar[1].get(idx));
					}
				}
			}
		}
		return model.coefVector;
	}

	public static MTable[] getSideTablesOfCoefficient(List <Row> modelRow,
													  double[] convergenceInfo,
													  List <Tuple3 <Double, Double, Vector>> inputData,
													  Integer vecSize,
													  final String[] featureNames,
													  final boolean hasInterception) {

		LinearModelData model = new LinearModelDataConverter().load(modelRow);

		int vectorSize = vecSize;
		if (hasInterception) {
			vectorSize--;
		}

		int iter = 0;
		double[] mu = new double[vectorSize];
		double[] mu2 = new double[vectorSize];
		if (featureNames == null) {
			for (Tuple3 <Double, Double, Vector> t3 : inputData) {
				if (t3.f0 < 0.0) {
					continue;
				}
				if (t3.f2 instanceof SparseVector) {
					SparseVector tmp = (SparseVector) t3.f2;
					tmp.setSize(vectorSize);

					double[] vecValues = tmp.getValues();
					int[] idx = tmp.getIndices();
					for (int i = 0; i < vecValues.length; ++i) {
						if (hasInterception) {
							if (idx[i] > 0) {
								mu[idx[i] - 1] += vecValues[i];
								mu2[idx[i] - 1] += vecValues[i] * vecValues[i];
							}
						} else {
							mu[idx[i]] += vecValues[i];
							mu2[idx[i]] += vecValues[i] * vecValues[i];
						}
					}
					iter++;
				} else {
					for (int i = 0; i < vectorSize; ++i) {
						double val = t3.f2.get(i + (hasInterception ? 1 : 0));
						mu[i] += val;
						mu2[i] += val * val;
					}
					iter++;
				}
			}
		} else {
			for (Tuple3 <Double, Double, Vector> t3 : inputData) {
				if (t3.f0 < 0.0) {
					continue;
				}
				for (int i = 0; i < vectorSize; ++i) {
					double val = t3.f2.get(i + (hasInterception ? 1 : 0));
					mu[i] += val;
					mu2[i] += val * val;
				}
				iter++;
			}
		}
		Tuple3 <Integer, double[], double[]> tuple3 = Tuple3.of(iter, mu, mu2);

		DenseVector coefVec = model.coefVector;
		double[] importanceVals;
		String[] colNames;
		if (featureNames == null) {
			colNames = new String[coefVec.size() - (hasInterception ? 1 : 0)];
			for (int i = 0; i < colNames.length; ++i) {
				colNames[i] = String.valueOf(i);
			}
		} else {
			colNames = featureNames;
		}

		if (hasInterception) {
			importanceVals = new double[coefVec.size() - 1];
		} else {
			importanceVals = new double[coefVec.size()];
		}
		for (int i = 0; i < tuple3.f1.length; ++i) {

			double nu = tuple3.f1[i] / tuple3.f0;
			double sigma = tuple3.f2[i] - tuple3.f0 * nu * nu;
			if (tuple3.f0 == 1) {
				sigma = 0.0;
			} else {
				sigma = Math.sqrt(Math.max(0.0, sigma) / (tuple3.f0 - 1));
			}
			importanceVals[i] = Math.abs(coefVec.get(i + (hasInterception ? 1 : 0)) * sigma);
		}
		Tuple5 <String, String[], double[], double[], double[]> allInfo =
			Tuple5.of(JsonConverter.toJson(model.getMetaInfo()), colNames, coefVec.getData(),
				importanceVals, convergenceInfo);

		ArrayList <Row> importance = new ArrayList <>();
		// note: colNames = allInfo.f1;
		// note: importanceVals = allInfo.f3;
		for (int i = 0; i < Objects.requireNonNull(colNames).length; ++i) {
			importance.add(Row.of(colNames[i], importanceVals[i]));
		}

		ArrayList <Row> weights = new ArrayList <>();
		double[] weightVals = allInfo.f2;
		if (weightVals.length == colNames.length) {
			for (int i = 0; i < colNames.length; ++i) {
				weights.add(Row.of(colNames[i], weightVals[i]));
			}
		} else {
			weights.add(Row.of("_intercept_", weightVals[0]));
			for (int i = 0; i < colNames.length; ++i) {
				weights.add(Row.of(colNames[i], weightVals[i + 1]));
			}
		}

		ArrayList <Row> summary = new ArrayList <>();
		final int NUM_COLLECT_THRESHOLD = 10000;
		if (allInfo.f1.length < NUM_COLLECT_THRESHOLD) {
			summary.add(Row.of(0L, allInfo.f0));
			summary.add(Row.of(1L, JsonConverter.toJson(allInfo.f1)));
			summary.add(Row.of(2L, JsonConverter.toJson(allInfo.f2)));
			summary.add(Row.of(3L, JsonConverter.toJson(allInfo.f3)));
			summary.add(Row.of(4L, JsonConverter.toJson(allInfo.f4)));
		} else {
			List <Tuple3 <String, Double, Double>> array = new ArrayList <>(allInfo.f1.length);
			int startIdx = hasInterception ? 1 : 0;
			for (int i = 0; i < allInfo.f1.length; ++i) {
				array.add(Tuple3.of(allInfo.f1[i], allInfo.f2[i + startIdx], allInfo.f3[i]));
			}
			array.sort(compare);
			String[] _colName = new String[NUM_COLLECT_THRESHOLD];
			double[] _weight = new double[NUM_COLLECT_THRESHOLD];
			double[] _importance = new double[NUM_COLLECT_THRESHOLD];
			for (int i = 0; i < NUM_COLLECT_THRESHOLD / 2; ++i) {
				_colName[i] = array.get(i).f0;
				_weight[i] = array.get(i).f1;
				_importance[i] = array.get(i).f2;
				int srcIdx = allInfo.f1.length - i - 1;
				int destIdx = NUM_COLLECT_THRESHOLD - i - 1;
				_colName[destIdx] = array.get(srcIdx).f0;
				_weight[destIdx] = array.get(srcIdx).f1;
				_importance[destIdx] = array.get(srcIdx).f2;
			}

			summary.add(Row.of(0L, allInfo.f0));
			summary.add(Row.of(1L, JsonConverter.toJson(_colName)));
			summary.add(Row.of(2L, JsonConverter.toJson(_weight)));
			summary.add(Row.of(3L, JsonConverter.toJson(_importance)));
			summary.add(Row.of(4L, JsonConverter.toJson(allInfo.f4)));
		}

		MTable summaryTable = new MTable(summary,
			new TableSchema(new String[] {"id", "info"}, new TypeInformation[] {Types.LONG, Types.STRING}));
		MTable importanceTable = new MTable(importance, new TableSchema(new String[] {"col_name", "importance"},
			new TypeInformation[] {Types.STRING, Types.DOUBLE}));
		MTable weightTable = new MTable(weights,
			new TableSchema(new String[] {"col_name", "weight"}, new TypeInformation[] {Types.STRING, Types.DOUBLE}));
		return new MTable[] {summaryTable, importanceTable, weightTable};
	}

	public static Comparator <Tuple3 <String, Double, Double>> compare = (o1, o2) -> o2.f2.compareTo(o1.f2);

	/**
	 * Order by the dictionary order, only classification problem need do this process.
	 *
	 * @param unorderedLabelRows Unordered label rows.
	 * @return Ordered label rows.
	 */
	private static Object[] orderLabels(Iterable <Object> unorderedLabelRows) {
		List <Object> tmpArr = new ArrayList <>();
		for (Object row : unorderedLabelRows) {
			tmpArr.add(row);
		}
		Object[] labels = tmpArr.toArray(new Object[0]);
		String str0 = labels[0].toString();
		String str1 = labels[1].toString();

		String positiveLabelValueString = (str1.compareTo(str0) > 0) ? str1 : str0;

		if (labels[1].toString().equals(positiveLabelValueString)) {
			Object t = labels[0];
			labels[0] = labels[1];
			labels[1] = t;
		}
		return labels;
	}

	private static Tuple4 <List <Tuple3 <Double, Double, Vector>>, DenseVector[], Integer, Object[]> preprocess(
		MTable mt, Params params, boolean isRegProc, LinearModelType linearModelType) {

		final boolean calcMeanVar = params.get(LinearTrainParams.STANDARDIZATION);
		final boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
		String[] featureColNames = params.get(LinearTrainParams.FEATURE_COLS);
		String labelName = params.get(LinearTrainParams.LABEL_COL);
		String weightColName = params.get(LinearTrainParams.WEIGHT_COL);
		String vectorColName = params.get(LinearTrainParams.VECTOR_COL);
		TableSchema dataSchema = mt.getSchema();
		if (null == featureColNames && null == vectorColName) {
			featureColNames = TableUtil.getNumericCols(dataSchema, new String[] {labelName});
			params.set(LinearTrainParams.FEATURE_COLS, featureColNames);
		}
		int[] featureIndices = null;
		int labelIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), labelName);
		if (featureColNames != null) {
			featureIndices = new int[featureColNames.length];
			for (int i = 0; i < featureColNames.length; ++i) {
				int idx = TableUtil.findColIndexWithAssertAndHint(mt.getColNames(), featureColNames[i]);
				featureIndices[i] = idx;
				TypeInformation <?> type = mt.getSchema().getFieldTypes()[idx];

				AkPreconditions.checkState(TableUtil.isSupportedNumericType(type),
					"linear algorithm only support numerical data type. Current type is : " + type);
			}
		}
		int weightIdx = weightColName != null ? TableUtil.findColIndexWithAssertAndHint(mt.getColNames(),
			weightColName) : -1;
		int vecIdx = vectorColName != null ?
			TableUtil.findColIndexWithAssertAndHint(mt.getColNames(), vectorColName) : -1;

		Set <Object> labelValues = new HashSet <>();

		final boolean useSparseVector;

		List <Tuple3 <Double, Object, DenseVector>> listDense = new ArrayList <>();
		List <Tuple3 <Double, Object, SparseVector>> listSparse = new ArrayList <>();
		int dimSparse = -1;
		int dimDense = -1;

		/**
		 * transform input features to sparse or dense vector format
		 */
		if (featureIndices != null) {    // input feature in columns format

			useSparseVector = false;
			dimDense = hasIntercept ? (featureIndices.length + 1) : featureIndices.length;
			int offset = hasIntercept ? 1 : 0;

			for (Row row : mt.getRows()) {
				Double weight = (weightIdx != -1) ? ((Number) row.getField(weightIdx)).doubleValue() : 1.0;
				Object val = row.getField(labelIdx);
				if (null == val) {
					continue;
				}
				DenseVector vec = new DenseVector(dimDense);
				if (hasIntercept) {
					vec.set(0, 1.0);
				}
				boolean notExistNull = true;
				for (int i = 0; i < featureIndices.length; ++i) {
					Object obj = row.getField(featureIndices[i]);
					if (obj == null) {
						notExistNull = false;
						break;
					} else {
						vec.set(i + offset, ((Number) obj).doubleValue());
					}
				}
				if (notExistNull) {
					listDense.add(Tuple3.of(weight, val, vec));
				}
			}

		} else {    // input feature in vector format

			int countSparse = 0;
			int countDense = 0;
			List <Tuple3 <Double, Object, Vector>> vecList = new ArrayList <>();

			for (Row row : mt.getRows()) {
				Double weight = weightIdx != -1 ? ((Number) row.getField(weightIdx)).doubleValue() : 1.0;
				Object val = row.getField(labelIdx);
				Vector vec = VectorUtil.getVector(row.getField(vecIdx));
				if (null != val && null != vec) {
					vecList.add(Tuple3.of(weight, val, hasIntercept ? vec.prefix(1.0) : vec));
				}
			}
			for (Tuple3 <Double, Object, Vector> t3 : vecList) {
				Vector vec = t3.f2;
				if (vec instanceof SparseVector) {
					countSparse++;
					int[] indices = ((SparseVector) vec).getIndices();
					if (indices.length > 0) {
						dimSparse = Math.max(indices[indices.length - 1] + 1, dimSparse);
					}
				} else {
					countDense++;
					int dim = ((DenseVector) vec).size();
					if (dimDense < 0) {
						dimDense = dim;
					} else {
						AkPreconditions.checkState((dim == dimDense),
							"Vector for linear model train have different dimension, please check your input data.");
					}
				}
			}

			if (countSparse > 2 * countDense) {
				useSparseVector = true;
				for (Tuple3 <Double, Object, Vector> t3 : vecList) {
					SparseVector vec = VectorUtil.getSparseVector(t3.f2);
					vec.setSize(dimSparse);
					listSparse.add(Tuple3.of(t3.f0, t3.f1, vec));
				}
			} else {
				useSparseVector = false;
				for (Tuple3 <Double, Object, Vector> t3 : vecList) {
					Vector vec = t3.f2;
					if (vec instanceof DenseVector) {
						listDense.add(Tuple3.of(t3.f0, t3.f1, (DenseVector) vec));
					} else {
						SparseVector svec = (SparseVector) vec;
						svec.setSize(dimDense);
						listDense.add(Tuple3.of(t3.f0, t3.f1, svec.toDenseVector()));
					}
				}
			}
		}

		DenseVector[] meanAndVar = new DenseVector[2];
		if (calcMeanVar) {
			if (useSparseVector) {
				double[] maxAbs = new double[dimSparse];
				for (Tuple3 <Double, Object, SparseVector> t3 : listSparse) {
					SparseVector vec = t3.f2;
					int[] indices = vec.getIndices();
					double[] values = vec.getValues();
					for (int i = 0; i < indices.length; i++) {
						maxAbs[indices[i]] = Math.max(maxAbs[indices[i]], Math.abs(values[i]));
					}
				}

				for (int i = 0; i < dimSparse; i++) {
					if (maxAbs[i] <= 0) {
						maxAbs[i] = 1.0;
					}
				}

				for (Tuple3 <Double, Object, SparseVector> t3 : listSparse) {
					SparseVector vec = t3.f2;
					int[] indices = vec.getIndices();
					double[] values = vec.getValues();
					for (int i = 0; i < indices.length; i++) {
						values[i] /= maxAbs[indices[i]];
					}
				}
				meanAndVar[0] = new DenseVector(dimSparse);
				meanAndVar[1] = new DenseVector(maxAbs);
			} else {
				double[] means = new double[dimDense];
				double[] stdvars = new double[dimDense];
				for (Tuple3 <Double, Object, DenseVector> t3 : listDense) {
					DenseVector vec = t3.f2;
					double[] values = vec.getData();
					for (int i = 0; i < dimDense; i++) {
						means[i] += values[i];
						stdvars[i] += values[i] * values[i];
					}
				}

				int m = listDense.size();
				for (int i = 0; i < dimDense; i++) {
					means[i] /= m;
					stdvars[i] =
						(m <= 1) ? 1.0 : Math.sqrt(Math.max(0.0, (stdvars[i] - m * means[i] * means[i]) / (m - 1)));
					if (0.0 == stdvars[i]) {
						// this feature has same values, we'd like to transform its value to 1.0
						stdvars[i] = (0.0 == means[i]) ? 1.0 : means[i];
						means[i] = 0.0;
					}
				}

				for (Tuple3 <Double, Object, DenseVector> t3 : listDense) {
					DenseVector vec = t3.f2;
					double[] values = vec.getData();
					for (int i = 0; i < dimDense; i++) {
						values[i] = (values[i] - means[i]) / stdvars[i];
					}
				}
				meanAndVar[0] = new DenseVector(means);
				meanAndVar[1] = new DenseVector(stdvars);
			}
		}

		HashMap <Object, Double> labelMap = new HashMap <>();
		Object[] labelsSort = new Object[0];
		if (!isRegProc) {
			if (useSparseVector) {
				for (Tuple3 <Double, Object, SparseVector> t3 : listSparse) {
					labelValues.add(t3.f1);
				}
			} else {
				for (Tuple3 <Double, Object, DenseVector> t3 : listDense) {
					labelValues.add(t3.f1);
				}
			}

			labelsSort = orderLabels(labelValues);
			if (LinearModelType.Softmax == linearModelType) {
				for (int i = 0; i < labelsSort.length; i++) {
					labelMap.put(labelsSort[i], Double.valueOf(i));
				}
			} else {
				if (labelsSort.length == 2) {
					labelMap.put(labelsSort[0], 1.0);
					labelMap.put(labelsSort[1], -1.0);
				} else {
					StringBuilder sbd = new StringBuilder();
					for (int i = 0; i < Math.min(labelsSort.length, 10); i++) {
						sbd.append(labelsSort[i]);
						if (i > 0) {sbd.append(",");}
					}
					if (labelsSort.length > 10) {
						sbd.append(", ...... ");
					}
					throw new AkIllegalDataException(
						linearModelType + " need 2 label values, but training data's distinct label values : " + sbd);
				}
			}
		}

		if (labelValues.size() > mt.getNumRow() * LABEL_RATIO && labelValues.size() > MAX_LABELS) {
			throw new AkIllegalDataException("label num is : " + labelValues.size() + ","
				+ " sample num is : " + mt.getNumRow() + ", please check your label column.");
		}

		List <Tuple3 <Double, Double, Vector>> finalList = new ArrayList <>();
		if (useSparseVector) {
			for (Tuple3 <Double, Object, SparseVector> t3 : listSparse) {
				Double label = isRegProc ? Double.parseDouble(t3.f1.toString()) : labelMap.get(t3.f1);
				finalList.add(Tuple3.of(t3.f0, label, t3.f2));
			}
			return Tuple4.of(finalList, meanAndVar, dimSparse, labelsSort);
		} else {
			for (Tuple3 <Double, Object, DenseVector> t3 : listDense) {
				Double label = isRegProc ? Double.parseDouble(t3.f1.toString()) : labelMap.get(t3.f1);
				finalList.add(Tuple3.of(t3.f0, label, t3.f2));
			}
			return Tuple4.of(finalList, meanAndVar, dimDense, labelsSort);
		}

	}

	/**
	 * Get feature types.
	 *
	 * @param in              train data.
	 * @param featureColNames feature column names.
	 * @return feature types.
	 */
	protected static String[] getFeatureTypes(BatchOperator <?> in, String[] featureColNames) {
		return getFeatureTypes(in.getSchema(), featureColNames);
	}

	protected static String[] getFeatureTypes(TableSchema schema, String[] featureColNames) {
		if (featureColNames != null) {
			String[] featureColTypes = new String[featureColNames.length];
			for (int i = 0; i < featureColNames.length; ++i) {
				int idx = TableUtil.findColIndexWithAssertAndHint(schema.getFieldNames(), featureColNames[i]);
				TypeInformation <?> type = schema.getFieldTypes()[idx];
				if (type.equals(Types.DOUBLE)) {
					featureColTypes[i] = "double";
				} else if (type.equals(Types.FLOAT)) {
					featureColTypes[i] = "float";
				} else if (type.equals(Types.LONG)) {
					featureColTypes[i] = "long";
				} else if (type.equals(Types.INT)) {
					featureColTypes[i] = "int";
				} else if (type.equals(Types.SHORT)) {
					featureColTypes[i] = "short";
				} else if (type.equals(Types.BOOLEAN)) {
					featureColTypes[i] = "bool";
				} else {
					throw new AkIllegalArgumentException(
						"Linear algorithm only support numerical data type. Current type is : " + type);
				}
			}
			return featureColTypes;
		}
		return null;
	}

	/**
	 * In this function, we do some parameters transformation, just like lambda, tau, and return the type of training:
	 * regression or classification.
	 *
	 * @param params          parameters for linear train.
	 * @param linearModelType linear model type.
	 * @param modelName       model name.
	 * @return training is regression or not.
	 */
	private static boolean getIsRegProc(Params params, LinearModelType linearModelType, String modelName) {
		if (linearModelType.equals(LinearModelType.LinearReg)) {
			if ("Ridge Regression".equals(modelName)) {
				double lambda = params.get(RidgeRegTrainParams.LAMBDA);
				AkPreconditions.checkState((lambda > 0), "Lambda must be positive number or zero! lambda is : " +
					lambda);
				params.set(HasL2.L_2, lambda);
				params.remove(RidgeRegTrainParams.LAMBDA);
			} else if ("LASSO".equals(modelName)) {
				double lambda = params.get(LassoRegTrainParams.LAMBDA);
				if (lambda < 0) {
					throw new AkIllegalArgumentException("Lambda must be positive number or zero!");
				}
				params.set(HasL1.L_1, lambda);
				params.remove(RidgeRegTrainParams.LAMBDA);
			}
			return true;
		} else if (linearModelType.equals(LinearModelType.SVR)) {
			Double tau = params.get(LinearSvrTrainParams.TAU);
			double cParam = params.get(LinearSvrTrainParams.C);
			if (tau < 0) {
				throw new AkIllegalArgumentException("Parameter tau must be positive number or zero!");
			}
			if (cParam <= 0) {
				throw new AkIllegalArgumentException("Parameter C must be positive number!");
			}

			params.set(HasL2.L_2, 1.0 / cParam);
			params.remove(LinearSvrTrainParams.C);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Build model data.
	 *
	 * @param meta            meta info.
	 * @param featureNames    feature column names.
	 * @param labelType       label type.
	 * @param meanVar         mean and variance of vector.
	 * @param hasIntercept    has interception or not.
	 * @param standardization do standardization or not.
	 * @param coefVector      coefficient vector.
	 * @return linear mode data.
	 */
	public static LinearModelData buildLinearModelData(Params meta,
													   String[] featureNames,
													   TypeInformation <?> labelType,
													   DenseVector[] meanVar,
													   boolean hasIntercept,
													   boolean standardization,
													   Tuple2 <DenseVector, double[]> coefVector) {
		int k1 = 1;
		if (LinearModelType.Softmax == meta.get(ModelParamName.LINEAR_MODEL_TYPE)) {
			k1 = meta.get(ModelParamName.NUM_CLASSES) - 1;
		}

		if (!(LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE)))) {
			modifyMeanVar(standardization, meanVar);
		}
		meta.set(ModelParamName.VECTOR_SIZE, coefVector.f0.size() / k1
			- (meta.get(ModelParamName.HAS_INTERCEPT_ITEM) ? 1 : 0)
			- (LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE)) ? 1 : 0));

		if (standardization) {
			if (!(LinearModelType.AFT.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE)))) {
				if (LinearModelType.Softmax.equals(meta.get(ModelParamName.LINEAR_MODEL_TYPE))) {
					if (hasIntercept) {
						int vecSize = meanVar[0].size();
						for (int i = 0; i < k1; ++i) {
							double sum = 0.0;
							for (int j = 1; j < vecSize; ++j) {
								int idx = i * vecSize + j;
								sum += coefVector.f0.get(idx) * meanVar[0].get(j) / meanVar[1].get(j);
								coefVector.f0.set(idx, coefVector.f0.get(idx) / meanVar[1].get(j));
							}
							coefVector.f0.set(i * vecSize, coefVector.f0.get(i * vecSize) - sum);
						}
					} else {
						for (int i = 0; i < coefVector.f0.size(); ++i) {
							int idx = i % meanVar[1].size();
							coefVector.f0.set(i, coefVector.f0.get(i) / meanVar[1].get(idx));
						}
					}
				} else {
					int n = meanVar[0].size();
					if (hasIntercept) {
						double sum = 0.0;
						for (int i = 1; i < n; ++i) {
							sum += coefVector.f0.get(i) * meanVar[0].get(i) / meanVar[1].get(i);
							coefVector.f0.set(i, coefVector.f0.get(i) / meanVar[1].get(i));
						}
						coefVector.f0.set(0, coefVector.f0.get(0) - sum);
					} else {
						for (int i = 0; i < n; ++i) {
							coefVector.f0.set(i, coefVector.f0.get(i) / meanVar[1].get(i));
						}
					}
				}
			}
		}

		LinearModelData modelData = new LinearModelData(labelType, meta, featureNames, coefVector.f0);
		modelData.labelName = meta.get(LinearTrainParams.LABEL_COL);
		modelData.featureTypes = meta.get(ModelParamName.FEATURE_TYPES);

		return modelData;
	}

	/**
	 * modify mean and variance, if variance equals zero, then modify them.
	 *
	 * @param standardization do standardization or not.
	 * @param meanVar         mean and variance.
	 */
	private static void modifyMeanVar(boolean standardization, DenseVector[] meanVar) {
		if (standardization) {
			for (int i = 0; i < meanVar[1].size(); ++i) {
				if (meanVar[1].get(i) == 0) {
					meanVar[1].set(i, 1.0);
					meanVar[0].set(i, 0.0);
				}
			}
		}
	}

	@Override
	public LinearModelTrainInfo createTrainInfo(List <Row> rows) {
		return new LinearModelTrainInfo(rows);
	}

	@Override
	public LocalOperator <?> getSideOutputTrainInfo() {
		return this.getSideOutput(0);
	}
}