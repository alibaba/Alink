package com.alibaba.alink.operator.common.finance;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.vector.VectorAssemblerMapper;
import com.alibaba.alink.operator.common.linear.FeatureLabelUtil;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.params.classification.LinearModelMapperParams;
import com.alibaba.alink.params.finance.ScorePredictParams;
import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScorePredictMapper extends ModelMapper {
	private static final long serialVersionUID = -6096135125528711852L;
	private int vectorColIndex = -1;
	private LinearModelData model;
	private int[] featureIdx;
	private int featureN;
	private boolean calculateScore;
	private boolean calculateScorePerFeature;
	private boolean calculateDetail;
	private int[] selectedIndices;

	public ScorePredictMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		//from LinearModelMapper
		if (null != params) {
			String vectorColName = params.get(LinearModelMapperParams.VECTOR_COL);
			if (null != vectorColName && vectorColName.length() != 0) {
				this.vectorColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(),
					vectorColName);
			}
		}
	}

	protected Double predictScore(Vector aVector) {
		return MatVecOp.dot(aVector, model.coefVector);
	}

	protected double[] predictBinsScore(Vector aVector) {
		if (aVector instanceof SparseVector) {
			double[] values = ((SparseVector) aVector).getValues();
			int[] indices = ((SparseVector) aVector).getIndices();
			int dim = values.length;
			double[] scores = new double[dim];
			for (int i = 0; i < dim; i++) {
				scores[i] = values[i] * model.coefVector.get(indices[i]);
			}
			return scores;
		} else {
			double[] values = ((DenseVector) aVector).getData();
			int dim = values.length;
			double[] score = new double[dim];
			for (int i = 0; i < dim; i++) {
				score[i] = values[i] * model.coefVector.get(i);
			}
			return score;
		}
	}

	protected Object predictResult(Vector aVector) throws Exception {
		double dotValue = MatVecOp.dot(aVector, model.coefVector);
		double prob = Math.exp(-dotValue);
		if (Double.isNaN(prob) || Double.isInfinite(prob)) {
			prob = 0;
		} else {
			prob = 1.0 / (1.0 + prob);
		}
		return prob;
	}

	//mapper operation.
	@Override
	public void loadModel(List <Row> modelRows) {
		LinearModelDataConverter linearModelDataConverter
			= new LinearModelDataConverter(LinearModelDataConverter.extractLabelType(super.getModelSchema()));
		this.model = linearModelDataConverter.load(modelRows);
		if (vectorColIndex == -1) {
			TableSchema dataSchema = getDataSchema();
			if (this.model.featureNames != null) {
				this.featureN = this.model.featureNames.length;
				this.featureIdx = new int[this.featureN];
				String[] predictTableColNames = dataSchema.getFieldNames();
				for (int i = 0; i < this.featureN; i++) {
					this.featureIdx[i] = TableUtil.findColIndexWithAssertAndHint(predictTableColNames,
						this.model.featureNames[i]);
				}
				String[] selectedCols = params.get(HasFeatureCols.FEATURE_COLS);
				selectedIndices = TableUtil.findColIndicesWithAssertAndHint(selectedCols, this.model.featureNames);
			} else {
				vectorColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(),
					model.vectorColName);
			}
		}

	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		//if has intercept, then add the intercept.
		Vector aVector = getFeatureVector(selection, model.hasInterceptItem, this.featureN,
			this.featureIdx,
			this.vectorColIndex, model.vectorSize);
		//here do not include linear model mapper, because the predictResultDetail in linear model mapper is protected.
		double[] scores = predictBinsScore(aVector);
		double sum = 0;
		for (double v : scores) {
			sum += v;
		}
		int index = 0;
		if (calculateDetail) {
			result.set(index, predictResultDetail(sum));
			index++;
		}
		if (calculateScore) {
			result.set(index, sum);
			index++;
		}
		if (calculateScorePerFeature) {
			//whether need to use the feature index?
			for (int i = 1; i < scores.length; i++) {
				if (selectedIndices != null) {
					result.set(index + selectedIndices[i - 1], scores[i]);
				} else {
					result.set(index + i - 1, scores[i]);
				}
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		String[] reservedColNames = params.get(RichModelMapperParams.RESERVED_COLS);
		calculateScore = params.contains(ScorePredictParams.PREDICTION_SCORE_COL);
		calculateDetail = params.contains(ScorePredictParams.PREDICTION_DETAIL_COL);
		calculateScorePerFeature = params.get(ScorePredictParams.CALCULATE_SCORE_PER_FEATURE);
		List <String> helperColNames = new ArrayList <>();
		List <TypeInformation> helperColTypes = new ArrayList <>();
		if (calculateDetail) {
			String predDetailColName = params.get(ScorePredictParams.PREDICTION_DETAIL_COL);
			helperColNames.add(predDetailColName);
			helperColTypes.add(Types.STRING);
		}
		if (calculateScore) {
			String predictionScore = params.get(ScorePredictParams.PREDICTION_SCORE_COL);
			helperColNames.add(predictionScore);
			helperColTypes.add(Types.DOUBLE);
		}
		if (calculateScorePerFeature) {
			String[] predictionScorePerFeature = params.get(ScorePredictParams.PREDICTION_SCORE_PER_FEATURE_COLS);
			helperColNames.addAll(Arrays.asList(predictionScorePerFeature));
			for (String aPredictionScorePerFeature : predictionScorePerFeature) {
				helperColTypes.add(Types.DOUBLE);
			}
		}

		return Tuple4.of(this.getDataSchema().getFieldNames(),
			helperColNames.toArray(new String[0]),
			helperColTypes.toArray(new TypeInformation[0]),
			reservedColNames);
	}


	// linear model mapper function
	protected String predictResultDetail(double sum) {
		Double[] result = predictWithProb(sum);
		Map <String, String> detail = new HashMap <>(1);
		int labelSize = model.labelValues.length;
		for (int i = 0; i < labelSize; ++i) {
			detail.put(model.labelValues[i].toString(), result[i].toString());
		}
		return JsonConverter.toJson(detail);
	}

	private Double[] predictWithProb(double sum) {
		double prob = sigmoid(sum);
		return new Double[] {prob, 1 - prob};

	}

	private double sigmoid(double val) {
		return 1 - 1.0 / (1.0 + Math.exp(val));
	}

	/**
	 * Retrieve the feature vector from the input row data.
	 */
	private static Vector getFeatureVector(SlicedSelectedSample selection, boolean hasInterceptItem, int featureN,
										   int[] featureIdx,
										   int vectorColIndex, Integer vectorSize) {
		Vector aVector;
		if (vectorColIndex != -1) {
			Vector vec = VectorUtil.getVector(selection.get(vectorColIndex));
			if (vec instanceof SparseVector) {
				SparseVector tmp = (SparseVector) vec;
				if (null != vectorSize) {
					tmp.setSize(vectorSize);
				}
				aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
			} else {
				DenseVector tmp = (DenseVector) vec;
				aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
			}
		} else {
			if (hasInterceptItem) {
				Object[] objs = new Object[featureN + 1];
				objs[0] = 1.0;
				for (int i = 0; i < featureN; i++) {
					objs[1 + i] = VectorUtil.getVector(selection.get(featureIdx[i]));
				}
				aVector = (Vector) VectorAssemblerMapper.assembler(objs);
			} else {
				Object[] objs = new Object[featureN];
				for (int i = 0; i < featureN; i++) {
					objs[i] = VectorUtil.getVector(selection.get(featureIdx[i]));
				}
				aVector = (Vector) VectorAssemblerMapper.assembler(objs);
			}
		}
		return aVector;
	}

}
