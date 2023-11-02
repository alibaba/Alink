package com.alibaba.alink.operator.common.finance.group;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.classification.LinearModelMapperParams;
import com.alibaba.alink.params.finance.GroupScorecardPredictParams;
import com.alibaba.alink.params.finance.GroupScorecardTrainParams;
import com.alibaba.alink.params.finance.ScorePredictParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleGroupScorecardModelMapper extends ModelMapper {
	private static String SCORE_SUFFIX = "_SCORE";
	private List <GroupNode> allNodes;

	private int vectorColIdx;
	private int[] groupColIndices;

	private boolean calculateScore;
	private boolean calculateScorePerFeature;
	private boolean calculateDetail;

	public Object[] labelValues = null;

	public TypeInformation <?> labelType;

	public SimpleGroupScorecardModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		//from LinearModelMapper
		if (null != params) {
			String vectorColName = params.get(LinearModelMapperParams.VECTOR_COL);
			if (null != vectorColName && vectorColName.length() != 0) {
				this.vectorColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(),
					vectorColName);
			}
			String[] groupColNames = params.get(GroupScorecardTrainParams.GROUP_COLS);
			this.groupColIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema, groupColNames);
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		SimpleGroupScoreModelData data = new GroupScoreModelConverter().load(modelRows);
		this.allNodes = data.allNodes;
		this.vectorColIdx = TableUtil.findColIndex(this.getDataSchema(),
			GroupScoreCardVariable.FEATURE_BINNING_OUTPUT_COL);
		this.labelValues = data.labelValues;
		this.labelType = data.labelType;
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Vector aVector = getFeatureVector(selection, true, this.vectorColIdx);
		GroupNode curNode = allNodes.get(0);

		double[] coef = null;
		while (curNode != null && !curNode.isLeaf) {
			SplitInfo info = curNode.split;
			System.out.println("info.splitColIdx: " + info.splitColIdx);
			System.out.println("this.groupColIndices[info.splitColIdx]: " + this.groupColIndices[info.splitColIdx]);
			long groupValue = (long) selection.get(this.groupColIndices[info.splitColIdx]);
			int splitValue = info.splitValueIdx;
			int newNodeId = -1;
			if (info.isStr) {
				if (splitValue == groupValue) {
					coef = curNode.split.leftWeights;
					newNodeId = curNode.leftNode.nodeId;
				} else {
					coef = curNode.split.rightWeights;
					newNodeId = curNode.rightNode.nodeId;
				}
			} else {
				if (splitValue <= groupValue) {
					coef = curNode.split.leftWeights;
					newNodeId = curNode.leftNode.nodeId;
				} else {
					coef = curNode.split.rightWeights;
					newNodeId = curNode.rightNode.nodeId;
				}
			}
			curNode = findNode(allNodes, newNodeId);
		}
		Vector aCoef = new DenseVector(coef);

		double[] scores = predictBinsScore(aVector, aCoef);
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
			for (int i = 1; i < scores.length; i++) {
				result.set(index + i - 1, scores[i]);
			}
		}
	}

	private static GroupNode findNode(List <GroupNode> allNodes, int nodeId) {
		for (GroupNode node : allNodes) {
			if (nodeId == node.nodeId) {
				return node;
			}
		}
		return null;
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		String[] reservedColNames = params.get(GroupScorecardPredictParams.RESERVED_COLS);
		calculateScore = params.contains(GroupScorecardPredictParams.PREDICTION_SCORE_COL);
		calculateDetail = params.contains(GroupScorecardPredictParams.PREDICTION_DETAIL_COL);
		calculateScorePerFeature = params.get(GroupScorecardPredictParams.CALCULATE_SCORE_PER_FEATURE);
		List <String> helperColNames = new ArrayList <>();
		List <TypeInformation <?>> helperColTypes = new ArrayList <>();
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

	private static Vector getFeatureVector(SlicedSelectedSample selection,
										   boolean hasInterceptItem,
										   int vectorColIndex) {
		Vector aVector;

		Vector vec = VectorUtil.getVector(selection.get(vectorColIndex));
		if (vec instanceof SparseVector) {
			SparseVector tmp = (SparseVector) vec;
			aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
		} else {
			DenseVector tmp = (DenseVector) vec;
			aVector = hasInterceptItem ? tmp.prefix(1.0) : tmp;
		}

		return aVector;
	}

	protected double[] predictBinsScore(Vector aVector, Vector coefVector) {
		if (aVector instanceof SparseVector) {
			double[] values = ((SparseVector) aVector).getValues();
			int[] indices = ((SparseVector) aVector).getIndices();
			int dim = values.length;
			double[] scores = new double[dim];
			for (int i = 0; i < dim; i++) {
				scores[i] = values[i] * coefVector.get(indices[i]);
			}
			return scores;
		} else {
			double[] values = ((DenseVector) aVector).getData();
			int dim = values.length;
			double[] score = new double[dim];
			for (int i = 0; i < dim; i++) {
				score[i] = values[i] * coefVector.get(i);
			}
			return score;
		}
	}

	protected String predictResultDetail(double sum) {
		Double[] result = predictWithProb(sum);
		Map <String, String> detail = new HashMap <>(1);
		int labelSize = labelValues.length;
		for (int i = 0; i < labelSize; ++i) {
			detail.put(labelValues[i].toString(), result[i].toString());
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

}
