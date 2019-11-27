package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.LabelCounter;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SaveModel extends CompleteResultFunction {
	public final static ParamInfo<Double> GBDT_Y_PERIOD = ParamInfoFactory
		.createParamInfo("gbdt.y.period", Double.class)
		.setDescription("gbdt.y.period")
		.setRequired()
		.build();
	private static final Logger LOG = LoggerFactory.getLogger(SaveModel.class);
	private int algoType;
	private int binNum;
	private Params meta;

	public SaveModel(int algoType, int binNum, Params meta) {
		this.algoType = algoType;
		this.binNum = binNum;
		this.meta = meta;
	}

	@Override
	public List<Row> calc(ComContext context) {
		if (context.getTaskId() != 0) {
			return null;
		}

		List<Row> quantileModelSerialized = context.getObj("quantileModel");
		QuantileDiscretizerModelDataConverter quantileModel = new QuantileDiscretizerModelDataConverter();
		if (quantileModelSerialized != null) {
			quantileModel.load(quantileModelSerialized);
		}
		List<Tuple2<Double, Integer>> sum = context.getObj("gbdt.y.sum");

		if (algoType == 0) {
			meta.set(GBDT_Y_PERIOD, sum.get(0).f0 / ((double) sum.get(0).f1));
		} else {
			meta.set(GBDT_Y_PERIOD, 0.0);
		}

		Iterable<TreeParaContainer> valueRaw = context.getObj("tree");
		TreeParaContainer value = valueRaw.iterator().next();

		List<Tuple2<Integer, Node>> roots = new ArrayList<>();
		//save model here
		for (TreeParaContainer.TreeParas para : value.getTreeParas()) {
			int[] featureIdx = para.getFeatureIdx();
			int[] splitPoint = para.getSplitPoints();
			boolean[] isCategorical = para.getIsCategorical();
			ArrayList<ArrayList<Boolean>> childHash = para.getChildHash();

			Criteria.MSE[] counters = para.getCounters();

			int len = featureIdx.length;

			Node[] root = new Node[len];
			for (int i = 0; i < len; ++i) {

				if (featureIdx[i] >= 0) {
					//not leaf
					Node cur = new Node()
						.setFeatureIndex(featureIdx[i]);

					if (i == 0 && counters[i] == null) {
						cur.setCounter(new LabelCounter(0, 0, new double[]{0.0, 0.0}));
					} else {
						cur.setCounter(counters[i].toLabelCounter());
					}

					//categorical data
					if (isCategorical[i]) {
						int[] realChildHash = new int[childHash.get(i).size()];
						for (int index = 0; index < realChildHash.length; index++) {
							realChildHash[index] = childHash.get(i).get(index) ? 1 : 0;
						}
						cur.setCategoricalSplit(realChildHash);
					} else {
						cur.setContinuousSplit(quantileModel
							.getData()
							.get(meta.get(HasFeatureCols.FEATURE_COLS)[featureIdx[i]])[splitPoint[i]]);
					}

					root[i] = cur;
				} else if (i == 0 || featureIdx[TreeParaContainer.TreeParas.parentIdx(i)] >= 0) {
					//for leaf

					root[i] = new Node()
						.makeLeaf();
					if (counters[i] == null) {
						root[i].setCounter(new Criteria.MSE(0, 0, 0, 0).toLabelCounter());
					} else {
						root[i].setCounter(counters[i].toLabelCounter())
							.makeLeafProb();
					}
				}
			}

			//set tree structure
			for (int i = 0; i < len; ++i) {
				if (root[i] != null && !root[i].isLeaf()) {
					Node[] nextNodes = new Node[2];

					nextNodes[0] = root[TreeParaContainer.TreeParas.childLeftIdx(i)];
					nextNodes[1] = root[TreeParaContainer.TreeParas.childRightIdx(i)];

					root[i].setNextNodes(nextNodes);
				}
			}

			roots.add(Tuple2.of(para.getTreeId(), root[0]));
		}

		roots.sort((o1, o2) -> o1.f0.compareTo(o2.f0));

		List<Row> stringIndexerModel = context.getObj("stringIndexerModel");
		List<Object[]> labelsList = context.getObj("labels");

		List<Row> output = TreeModelDataConverter.saveModelWithData(
			roots.stream().map(x -> x.f1).collect(Collectors.toList()),
			meta,
			stringIndexerModel,
			labelsList == null || labelsList.isEmpty() ? null : labelsList.get(0)
		);
		return output;
	}
}
