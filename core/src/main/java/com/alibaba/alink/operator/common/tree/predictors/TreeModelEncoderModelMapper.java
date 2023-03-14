package com.alibaba.alink.operator.common.tree.predictors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.common.tree.Node;

import java.util.Arrays;
import java.util.List;

/**
 * Mapper class to encode the instance tree model.
 */
public class TreeModelEncoderModelMapper extends TreeModelMapper {
	private static final long serialVersionUID = 4543856042065853798L;
	/**
	 * for constructor of {@link SparseVector}
	 */
	private int dim;

	/**
	 * Wrapper class to wrap id and node together.
	 */
	private NodeWithId[] roots;

	private transient ThreadLocal <Row> inputBufferThreadLocal;

	public TreeModelEncoderModelMapper(
		TableSchema modelSchema,
		TableSchema dataSchema,
		Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected TypeInformation <?> initPredResultColType(TableSchema modelSchema) {
		return AlinkTypes.SPARSE_VECTOR;
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		init(modelRows);

		roots = new NodeWithId[treeModel.roots.length];

		dim = encode();

		inputBufferThreadLocal = ThreadLocal.withInitial(() -> new Row(ioSchema.f0.length));
	}

	private int encodeDFS(NodeWithId nodeWithId, int code) {
		if (nodeWithId.node.isLeaf()) {
			nodeWithId.setId(code);
			return code + 1;
		}

		int len = nodeWithId.node.getNextNodes().length;

		nodeWithId.setNext(new NodeWithId[len]);
		for (int i = 0; i < len; ++i) {
			nodeWithId.next[i] = new NodeWithId()
				.setNode(nodeWithId.node.getNextNodes()[i]);
			code = encodeDFS(nodeWithId.next[i], code);
		}

		return code;
	}

	private int encode() {
		int code = 0;
		for (int i = 0; i < roots.length; ++i) {
			roots[i] = new NodeWithId().setNode(treeModel.roots[i]);
			code = encodeDFS(roots[i], code);
		}

		return code;
	}

	private int selectMaxWeightedCriteriaOfChild(Node node) {
		if (node.getMissingSplit() != null && node.getMissingSplit().length == 1) {
			return node.getMissingSplit()[0];
		}

		int maxIndex = 0;
		double maxWeightedCriteria = 0.;

		int index = 0;
		for (Node child : node.getNextNodes()) {
			if (child.getCounter() != null) {
				double weightedCriteria = child.getCounter().getWeightSum();
				if (weightedCriteria > maxWeightedCriteria) {
					maxWeightedCriteria = weightedCriteria;
					maxIndex = index;
				}
			}

			index++;
		}

		return maxIndex;
	}

	private int predictWithId(Row row, NodeWithId nodeWithId) {
		if (nodeWithId.node.isLeaf()) {
			return nodeWithId.id;
		}

		int featureIndex = nodeWithId.node.getFeatureIndex();

		int predictionFeatureIndex = featuresIndex[featureIndex];

		if (predictionFeatureIndex < 0) {
			throw new IllegalArgumentException("Can not find train column index: " + featureIndex);
		}

		Object featureValue = row.getField(predictionFeatureIndex);

		if (featureValue == null) {
			return predictWithId(row, nodeWithId.next[selectMaxWeightedCriteriaOfChild(nodeWithId.node)]);
		} else {
			int[] categoryHash = nodeWithId.node.getCategoricalSplit();

			if (categoryHash == null) {
				if ((Double) featureValue <= nodeWithId.node.getContinuousSplit()) {
					return predictWithId(row, nodeWithId.next[0]);
				} else {
					return predictWithId(row, nodeWithId.next[1]);
				}
			} else {
				int hashValue = categoryHash[(Integer) featureValue];
				if (hashValue < 0) {
					return predictWithId(row, nodeWithId.next[selectMaxWeightedCriteriaOfChild(nodeWithId.node)]);
				} else {
					return predictWithId(row, nodeWithId.next[hashValue]);
				}
			}
		}
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Row inputBuffer = inputBufferThreadLocal.get();
		selection.fillRow(inputBuffer);

		transform(inputBuffer);

		int len = roots.length;

		Object result = null;

		if (len > 0) {
			int[] indices = new int[treeModel.roots.length];
			double[] data = new double[treeModel.roots.length];
			Arrays.fill(data, 1.0);

			for (int i = 0; i < len; ++i) {
				indices[i] = predictWithId(inputBuffer, roots[i]);
			}

			result = new SparseVector(dim, indices, data);
		}

		return new Tuple2 <>(result, null);
	}

	private static class NodeWithId {
		Node node;
		int id;
		NodeWithId[] next;

		public NodeWithId setNode(Node node) {
			this.node = node;
			return this;
		}

		public NodeWithId setId(int id) {
			this.id = id;
			return this;
		}

		public NodeWithId setNext(NodeWithId[] next) {
			this.next = next;
			return this;
		}
	}
}
