package com.alibaba.alink.operator.common.tree.predictors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.itree.IForest;

import java.util.List;

public class IForestsModelMapper extends TreeModelMapper {
	public final static double EULER_CONSTANT = 0.5772156649;
	private static final long serialVersionUID = 4303316486225031625L;
	private double totalC;

	private transient ThreadLocal <Row> inputBufferThreadLocal;

	public IForestsModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		init(modelRows);

		totalC = c(treeModel.meta.get(IForest.ISOLATION_GROUP_MAXCOUNT));

		inputBufferThreadLocal = ThreadLocal.withInitial(() -> new Row(ioSchema.f0.length));
	}

	public double predict(Row row, Node node, int pathLen) {
		if (node.isLeaf()) {
			return pathLen + c(node.getCounter().getWeightSum());
		}

		int featureIndex = node.getFeatureIndex();

		int predictionFeatureIndex = featuresIndex[featureIndex];

		if (predictionFeatureIndex < 0) {
			throw new RuntimeException("Can not find train column index: " + featureIndex);
		}

		Object featureValue = row.getField(predictionFeatureIndex);

		if (featureValue == null) {
			throw new RuntimeException("Unsupported missing now.");
		} else {
			int[] categoryHash = node.getCategoricalSplit();

			if (categoryHash == null) {
				if ((Double) featureValue
					<= node.getContinuousSplit()) {
					//left
					return predict(row, node.getNextNodes()[0], pathLen + 1);
				} else {
					//right
					return predict(row, node.getNextNodes()[1], pathLen + 1);
				}
			} else {
				throw new RuntimeException("Unsupported categorical now.");
			}
		}
	}

	private double c(double n) {
		if (n <= 1.0) {
			return 0;
		}
		return 2.0 * (Math.log(n - 1) + EULER_CONSTANT) - (2.0 * (n - 1) / n);
	}

	private double s(double eh, double c) {
		return Math.pow(2, -1.0 * eh / c);
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Node[] root = treeModel.roots;

		Row inputBuffer = inputBufferThreadLocal.get();
		selection.fillRow(inputBuffer);

		transform(inputBuffer);

		int len = root.length;

		Object result = null;

		if (len > 0) {
			double hSum = 0.0;

			for (Node node : root) {
				hSum += predict(inputBuffer, node, 1);
			}

			result = s(hSum / len, totalC);
		}

		return new Tuple2 <>(result, null);
	}
}
