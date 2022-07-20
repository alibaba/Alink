package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseVector;

import java.util.ArrayList;
import java.util.List;

/**
 * The topology of a feed forward neural network.
 */
public class FeedForwardTopology extends Topology {
	private static final long serialVersionUID = 5210357186185961635L;
	/**
	 * All layers of the topology.
	 */
	private final List <Layer> layers;

	public FeedForwardTopology(List <Layer> layers) {
		this.layers = layers;
	}

	public static FeedForwardTopology multiLayerPerceptron(int[] layerSize, boolean softmaxOnTop) {
		List <Layer> layers = new ArrayList <>((layerSize.length - 1) * 2);
		for (int i = 0; i < layerSize.length - 1; i++) {
			layers.add(new AffineLayer(layerSize[i], layerSize[i + 1]));
			if (i == layerSize.length - 2) {
				layers.add(new SoftmaxLayerWithCrossEntropyLoss());
			} else {
				layers.add(new FuntionalLayer(new SigmoidFunction()));
			}
		}
		return new FeedForwardTopology(layers);
	}

	@Override
	public TopologyModel getModel(DenseVector weights) {
		FeedForwardModel feedForwardModel = new FeedForwardModel(this.layers);
		feedForwardModel.resetModel(weights);
		return feedForwardModel;
	}

	@Override
	public int getWeightSize() {
		int s = 0;
		for (Layer layer : layers) {
			s += layer.getWeightSize();
		}
		return s;
	}
}
