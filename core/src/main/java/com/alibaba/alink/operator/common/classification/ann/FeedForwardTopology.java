package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseVector;

import java.util.ArrayList;
import java.util.List;

/**
 * The topology of a feed forward neural network.
 */
public class FeedForwardTopology extends Topology {
    /**
     * All layers of the topology.
     */
    private List<Layer> layers;

    public FeedForwardTopology(List<Layer> layers) {
        this.layers = layers;
    }

    public static FeedForwardTopology multiLayerPerceptron(int[] layerSize, boolean softmaxOnTop) {
        List<Layer> layers = new ArrayList<>((layerSize.length - 1) * 2);
        for (int i = 0; i < layerSize.length - 1; i++) {
            layers.add(new AffineLayer(layerSize[i], layerSize[i + 1]));
            if (i == layerSize.length - 2) {
                if (softmaxOnTop) {
                    layers.add(new SoftmaxLayerWithCrossEntropyLoss());
                } else {
                    layers.add(new SigmoidLayerWithSquaredError());
                }
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
