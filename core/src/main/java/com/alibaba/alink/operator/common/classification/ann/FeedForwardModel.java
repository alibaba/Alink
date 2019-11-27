package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import java.util.ArrayList;
import java.util.List;

/**
 * The TopologyModel for {@link FeedForwardTopology}.
 */
public class FeedForwardModel extends TopologyModel {
    private List<Layer> layers;
    private List<LayerModel> layerModels;

    /**
     * Buffers of outputs of each layers.
     */
    private transient List<DenseMatrix> outputs = null;

    /**
     * Buffers of deltas of each layers.
     */
    private transient List<DenseMatrix> deltas = null;

    public FeedForwardModel(List<Layer> layers) {
        this.layers = layers;
        this.layerModels = new ArrayList<>(layers.size());
        for (int i = 0; i < layers.size(); i++) {
            layerModels.add(layers.get(i).createModel());
        }
    }

    @Override
    public void resetModel(DenseVector weights) {
        int offset = 0;
        for (int i = 0; i < layers.size(); i++) {
            layerModels.get(i).resetModel(weights, offset);
            offset += layers.get(i).getWeightSize();
        }
    }

    @Override
    public List<DenseMatrix> forward(DenseMatrix data, boolean includeLastLayer) {
        int currentBatchSize = data.numRows();
        if (outputs == null || outputs.get(0).numRows() != currentBatchSize) {
            outputs = new ArrayList<>(layers.size());
            int inputSize = data.numCols();
            for (int i = 0; i < layers.size(); i++) {
                if (layers.get(i).isInPlace()) {
                    outputs.add(outputs.get(i - 1));
                } else {
                    int outputSize = layers.get(i).getOutputSize(inputSize);
                    outputs.add(new DenseMatrix(currentBatchSize, outputSize));
                    inputSize = outputSize;
                }
            }
        }
        layerModels.get(0).eval(data, outputs.get(0));
        int end = includeLastLayer ? layers.size() : layers.size() - 1;
        for (int i = 1; i < end; i++) {
            layerModels.get(i).eval(outputs.get(i - 1), outputs.get(i));
        }
        return outputs;
    }

    @Override
    public DenseVector predict(DenseVector features) {
        DenseMatrix data = new DenseMatrix(1, features.size(), features.getData().clone());
        List<DenseMatrix> result = forward(data, true);
        return new DenseVector(result.get(result.size() - 1).getData().clone());
    }

    @Override
    public double computeGradient(DenseMatrix data, DenseMatrix target, DenseVector cumGrad) {
        outputs = forward(data, true);
        int currentBatchSize = data.numRows();
        if (deltas == null || deltas.get(0).numRows() != currentBatchSize) {
            deltas = new ArrayList<>(layers.size() - 1);
            int inputSize = data.numCols();
            for (int i = 0; i < layers.size() - 1; i++) {
                int outputSize = layers.get(i).getOutputSize(inputSize);
                deltas.add(new DenseMatrix(currentBatchSize, outputSize));
                inputSize = outputSize;
            }
        }
        int L = layerModels.size() - 1;
        if (!(this.layerModels.get(L) instanceof AnnLossFunction)) {
            throw new UnsupportedOperationException("The last layer should be loss function");
        }
        AnnLossFunction labelWithError = (AnnLossFunction) this.layerModels.get(L);
        double loss = labelWithError.loss(outputs.get(L), target, deltas.get(L - 1));
        if (cumGrad == null) {
            return loss;
        }
        for (int i = L - 1; i >= 1; i--) {
            layerModels.get(i).computePrevDelta(deltas.get(i), outputs.get(i), deltas.get(i - 1));
        }
        int offset = 0;
        for (int i = 0; i < layerModels.size(); i++) {
            DenseMatrix input = i == 0 ? data : outputs.get(i - 1);
            if (i == layerModels.size() - 1) {
                layerModels.get(i).grad(null, input, cumGrad, offset);
            } else {
                layerModels.get(i).grad(deltas.get(i), input, cumGrad, offset);
            }
            offset += layers.get(i).getWeightSize();
        }
        return loss;
    }
}
