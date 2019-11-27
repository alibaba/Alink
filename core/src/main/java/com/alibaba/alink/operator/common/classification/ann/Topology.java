package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseVector;

import java.io.Serializable;

/**
 * Base class for neural network topology.
 */
public abstract class Topology implements Serializable {
    /**
     * Create the TopologyModel for this topology.
     *
     * @param weights The weights to initialize the TopologyModel.
     * @return A newly created TopologyModel.
     */
    public abstract TopologyModel getModel(DenseVector weights);

    /**
     * Get the size of weights of this topology.
     *
     * @return Size of weight.
     */
    public abstract int getWeightSize();
}
