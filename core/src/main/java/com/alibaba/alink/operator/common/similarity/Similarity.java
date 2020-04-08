package com.alibaba.alink.operator.common.similarity;

import com.alibaba.alink.operator.common.distance.CategoricalDistance;
import com.alibaba.alink.operator.common.distance.FastCompareCategoricalDistance;

import java.io.Serializable;

/**
 * Interface for different similarity method.
 */
public abstract class Similarity<T> implements Serializable, FastCompareCategoricalDistance<T> {
    protected CategoricalDistance distance;

    public CategoricalDistance getDistance() {
        return distance;
    }

    public abstract double similarity(String left, String right);

    public abstract double similarity(String[] left, String[] right);
}
