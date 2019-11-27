package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;

/**
 * Implements Locality Sensitive Hashing functions for Euclidean distance metrics. Hash a vector in the Euclidean
 * distance space to a new vector of given dimensions.
 * <p>
 * Alexandr Andoni, Piotr Indyk, Thijs Laarhoven, Ilya Razenshteyn, and Ludwig Schmidt. 2015. Practical and optimal LSH
 * for angular distance. In Proceedings of the 28th International Conference on Neural Information Processing Systems -
 * Volume 1 (NIPS'15), C. Cortes, D. D. Lee, M. Sugiyama, and R. Garnett (Eds.), Vol. 1. MIT Press, Cambridge, MA, USA,
 * 1225-1233.
 */
public class BucketRandomProjectionLSH extends BaseLSH {
    double projectionWidth;
    DenseVector[][] randVectors;
    double[][] randNumber;

    public BucketRandomProjectionLSH(long seed, int vectorSize, int numProjectionsPerTable, int numHashTables,
                                     double projectionWidth) {
        super(seed, new EuclideanDistance());
        this.projectionWidth = projectionWidth;
        this.randVectors = new DenseVector[numHashTables][numProjectionsPerTable];
        this.randNumber = new double[numHashTables][numProjectionsPerTable];
        for (int i = 0; i < numHashTables; i++) {
            for (int j = 0; j < numProjectionsPerTable; j++) {
                double[] data = new double[vectorSize];
                for (int m = 0; m < vectorSize; m++) {
                    data[m] = random.nextGaussian();
                }
                this.randVectors[i][j] = new DenseVector(data);
                this.randVectors[i][j].normalizeEqual(2.0);
                this.randNumber[i][j] = random.nextDouble() * projectionWidth;
            }
        }
    }

    public BucketRandomProjectionLSH(DenseVector[][] randVectors, double[][] randNumber, double projectionWidth){
        super(new EuclideanDistance());
        this.randVectors =randVectors;
        this.randNumber = randNumber;
        this.projectionWidth = projectionWidth;
    }

    /**
     * hashValue = floor((dot(elem, randVector) + randNumber) / projectionWidth)
     * <p>
     * Each hash function maps a d dimensional vector onto the set of integers.
     * <p>
     * Here, `randVectors` are d dimensional vectors with entries chosen independently from a p-stable distribution and
     * randNumbers are real numbers chosen uniformly from the range [0, projectionWidth].
     */
    @Override
    public DenseVector hashFunction(Vector elemVec) {
        double[] brpSet = new double[randVectors.length];
        if (randVectors.length > 0) {
            int[] hashValues = new int[randVectors[0].length];
            for (int i = 0; i < brpSet.length; i++) {
                for (int j = 0; j < hashValues.length; j++) {
                    double dot = elemVec.dot(randVectors[i][j]);
                    hashValues[j] = (int) Math.floor((dot + randNumber[i][j]) / projectionWidth);
                }
                brpSet[i] = tableHash(hashValues);
            }
        }
        return new DenseVector(brpSet);
    }
}
