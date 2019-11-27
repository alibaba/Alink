package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.JaccardDistance;

/**
 * Implements Locality Sensitive Hashing functions for Jaccard distance metrics.
 * <p>
 * Hash a vector in the Jaccard distance space to a new vector of given dimensions.
 * <p>
 * Tom Bohman, Colin Cooper, and Alan Frieze. "Min-wise independent linear permutations." Electronic Journal of
 * Combinatorics 7 (2000): R26.
 */
public class MinHashLSH extends BaseLSH {
    private static int HASH_PRIME = 2038074743;

    int[][] randCoefficientsA, randCoefficientsB;

    public MinHashLSH(long seed, int numProjectionsPerTable, int numHashTables) {
        super(seed, new JaccardDistance());
        randCoefficientsA = new int[numHashTables][numProjectionsPerTable];
        randCoefficientsB = new int[numHashTables][numProjectionsPerTable];
        for (int i = 0; i < numHashTables; i++) {
            for (int j = 0; j < numProjectionsPerTable; j++) {
                randCoefficientsA[i][j] = 1 + random.nextInt(HASH_PRIME - 1);
                randCoefficientsB[i][j] = random.nextInt(HASH_PRIME - 1);
            }
        }
    }

    public MinHashLSH(int[][] randCoefficientsA, int[][] randCoefficientsB){
        super(new JaccardDistance());
        this.randCoefficientsA = randCoefficientsA;
        this.randCoefficientsB = randCoefficientsB;
    }

    /**
     * indices: indexes of data in vec whose values are not zero.
     * <p>
     * hashValue = (((1 + indices) * randCoefficientA + randCoefficientB) % HASH_PRIME).min.
     * <p>
     * Here randCoefficientA and randCoefficientB are all real numbers chosen uniformly from the range [0,
     * HASH_PRIME-1].
     */
    @Override
    public DenseVector hashFunction(Vector vec) {
        double[] minHashSet = new double[randCoefficientsA.length];
        if (randCoefficientsA.length > 0) {
            int[] hashValues = new int[randCoefficientsA[0].length];
            if (vec instanceof SparseVector) {
                SparseVector elem = (SparseVector)vec;
                int[] indices = elem.getIndices();
                for (int i = 0; i < minHashSet.length; i++) {
                    for (int j = 0; j < hashValues.length; j++) {
                        int tmp = HASH_PRIME, cur;
                        for (int index : indices) {
                            cur = (int)((1L + index) * randCoefficientsA[i][j] + randCoefficientsB[i][j])
                                % HASH_PRIME;
                            tmp = Math.min(tmp, cur);
                        }
                        hashValues[j] = tmp;
                    }
                    minHashSet[i] = tableHash(hashValues);
                }
            } else if (vec instanceof DenseVector) {
                double[] elem = ((DenseVector)vec).getData();
                for (int i = 0; i < minHashSet.length; i++) {
                    for (int j = 0; j < hashValues.length; j++) {
                        int tmp = HASH_PRIME, cur;
                        for (int m = 0; m < elem.length; m++) {
                            if (elem[m] != 0) {
                                cur = (int)((1L + m) * randCoefficientsA[i][j] + randCoefficientsB[i][j]) % HASH_PRIME;
                                tmp = Math.min(tmp, cur);
                            }
                        }
                        hashValues[j] = tmp;
                    }
                    minHashSet[i] = tableHash(hashValues);
                }
            }
        }
        return new DenseVector(minHashSet);
    }

}
