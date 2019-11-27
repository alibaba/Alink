package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;

import java.io.Serializable;
import java.util.Random;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

/**
 * LocalitySensitiveHash is the base class of Locality Sensitive Hash Algorithm(LSH). It reduces the dimensionality of
 * high-dimensional data by hashing input items to buckets so that similar items will be mapped to the same buckets with
 * high probabilities. LSH is usually used to search nearest neighbor or join similar items. Here, we realized the
 * function of approxNearestNeighbors and approxSimilarityJoin for Euclidean distance and Jaccard distance.
 * <p>
 * Mayur Datar, Nicole Immorlica, Piotr Indyk, and Vahab S. Mirrokni. 2004. Locality-sensitive hashing scheme based on
 * p-stable distributions. In Proceedings of the twentieth annual symposium on Computational geometry (SCG '04). ACM,
 * New York, NY, USA, 253-262.
 * <p>
 * Alexandr Andoni and Piotr Indyk. 2008. Near-optimal hashing algorithms for approximate nearest neighbor in high
 * dimensions. Commun. ACM 51, 1 (January 2008), 117-122.
 */
public abstract class BaseLSH implements Serializable {
    Random random;

    /**
     * Support Euclidean Distance and Jaccard distance.
     */
    private final ContinuousDistance distance;

    private static final HashFunction HASH = murmur3_32(0);

    public BaseLSH(long seed, ContinuousDistance distance) {
        this.distance = distance;
        this.random = new Random(seed);
    }

    public BaseLSH(ContinuousDistance distance){
        this.distance = distance;
    }

    /**
     * Hashes the vector.
     *
     * @param vec target vector, support sparse vector and dense vector.
     * @return the hash value array.
     */
    abstract DenseVector hashFunction(Vector vec);

    /**
     * Calculates the distance between x and y.
     */
    double keyDistance(Vector x, Vector y) {
        return distance.calc(x, y);
    }

    /**
     * Each hash table has `numProjectionsPerTable` hash functions, and there are `numHashTables` hash tables.
     */
    public static int tableHash(int[] hashValues) {
        byte[] targets = new byte[4 * hashValues.length];
        for(int i = 0; i < hashValues.length; i++){
            intToByte4(hashValues[i], i * 4, targets);
        }
        return HASH.hashBytes(targets).asInt();
    }

    private static void intToByte4(int number, int startIndex, byte[] targets) {
        targets[3 + startIndex] = (byte) (number & 0xFF);
        targets[2 + startIndex] = (byte) (number >> 8 & 0xFF);
        targets[1 + startIndex] = (byte) (number >> 16 & 0xFF);
        targets[startIndex] = (byte) (number >> 24 & 0xFF);
    }

}
