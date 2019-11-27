package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Stack a batch of samples (represented as vectors) to a matrix.
 */
public class Stacker implements Serializable {
    public static final int BATCH_SIZE = 64;

    private int inputSize;
    private int outputSize;
    private boolean onehot;

    private transient DenseMatrix features;
    private transient DenseMatrix labels;

    public Stacker(int inputSize, int outputSize, boolean onehot) {
        this.inputSize = inputSize;
        this.outputSize = outputSize;
        this.onehot = onehot;
    }

    public Tuple3<Double, Double, Vector> stack(List<Tuple2<Double, DenseVector>> batchData, int count) {
        int l = inputSize * count + count;
        DenseVector stacked = new DenseVector(l);
        int offset = 0;
        for (int i = 0; i < count; i++) {
            System.arraycopy(batchData.get(i).f1.getData(), 0, stacked.getData(), offset, inputSize);
            offset += inputSize;
        }
        for (int i = 0; i < count; i++) {
            stacked.set(offset, batchData.get(i).f0);
            offset++;
        }
        return Tuple3.of((double) count, 0., stacked);
    }

    public Tuple2<DenseMatrix, DenseMatrix> unstack(Tuple3<Double, Double, Vector> labledVector) {
        int batchSize = labledVector.f0.intValue();
        DenseVector stacked = (DenseVector) labledVector.f2;
        if (features == null || features.numRows() != batchSize) {
            features = new DenseMatrix(batchSize, inputSize);
        }
        int offset = 0;
        for (int i = 0; i < batchSize; i++) {
            for (int j = 0; j < inputSize; j++) {
                features.set(i, j, stacked.get(offset));
                offset++;
            }
        }
        if (labels == null || labels.numRows() != batchSize) {
            labels = new DenseMatrix(batchSize, onehot ? outputSize : 1);
        }
        if (onehot) {
            Arrays.fill(labels.getData(), 0.);
            int start = batchSize * inputSize;
            for (int i = 0; i < batchSize; i++) {
                int target = (int) stacked.get(start + i);
                if (target < 0 || target >= outputSize) {
                    throw new RuntimeException("Invalid target value: " + target);
                }
                labels.set(i, target, 1.);
            }
        } else {
            int start = batchSize * inputSize;
            for (int i = 0; i < batchSize; i++) {
                labels.set(i, 0, stacked.get(start + i));
            }
        }
        return Tuple2.of(features, labels);
    }
}
