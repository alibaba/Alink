package com.alibaba.alink.operator.common.statistics.basicstatistic;

/**
 * It is median result of sparse vector summary.
 */
public class VectorStatCol {
    /**
     * number of non-zero value.
     */
    public long numNonZero = 0;

    /**
     * sum of  ith dimension: sum(x_i).
     */
    public double sum = 0;

    /**
     * square sum of feature: sum(x_i * x_i).
     */
    public double squareSum = 0;

    /**
     * min of feature: min(x_i).
     */
    public double min = Double.MAX_VALUE;

    /**
     * max of feature: max(x_i).
     */
    public double max = -Double.MAX_VALUE;

    /**
     * norm l1.
     */
    public double normL1 = 0;

    /**
     * update by value.
     */
    public void visit(double value) {
        if (!Double.isNaN(value)) {
            double tmp = value;
            sum += tmp;

            tmp *= value;
            squareSum += tmp;

            if (value != 0) {
                numNonZero++;
            }

            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }

            normL1 += Math.abs(value);
        }
    }

    /**
     * merge.
     */
    public void merge(VectorStatCol vsc) {
        numNonZero += vsc.numNonZero;
        sum += vsc.sum;
        squareSum += vsc.squareSum;
        if (vsc.min < min) {
            min = vsc.min;
        }
        if (vsc.max > max) {
            max = vsc.max;
        }
        normL1 += vsc.normL1;
    }

    @Override
    public String toString() {
        return "VectorStatCol{" +
            "numNonZero=" + numNonZero +
            ", sum=" + sum +
            ", squareSum=" + squareSum +
            ", min=" + min +
            ", max=" + max +
            ", normL1=" + normL1 +
            '}';
    }

    //todo: change to copy
    @Override
    public VectorStatCol clone() {
        VectorStatCol vsc = new VectorStatCol();
        vsc.numNonZero = numNonZero;
        vsc.sum = sum;
        vsc.squareSum = squareSum;
        vsc.min = min;
        vsc.max = max;
        vsc.normL1 = normL1;
        return vsc;
    }

    /**
     * mean.
     */
    public double mean(double count) {
        if (count == 0) {
            return 0;
        } else {
            return sum / count;
        }
    }

    /**
     * variance.
     */
    public double variance(double count) {
        if (0 == count || 1 == count) {
            return 0;
        } else {
            return Math.max(0.0, (squareSum - mean(count) * sum) / (count - 1));
        }
    }

    /**
     * standardDeviation.
     */
    public double standardDeviation(double count) {
        return Math.sqrt(variance(count));
    }
}
