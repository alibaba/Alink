package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorIterator;

import java.util.HashMap;
import java.util.Map;

/**
 * It is summary of sparse vector, and uses Map to store intermediate result.
 * It will compute sum, squareSum = sum(x_i*x_i), min, max, normL1.
 * Other statistics value can be calculated from these statistics.
 *
 *<p>  Inheritance relationship as follow:
 *            BaseSummarizer
 *              /       \
 *             /         \
 *   TableSummarizer   BaseVectorSummarizer
 *                       /            \
 *                      /              \
 *        SparseVectorSummarizer    DenseVectorSummarizer
 *
 *<p>  SparseVectorSummarizer is for sparse vector, DenseVectorSummarizer is for dense vector.
 *  It can use toSummary() to get the result BaseVectorSummary.
 */
public class SparseVectorSummarizer extends BaseVectorSummarizer {

    /**
     * colNum.
     */
    protected int colNum = -1;

    /**
     * statistics result.
     */
    public Map<Integer, VectorStatCol> cols = new HashMap<>();

    /**
     * default constructor. it will not calculate outerProduct.
     */
    public SparseVectorSummarizer() {
        this.calculateOuterProduct = false;
    }

    /**
     * If calculateOuterProduction is true, it will calculate outerProduction, it will be used for covariance and
     */
    public SparseVectorSummarizer(boolean calculateOuterProduction) {
        this.calculateOuterProduct = calculateOuterProduction;
    }


    /**
     * update by vector.
     */
    @Override
    public BaseVectorSummarizer visit(Vector vec) {
        SparseVector sv;

        if (vec instanceof DenseVector) {
            DenseVector dv = (DenseVector) vec;
            int[] indices = new int[dv.size()];
            for (int i = 0; i < dv.size(); i++) {
                indices[i] = i;
            }

            sv = new SparseVector(dv.size(), indices, dv.getData());
        } else {
            sv = (SparseVector) vec;
        }

        count++;

        this.colNum = Math.max(this.colNum, sv.size());

        if (sv.numberOfValues() != 0) {

            //max index + 1 for size.
            VectorIterator iter = sv.iterator();
            while (iter.hasNext()) {
                int index = iter.getIndex();
                double value = iter.getValue();

                if (cols.containsKey(index)) {
                    cols.get(index).visit(value);
                } else {
                    VectorStatCol statCol = new VectorStatCol();
                    statCol.visit(value);
                    cols.put(index, statCol);
                }
                iter.next();
            }

            if (calculateOuterProduct) {
                int size = sv.getIndices()[sv.getIndices().length - 1] + 1;

                if (outerProduct == null) {
                    outerProduct = DenseMatrix.zeros(size, size);
                } else {
                    if (size > outerProduct.numRows()) {
                        DenseMatrix dpNew = DenseMatrix.zeros(size, size);
                        if (outerProduct != null) {
                            outerProduct = VectorSummarizerUtil.plusEqual(dpNew, outerProduct);
                        }
                    }
                }
                for (int i = 0; i < sv.getIndices().length; i++) {
                    double val = sv.getValues()[i];
                    int iIdx = sv.getIndices()[i];
                    for (int j = 0; j < sv.getIndices().length; j++) {
                        outerProduct.add(iIdx, sv.getIndices()[j], val * sv.getValues()[j]);
                    }
                }
            }
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sbd = new StringBuilder()
            .append("rowNum: ")
            .append(count);

        for (Map.Entry<Integer, VectorStatCol> entry : cols.entrySet()) {
            sbd.append("\n")
                .append(entry.getKey())
                .append("|")
                .append(entry.getValue().toString());
        }
        return sbd.toString();
    }

    /**
     * @return summary result.
     */
    @Override
    public BaseVectorSummary toSummary() {
        SparseVectorSummary summary = new SparseVectorSummary();

        summary.count = count;
        summary.cols = cols;
        summary.colNum = colNum;

        return summary;
    }


    public SparseVectorSummarizer copy() {
        SparseVectorSummarizer summarizer = new SparseVectorSummarizer();
        summarizer.count = count;
        summarizer.colNum = colNum;
        for (Map.Entry<Integer, VectorStatCol> entry : cols.entrySet()) {
            summarizer.cols.put(entry.getKey(), entry.getValue());
        }
        return summarizer;
    }

}
