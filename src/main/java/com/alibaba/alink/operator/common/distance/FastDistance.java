package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.*;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.TableUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.google.common.collect.Iterables;
import org.apache.flink.util.Preconditions;

import java.util.*;

/**
 * FastDistance is used to accelerate the speed of calculating the distance. The two main points are as below:
 * <p>
 * 1. By pre-calculating some extra info of the vector, such as L1 norm or L2 norm.
 * <p>
 * 2. By organizing several vectors in a single matrix to increate the cache hit rate.
 * <p>
 * The first point applies for both dense and sparse vectors. The second points is only useful for dense vector as the
 * indices length of sparse vector is not the same and only works when we need to access the vectors in batch.
 */
public abstract class FastDistance implements ContinuousDistance{
    /**
     * Maximum size of a matrix.
     */
    private static int SIZE = 5 * 1024 * 1024;

    private static int MAX_ROW_NUMBER = (int)Math.sqrt(200 * 1024 * 1024 / 8.0);

    /**
     * Prepare the FastDistanceData, the output could be a list of FastDistanceVectorData if the vector is sparse. If
     * the vectors are dense, the output is a list of FastDistanceMatrixData. As the size of the each element in a
     * dataset is limited, we can not union all the dense vectors in a single matrix. We must seperate the vectors and
     * store them in several matrices.
     *
     * @param rows      input rows.
     * @param vectorIdx the index of the vector columns.
     * @param keepIdxs  the indexes of columns who are kept.
     * @return a list of <code>FastDistanceData</code>
     */
    public List<FastDistanceData> prepareMatrixData(Iterable<Row> rows, int vectorIdx, int... keepIdxs) {
        Iterable<Tuple2<Vector, Row>> newItearble = Iterables.transform(rows, (row) -> {
            Vector vec = VectorUtil.getVector(row.getField(vectorIdx));
            row = TableUtil.getRow(row, keepIdxs);
            return Tuple2.of(vec, row);
        });
        return prepareMatrixData(newItearble);
    }

    /**
     * Prepare the FastDistanceVectorData which only save one vector(sparse or dense).
     *
     * @param row       input row.
     * @param vectorIdx the index of the vector column.
     * @param keepIdxs  the indexes of columns who are kept.
     * @return <code>FastDistanceVectorData</code>
     */
    public FastDistanceVectorData prepareVectorData(Row row, int vectorIdx, int... keepIdxs) {
        return prepareVectorData(Tuple2.of(VectorUtil.getVector(row.getField(vectorIdx)),
            TableUtil.getRow(row, keepIdxs)));
    }

    /**
     * Prepare the FastDistanceData. If the vector is dense, we organize <code>rowNumber = SIZE / 8 / vectorSize</code>
     * vectors into a matrix. If the number of remaining vectors is n ,which is less than rowNumber, then the last
     * matrix only contains n columns.
     * <p>
     * If the vector is sparse, we deal with the inputs row by row and returns a list of FastDistanceVectorData.
     *
     * @param tuples support vector and row input.
     * @return a list of FastDistanceData.
     */
    public List<FastDistanceData> prepareMatrixData(Iterable<Tuple2<Vector, Row>> tuples) {
        Iterator<Tuple2<Vector, Row>> iterator = tuples.iterator();
        Tuple2<Vector, Row> row = null;
        int vectorSize = 0;
        boolean isDense = false;
        if (iterator.hasNext()) {
            row = iterator.next();
            if (row.f0 instanceof DenseVector) {
                isDense = true;
                vectorSize = row.f0.size();
            }
        }
        if (isDense) {
            return prepareDenseMatrixData(row, iterator, vectorSize);
        } else {
            return prepareSparseMatrixData(row, iterator);
        }
    }

    private List<FastDistanceData> prepareSparseMatrixData(Tuple2<Vector, Row> tuple,
                                                           Iterator<Tuple2<Vector, Row>> iterator) {
        List<FastDistanceData> res = new ArrayList<>();
        while (null != tuple) {
            Preconditions.checkState(tuple.f0 instanceof SparseVector, "Inputs should be the same vector type!");
            res.add(prepareVectorData(tuple));
            tuple = iterator.hasNext() ? iterator.next() : null;
        }
        return res;
    }

    private List<FastDistanceData> prepareDenseMatrixData(Tuple2<Vector, Row> tuple,
                                                          Iterator<Tuple2<Vector, Row>> iterator,
                                                          int vectorSize) {
        final int rowNumber = Math.min(SIZE / 8 / vectorSize, MAX_ROW_NUMBER);
        List<FastDistanceData> res = new ArrayList<>();
        while (null != tuple) {
            int index = 0;
            DenseMatrix matrix = new DenseMatrix(vectorSize, rowNumber);
            Row[] rows = new Row[rowNumber];
            while (index < rowNumber && null != tuple) {
                Preconditions.checkState(tuple.f0 instanceof DenseVector, "Inputs should be the same vector type!");
                rows[index] = tuple.f1;
                MatVecOp.appendVectorToMatrix(matrix, false, index++, tuple.f0);
                tuple = iterator.hasNext() ? iterator.next() : null;
            }
            FastDistanceData data = index == rowNumber ? new FastDistanceMatrixData(matrix, rows) :
                new FastDistanceMatrixData(
                    new DenseMatrix(vectorSize, index, Arrays.copyOf(matrix.getData(), index * vectorSize)),
                    Arrays.copyOf(rows, index));
            updateLabel(data);
            res.add(data);
        }
        return res;
    }

    /**
     * Prepare the FastDistanceVectorData.
     *
     * @param tuple support vector and row input.
     * @return FastDistanceVectorData.
     */
    public FastDistanceVectorData prepareVectorData(Tuple2<Vector, Row> tuple) {
        FastDistanceVectorData data = new FastDistanceVectorData(tuple.f0, tuple.f1);
        updateLabel(data);
        return data;
    }

    /**
     * Calculate the distances between vectors in <code>left</code> and <code>right</code>.
     *
     * @param left FastDistanceData.
     * @param right FastDistanceData.
     * @return a new DenseMatrix.
     */
    public DenseMatrix calc(FastDistanceData left, FastDistanceData right) {
        return calc(left, right, null);
    }

    /**
     * Calculate the distances between vectors in <code>left</code> and <code>right</code>. The operation is a Cartesian
     * Product of left and right. The inputs fall into four types: 1. left is FastDistanceVectorData, right is
     * FastDistanceVectorData, the dimension of the result matrix is 1 X 1.
     * <p>
     * 2. left is FastDistanceVectorData, right is FastDistanceMatrixData which saves m vectors. The dimension of the
     * result matrix is m X 1.
     * <p>
     * 3. left is FastDistanceMatrixData which saves n vectors, right is FastDistanceVectorData. The dimension of the
     * result matrix is 1 X n.
     * <p>
     * 4. left is FastDistanceMatrixData which saves n vectors, right is FastDistanceMatrixData which saves m vectors.
     * the dimension of the result matrix is m X n.
     *
     * @param left FastDistanceData.
     * @param right FastDistanceData.
     * @param res   if res is null or the dimension of res is not satisfied, a new DenseMatrix is created, otherwise,
     *              the given res is refilled.
     * @return the distances.
     */
    public DenseMatrix calc(FastDistanceData left, FastDistanceData right, DenseMatrix res) {
        if (left instanceof FastDistanceVectorData) {
            if (right instanceof FastDistanceVectorData) {
                FastDistanceVectorData leftData = (FastDistanceVectorData)left;
                FastDistanceVectorData rightData = (FastDistanceVectorData)right;
                double d = calc(leftData, rightData);
                if (null == res || res.numCols() != 1 || res.numRows() != 1) {
                    res = new DenseMatrix(1, 1, new double[] {d});
                }
                res.set(0, 0, d);
            } else {
                FastDistanceMatrixData matrixData = (FastDistanceMatrixData)right;
                if (null == res || res.numRows() != matrixData.vectors.numCols() || res.numCols() != 1) {
                    res = new DenseMatrix(matrixData.vectors.numCols(), 1);
                }
                calc((FastDistanceVectorData)left, matrixData, res.getData());
            }
        } else {
            if (right instanceof FastDistanceVectorData) {
                FastDistanceMatrixData matrixData = (FastDistanceMatrixData)left;
                if (null == res || res.numRows() != 1 || res.numCols() != matrixData.vectors.numCols()) {
                    res = new DenseMatrix(1, matrixData.vectors.numCols());
                }
                calc((FastDistanceVectorData)right, matrixData, res.getData());
            } else {
                FastDistanceMatrixData leftData = (FastDistanceMatrixData)left;
                FastDistanceMatrixData rightData = (FastDistanceMatrixData)right;

                if (null == res || res.numRows() != rightData.vectors.numCols() || res.numCols() != leftData.vectors
                    .numCols()) {
                    res = new DenseMatrix(rightData.vectors.numCols(), leftData.vectors.numCols());
                }
                calc(leftData, rightData, res);
            }
        }
        return res;
    }

    /**
     * When a instance of FastDistanceData is created or the data inside is updated, we must update the label as well.
     *
     * @param data FastDistanceData.
     */
    public abstract void updateLabel(FastDistanceData data);

    /**
     * Calculate the distance between the two vectors in left and right.
     *
     * @param left FastDistanceVectorData.
     * @param right FastDistanceVectorData.
     * @return the distance.
     */
    abstract double calc(FastDistanceVectorData left, FastDistanceVectorData right);

    /**
     * Calculate the distances between a vector in <code>vector</code> and several vectors in <code>matrix</code>. The
     * result is a double array.
     *
     * @param vector FastDistanceVectorData.
     * @param matrix FastDistanceMatrixData.
     * @param res distances.
     */
    abstract void calc(FastDistanceVectorData vector, FastDistanceMatrixData matrix, double[] res);

    /**
     * Calcualate the distances between m vectors in <code>left</code> and n vectors in <code>right</code>. The result
     * is a n X m dimension matrix.
     *
     * @param left FastDistanceMatrixData.
     * @param right FastDistanceMatrixData.
     * @param res distances.
     */
    abstract void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res);

}
