package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;

/**
 * LongMatrix stores a 2 dimension long array, and provides some methods to operate on the array it represents.
 */
public class LongMatrix implements Serializable {
    /**
     * data saved in row-major.
     */
    private long[][] matrix;

    /**
     * Row number.
     */
    private int rowNum;

    /**
     * Column number.
     */
    private int colNum;

    public LongMatrix(long[][] matrix) {
        Preconditions.checkNotNull(matrix, "The matrix is null!");
        Preconditions.checkState(matrix.length > 0 && matrix[0].length > 0, "The matrix is empty!");
        this.matrix = matrix;
        this.rowNum = matrix.length;
        this.colNum = matrix[0].length;
        for (int i = 1; i < rowNum; i++) {
            Preconditions.checkState(matrix[i].length == colNum, "The column numbers are not equal!");
        }
    }

    public long[][] getMatrix() {
        return matrix;
    }

    public int getRowNum() {
        return rowNum;
    }

    public int getColNum() {
        return colNum;
    }

    public void plusEqual(LongMatrix other) {
        Preconditions.checkArgument(other.rowNum == rowNum && other.colNum == colNum,
            "Matrix size not the same! The rowNums are {},{}; the colNums are {},{}.", rowNum, other.rowNum, colNum,
            other.colNum);
        for (int i = 0; i < rowNum; i++) {
            for (int j = 0; j < colNum; j++) {
                matrix[i][j] += other.matrix[i][j];
            }
        }
    }

    public long[] getRowSums() {
        long[] rowSums = new long[rowNum];
        for (int i = 0; i < rowNum; i++) {
            for (int j = 0; j < colNum; j++) {
                rowSums[i] += matrix[i][j];
            }
        }
        return rowSums;
    }

    public long[] getColSums() {
        long[] colSums = new long[colNum];
        for (int i = 0; i < rowNum; i++) {
            for (int j = 0; j < colNum; j++) {
                colSums[j] += matrix[i][j];
            }
        }
        return colSums;
    }

    public long getTotal() {
        long sum = 0L;
        for (int i = 0; i < rowNum; i++) {
            sum += Arrays.stream(matrix[i]).sum();
        }
        return sum;
    }

    public long getValue(int i, int j) {
        Preconditions.checkArgument(i >= 0 && i < rowNum && j >= 0 && j < colNum, "Index out of bound!");
        return matrix[i][j];
    }

    public void setValue(int i, int j, long value) {
        Preconditions.checkArgument(i >= 0 && i < rowNum && j >= 0 && j < colNum, "Index out of bound!");
        matrix[i][j] = value;
    }
}
