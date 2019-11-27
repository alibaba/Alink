package com.alibaba.alink.common.linalg;

import org.apache.flink.util.Preconditions;

import com.github.fommil.netlib.F2jBLAS;

/**
 * A utility class that provides BLAS routines over matrices and vectors.
 */
public class BLAS {

    /**
     * For level-1 routines, we use Java implementation.
     */
    private static final com.github.fommil.netlib.BLAS F2J_BLAS = new F2jBLAS();

    /**
     * For level-2 and level-3 routines, we use the native BLAS.
     * The NATIVE_BLAS instance tries to load BLAS implementations in the order:
     * 1) optimized system libraries such as Intel MKL,
     * 2) self-contained native builds using the reference Fortran from netlib.org,
     * 3) F2J implementation.
     * If to use optimized system libraries, it is important to turn of their multi-thread support.
     * Otherwise, it will conflict with Flink's executor and leads to performance loss.
     */
    private static final com.github.fommil.netlib.BLAS NATIVE_BLAS = com.github.fommil.netlib.BLAS.getInstance();

    /**
     * \sum_i |x_i| .
     */
    public static double asum(int n, double[] x, int offset) {
        return F2J_BLAS.dasum(n, x, offset, 1);
    }

    /**
     * \sum_i |x_i| .
     */
    public static double asum(DenseVector x) {
        return asum(x.data.length, x.data, 0);
    }

    /**
     * \sum_i |x_i| .
     */
    public static double asum(SparseVector x) {
        return asum(x.values.length, x.values, 0);
    }

    /**
     * y += a * x .
     */
    public static void axpy(double a, double[] x, double[] y) {
        Preconditions.checkArgument(x.length == y.length, "Array dimension mismatched.");
        F2J_BLAS.daxpy(x.length, a, x, 1, y, 1);
    }

    /**
     * y += a * x .
     */
    public static void axpy(double a, DenseVector x, DenseVector y) {
        Preconditions.checkArgument(x.data.length == y.data.length, "Vector dimension mismatched.");
        F2J_BLAS.daxpy(x.data.length, a, x.data, 1, y.data, 1);
    }

    /**
     * y += a * x .
     */
    public static void axpy(double a, SparseVector x, DenseVector y) {
        for (int i = 0; i < x.indices.length; i++) {
            y.data[x.indices[i]] += a * x.values[i];
        }
    }

    /**
     * y += a * x .
     */
    public static void axpy(double a, Vector x, DenseVector y) {
        if(x instanceof SparseVector){
            axpy(a, (SparseVector)x, y);
        }else {
            axpy(a, (DenseVector)x, y);
        }
    }

    /**
     * y += a * x .
     */
    public static void axpy(double a, DenseMatrix x, DenseMatrix y) {
        Preconditions.checkArgument(x.m == y.m && x.n == y.n, "Matrix dimension mismatched.");
        F2J_BLAS.daxpy(x.data.length, a, x.data, 1, y.data, 1);
    }

    /**
     * y[yOffset:yOffset+n] += a * x[xOffset:xOffset+n] .
     */
    public static void axpy(int n, double a, double[] x, int xOffset, double[] y, int yOffset) {
        F2J_BLAS.daxpy(n, a, x, xOffset, 1, y, yOffset, 1);
    }

    /**
     * x \cdot y .
     */
    public static double dot(double[] x, double[] y) {
        Preconditions.checkArgument(x.length == y.length, "Array dimension mismatched.");
        double s = 0.;
        for (int i = 0; i < x.length; i++) {
            s += x[i] * y[i];
        }
        return s;
    }

    /**
     * x \cdot y .
     */
    public static double dot(DenseVector x, DenseVector y) {
        return dot(x.getData(), y.getData());
    }

    /**
     * x = x * a .
     */
    public static void scal(double a, double[] x) {
        F2J_BLAS.dscal(x.length, a, x, 1);
    }

    /**
     * x = x * a .
     */
    public static void scal(double a, double[] x, int xOffset, int length) {
        F2J_BLAS.dscal(length, a, x, xOffset, 1);
    }

    /**
     * x = x * a .
     */
    public static void scal(double a, DenseVector x) {
        F2J_BLAS.dscal(x.data.length, a, x.data, 1);
    }

    /**
     * x = x * a .
     */
    public static void scal(double a, SparseVector x) {
        F2J_BLAS.dscal(x.values.length, a, x.values, 1);
    }

    /**
     * x = x * a .
     */
    public static void scal(double a, DenseMatrix x) {
        F2J_BLAS.dscal(x.data.length, a, x.data, 1);
    }

    /**
     * C := alpha * A * B + beta * C .
     */
    public static void gemm(double alpha, DenseMatrix matA, boolean transA, DenseMatrix matB, boolean transB,
                            double beta, DenseMatrix matC) {
        int ma = transA ? matA.n : matA.m;
        int na = transA ? matA.m : matA.n;
        int mb = transB ? matB.n : matB.m;
        int nb = transB ? matB.m : matB.n;
        Preconditions.checkArgument(na == mb && ma == matC.m && nb == matC.n, "matrix size mismatched.");

        final int m = matC.numRows();
        final int n = matC.numCols();
        final int k = transA ? matA.numRows() : matA.numCols();
        final int lda = matA.numRows();
        final int ldb = matB.numRows();
        final int ldc = matC.numRows();
        final String ta = transA ? "T" : "N";
        final String tb = transB ? "T" : "N";
        NATIVE_BLAS.dgemm(ta, tb, m, n, k, alpha, matA.getData(), lda, matB.getData(), ldb, beta, matC.getData(), ldc);
    }

    /**
     * Check the compatibility of matrix and vector sizes in <code>gemv</code>.
     */
    private static void gemvDimensionCheck(DenseMatrix matA, boolean transA, Vector x, Vector y) {
        if (transA) {
            Preconditions.checkArgument(matA.numCols() == y.size() && matA.numRows() == x.size(),
                "Matrix and vector size mismatched.");
        } else {
            Preconditions.checkArgument(matA.numRows() == y.size() && matA.numCols() == x.size(),
                "Matrix and vector size mismatched.");
        }
    }

    /**
     * y := alpha * A * x + beta * y .
     */
    public static void gemv(double alpha, DenseMatrix matA, boolean transA,
                            Vector x, double beta, DenseVector y) {
        if(x instanceof SparseVector){
            gemv(alpha, matA, transA, (SparseVector)x, beta, y);
        }else{
            gemv(alpha, matA, transA, (DenseVector)x, beta, y);
        }
    }

    /**
     * y := alpha * A * x + beta * y .
     */
    public static void gemv(double alpha, DenseMatrix matA, boolean transA,
                            DenseVector x, double beta, DenseVector y) {
        gemvDimensionCheck(matA, transA, x, y);
        final int m = matA.numRows();
        final int n = matA.numCols();
        final int lda = matA.numRows();
        final String ta = transA ? "T" : "N";
        NATIVE_BLAS.dgemv(ta, m, n, alpha, matA.getData(), lda, x.getData(), 1, beta, y.getData(), 1);
    }

    /**
     * y := alpha * A * x + beta * y .
     */
    public static void gemv(double alpha, DenseMatrix matA, boolean transA,
                            SparseVector x, double beta, DenseVector y) {
        gemvDimensionCheck(matA, transA, x, y);
        final int m = matA.numRows();
        final int n = matA.numCols();
        if (transA) {
            int start = 0;
            for (int i = 0; i < n; i++) {
                double s = 0.;
                for (int j = 0; j < x.indices.length; j++) {
                    s += x.values[j] * matA.data[start + x.indices[j]];
                }
                y.data[i] = beta * y.data[i] + alpha * s;
                start += m;
            }
        } else {
            scal(beta, y);
            for (int i = 0; i < x.indices.length; i++) {
                int index = x.indices[i];
                double value = alpha * x.values[i];
                F2J_BLAS.daxpy(m, value, matA.data, index * m, 1, y.data, 0, 1);
            }
        }
    }
}