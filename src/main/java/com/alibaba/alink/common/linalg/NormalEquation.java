package com.alibaba.alink.common.linalg;

import java.util.Arrays;

/**
 * A normal equation is A^T * A * x = A^T * b, where A * x = b is a lease square problem.
 */
public class NormalEquation {
    private static final com.github.fommil.netlib.BLAS NATIVE_BLAS = com.github.fommil.netlib.BLAS.getInstance();

    /**
     * Rank of the equation.
     */
    private int n;

    /**
     * A^T * A.
     */
    private DenseMatrix ata;

    /**
     * A^T * b.
     */
    private DenseVector atb;

    /**
     * The constructor.
     *
     * @param n Rank of the equation.
     */
    public NormalEquation(int n) {
        this.n = n;
        this.ata = new DenseMatrix(n, n);
        this.atb = new DenseVector(n);
    }

    /**
     * Add coefficients to the normal equation.
     *
     * @param a A row of matrix "A".
     * @param b An element of right hand side "b".
     * @param c The scale factor of "a".
     */
    public void add(DenseVector a, double b, double c) {
        // ata += c * a.t * a
        NATIVE_BLAS.dger(n, n, c, a.getData(), 1, a.getData(), 1, this.ata.getData(), n);

        // atb += b * a.t
        BLAS.axpy(b, a, this.atb);
    }

    /**
     * Reset the system to zeros.
     */
    public void reset() {
        Arrays.fill(ata.getData(), 0.);
        Arrays.fill(atb.getData(), 0.);
    }

    /**
     * Merge with another A^T*A
     */
    public void merge(DenseMatrix otherAta) {
        BLAS.axpy(1.0, otherAta, this.ata);
    }

    /**
     * Regularize the system by adding "lambda" to diagonals.
     */
    public void regularize(double lambda) {
        for (int i = 0; i < n; i++) {
            this.ata.add(i, i, lambda);
        }
    }

    /**
     * Solve the system. After solving the system, the result is returned in <code>x</code>,
     * and the data in <code>ata</code> and <code>atb</code> will be reset to zeros.
     *
     * @param x           For holding the result.
     * @param nonNegative Whether to enforce non-negative constraint.
     */
    public void solve(DenseVector x, boolean nonNegative) {
        if (nonNegative) {
            DenseVector ret = NNLSSolver.solve(ata, atb);
            System.arraycopy(ret.getData(), 0, x.getData(), 0, n);
        } else {
            LinearSolver.symmetricPositiveDefiniteSolve(ata, new DenseMatrix(n, 1, atb.getData()));
            System.arraycopy(atb.getData(), 0, x.getData(), 0, n);
        }
        this.reset();
    }
}
