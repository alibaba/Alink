package com.alibaba.alink.common.linalg;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for DenseMatrix.
 */
public class DenseMatrixTest {

    private static final double TOL = 1.0e-6;

    private static void assertEqual2D(double[][] matA, double[][] matB) {
        assert (matA.length == matB.length);
        assert (matA[0].length == matB[0].length);
        int m = matA.length;
        int n = matA[0].length;
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                Assert.assertEquals(matA[i][j], matB[i][j], TOL);
            }
        }
    }

    private static double[][] simpleMM(double[][] matA, double[][] matB) {
        int m = matA.length;
        int n = matB[0].length;
        int k = matA[0].length;
        double[][] matC = new double[m][n];
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                matC[i][j] = 0.;
                for (int l = 0; l < k; l++) {
                    matC[i][j] += matA[i][l] * matB[l][j];
                }
            }
        }
        return matC;
    }

    private static double[] simpleMV(double[][] matA, double[] x) {
        int m = matA.length;
        int n = matA[0].length;
        assert (n == x.length);
        double[] y = new double[m];
        for (int i = 0; i < m; i++) {
            y[i] = 0.;
            for (int j = 0; j < n; j++) {
                y[i] += matA[i][j] * x[j];
            }
        }
        return y;
    }

    @Test
    public void testPlusEquals() throws Exception {
        DenseMatrix matA = new DenseMatrix(new double[][]{
            new double[]{1, 3, 5},
            new double[]{2, 4, 6},
        });
        DenseMatrix matB = DenseMatrix.ones(2, 3);
        matA.plusEquals(matB);
        Assert.assertArrayEquals(matA.getData(), new double[]{2, 3, 4, 5, 6, 7}, TOL);
        matA.plusEquals(1.0);
        Assert.assertArrayEquals(matA.getData(), new double[]{3, 4, 5, 6, 7, 8}, TOL);
    }

    @Test
    public void testMinusEquals() throws Exception {
        DenseMatrix matA = new DenseMatrix(new double[][]{
            new double[]{1, 3, 5},
            new double[]{2, 4, 6},
        });
        DenseMatrix matB = DenseMatrix.ones(2, 3);
        matA.minusEquals(matB);
        Assert.assertArrayEquals(matA.getData(), new double[]{0, 1, 2, 3, 4, 5}, TOL);
    }

    @Test
    public void testPlus() throws Exception {
        DenseMatrix matA = new DenseMatrix(new double[][]{
            new double[]{1, 3, 5},
            new double[]{2, 4, 6},
        });
        DenseMatrix matB = DenseMatrix.ones(2, 3);
        DenseMatrix matC = matA.plus(matB);
        Assert.assertArrayEquals(matC.getData(), new double[]{2, 3, 4, 5, 6, 7}, TOL);
        DenseMatrix matD = matA.plus(1.0);
        Assert.assertArrayEquals(matD.getData(), new double[]{2, 3, 4, 5, 6, 7}, TOL);
    }

    @Test
    public void testMinus() throws Exception {
        DenseMatrix matA = new DenseMatrix(new double[][]{
            new double[]{1, 3, 5},
            new double[]{2, 4, 6},
        });
        DenseMatrix matB = DenseMatrix.ones(2, 3);
        DenseMatrix matC = matA.minus(matB);
        Assert.assertArrayEquals(matC.getData(), new double[]{0, 1, 2, 3, 4, 5}, TOL);
    }

    @Test
    public void testMM() throws Exception {
        DenseMatrix matA = DenseMatrix.rand(4, 3);
        DenseMatrix matB = DenseMatrix.rand(3, 5);
        DenseMatrix matC = matA.multiplies(matB);
        assertEqual2D(matC.getArrayCopy2D(), simpleMM(matA.getArrayCopy2D(), matB.getArrayCopy2D()));

        DenseMatrix matD = new DenseMatrix(5, 4);
        BLAS.gemm(1., matB, true, matA, true, 0., matD);
        Assert.assertArrayEquals(matD.transpose().getData(), matC.data, TOL);
    }

    @Test
    public void testMV() throws Exception {
        DenseMatrix matA = DenseMatrix.rand(4, 3);
        DenseVector x = DenseVector.ones(3);
        DenseVector y = matA.multiplies(x);
        Assert.assertArrayEquals(y.getData(), simpleMV(matA.getArrayCopy2D(), x.getData()), TOL);

        SparseVector x2 = new SparseVector(3, new int[]{0, 1, 2}, new double[]{1, 1, 1});
        DenseVector y2 = matA.multiplies(x2);
        Assert.assertArrayEquals(y2.getData(), y.getData(), TOL);
    }

    @Test
    public void testDataSelection() throws Exception {
        DenseMatrix mat = new DenseMatrix(new double[][]{
            new double[]{1, 2, 3},
            new double[]{4, 5, 6},
            new double[]{7, 8, 9},
        });
        DenseMatrix sub1 = mat.selectRows(new int[]{1});
        DenseMatrix sub2 = mat.getSubMatrix(1, 2, 1, 2);
        Assert.assertEquals(sub1.numRows(), 1);
        Assert.assertEquals(sub1.numCols(), 3);
        Assert.assertEquals(sub2.numRows(), 1);
        Assert.assertEquals(sub2.numCols(), 1);
        Assert.assertArrayEquals(sub1.getData(), new double[]{4, 5, 6}, TOL);
        Assert.assertArrayEquals(sub2.getData(), new double[]{5}, TOL);

        double[] row = mat.getRow(1);
        double[] col = mat.getColumn(1);
        Assert.assertArrayEquals(row, new double[]{4, 5, 6}, 0.);
        Assert.assertArrayEquals(col, new double[]{2, 5, 8}, 0.);
    }

    @Test
    public void testSum() throws Exception {
        DenseMatrix matA = DenseMatrix.ones(3, 2);
        Assert.assertEquals(matA.sum(), 6.0, TOL);
    }

    @Test
    public void testRowMajorFormat() throws Exception {
        double[] data = new double[]{1, 2, 3, 4, 5, 6};
        DenseMatrix matA = new DenseMatrix(2, 3, data, true);
        Assert.assertArrayEquals(data, new double[]{1, 4, 2, 5, 3, 6}, 0.);
        Assert.assertArrayEquals(matA.getData(), new double[]{1, 4, 2, 5, 3, 6}, 0.);

        data = new double[]{1, 2, 3, 4};
        matA = new DenseMatrix(2, 2, data, true);
        Assert.assertArrayEquals(data, new double[]{1, 3, 2, 4}, 0.);
        Assert.assertArrayEquals(matA.getData(), new double[]{1, 3, 2, 4}, 0.);
    }


    // ------------------- below are not intended to open source at the moment ---------

    @Test
    public void testNorm2() throws Exception {
        DenseMatrix matA = DenseMatrix.zeros(4, 5);
        matA.set(0, 0, 1);
        matA.set(0, 4, 2);
        matA.set(1, 2, 3);
        matA.set(3, 1, 2);
        Assert.assertEquals(matA.norm2(), 3., TOL);
    }

    @Test
    public void testCond() throws Exception {
        DenseMatrix matA = DenseMatrix.zeros(4, 5);
        matA.set(0, 0, 1);
        matA.set(0, 4, 2);
        matA.set(1, 2, 3);
        matA.set(3, 1, 2);
        double[] answer = new double[]{3.0, 2.23606797749979, 2.0, 0.0};
        Assert.assertArrayEquals(answer, new SingularValueDecomposition(matA).getSingularValues().getData(), TOL);
    }

    @Test
    public void testDet() throws Exception {
        double[][] data1 = {
            {-2, 2, -3},
            {-1, 1, 3},
            {2, 0, -1},
        };
        DenseMatrix matA = new DenseMatrix(data1);
        Assert.assertEquals(matA.det(), 18.0, TOL);
    }

    @Test
    public void testRank() throws Exception {
        double[][] data1 = {
            {-2, 2, -3},
            {-1, 1, 3},
            {2, 0, -1},
        };
        DenseMatrix matA = new DenseMatrix(data1);
        Assert.assertEquals(matA.rank(), 3);

        double[][] data2 = {
            {-2, 2, -3},
            {-1, 1, -1.5},
            {2, 0, -1},
            {2, 0, -1},
        };
        DenseMatrix matB = new DenseMatrix(data2);
        Assert.assertEquals(matB.rank(), 2);
    }

    @Test
    public void testSolve() throws Exception {
        {
            // symmetric case
            DenseMatrix matA = DenseMatrix.randSymmetric(5);
            DenseMatrix b = DenseMatrix.rand(5, 7);
            DenseMatrix x = matA.solve(b);
            DenseMatrix b0 = matA.multiplies(x);
            assertEqual2D(b.getArrayCopy2D(), b0.getArrayCopy2D());
        }

        {
            // non-symmetric case
            DenseMatrix matA = DenseMatrix.rand(5, 5);
            DenseMatrix b = DenseMatrix.rand(5, 7);
            DenseMatrix x = matA.solve(b);
            DenseMatrix b0 = matA.multiplies(x);
            assertEqual2D(b.getArrayCopy2D(), b0.getArrayCopy2D());
        }

        {
            // under determined case
            DenseMatrix matA = DenseMatrix.rand(3, 5);
            DenseMatrix b = DenseMatrix.rand(3, 7);
            DenseMatrix x = matA.solve(b);
            DenseMatrix b0 = matA.multiplies(x);
            assertEqual2D(b.getArrayCopy2D(), b0.getArrayCopy2D());
        }

        {
            // over determined case
            DenseMatrix matA = DenseMatrix.rand(5, 3);
            DenseVector b = DenseVector.rand(5);
            DenseVector x = matA.solve(b);
            DenseVector r = matA.multiplies(x).minus(b);
            double nr = r.normL2Square();

            for (int i = 0; i < 10; i++) {
                DenseVector x0 = DenseVector.rand(3);
                double nr0 = matA.multiplies(x0).minus(b).normL2();
                Assert.assertTrue(nr <= nr0);
            }
        }
    }

    @Test
    public void testSolveLS() throws Exception {
        double[][] data = {
            {-2, 2, -3},
            {-1, 1, -1.5},
            {2, 0, -1},
            {2, 0, -1},
        };
        DenseMatrix matA = new DenseMatrix(data);
        DenseMatrix matB = DenseMatrix.rand(4, 1);
        DenseMatrix matX = matA.solveLS(matB);

        DenseMatrix r = matA.multiplies(matX).minus(matB);

        DenseVector b = DenseVector.rand(4);
        DenseVector x = matA.solveLS(b);
    }

    @Test
    public void testSolveEigen() throws Exception {
        DenseMatrix matA = DenseMatrix.randSymmetric(5);
        scala.Tuple2<DenseVector, DenseMatrix> result = EigenSolver.solve(matA, 3, 0.01, 300);
        DenseVector evs = result._1;
        DenseMatrix evec = result._2;
    }

    @Test
    public void testInverse() throws Exception {
        DenseMatrix matA = DenseMatrix.rand(5, 5);
        DenseMatrix matIA = matA.inverse();
        DenseMatrix matAIA = matA.multiplies(matIA);
        DenseMatrix matI = DenseMatrix.eye(5);
        assertEqual2D(matAIA.getArrayCopy2D(), matI.getArrayCopy2D());
    }

    @Test
    public void testNnls() throws Exception {
        DenseMatrix matA = DenseMatrix.rand(8, 4);
        DenseVector b = DenseVector.ones(8);
        DenseMatrix matATA = matA.transpose().multiplies(matA);
        DenseVector vecATb = matA.transpose().multiplies(b);
        DenseVector x = NNLSSolver.solve(matATA, vecATb);

        DenseMatrix matX = new DenseMatrix(8, 1, b.getData().clone());
        LeastSquareSolver.solve(matA.clone(), matX);
        double[] xdata = new double[4];
        System.arraycopy(matX.getData(), 0, xdata, 0, 4);
        DenseVector xLS = new DenseVector(xdata);

        System.out.println(matA.multiplies(x).minus(b).normL2());
        System.out.println(matA.multiplies(xLS).minus(b).normL2());
        System.out.println(matA.multiplies(DenseVector.ones(4).scale(0.5)).minus(b).normL2());
        System.out.println(x.toString());
    }

}