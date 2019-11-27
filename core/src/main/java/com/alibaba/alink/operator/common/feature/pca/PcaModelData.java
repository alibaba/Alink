package com.alibaba.alink.operator.common.feature.pca;

public class PcaModelData {

    /**
     * col names
     */
    public String[] featureColNames;

    /**
     * vector column name
     */
    public String vectorColName;

    /**
     * pca type
     */
    public String pcaType;

    /**
     * name of calculate cols
     */
    public String[] nameX = null;

    /**
     * mean of cols
     */
    public double[] means = null;

    /**
     * standard deviation of cols
     */
    public double[] stddevs = null;

    /**
     * number of principal component
     */
    public int p;

    /**
     * eigenvalues
     */
    public double[] lambda;

    /**
     * eigenvector
     */
    public double[][] coef = null;

    /**
     * covariance or correlation coefficient
     **/
    public double[][] cov = null;

    /**
     * col is the same value
     */
    public Integer[] idxNonEqual = null;

    /**
     * num of colnames
     */
    public int nx;


    /**
     * *
     * calcultion principal component
     *
     * @param vec data
     * @return principal component
     */
    public double[] calcPrinValue(double[] vec) {
        int nx = vec.length;
        double[] v = new double[nx];
        double[] r = new double[p];
        for (int i = 0; i < nx; i++) {
            v[i] = vec[i];
        }
        for (int k = 0; k < p; k++) {
            r[k] = 0;
            for (int i = 0; i < nx; i++) {
                r[k] += v[i] * coef[k][i];
            }
        }
        return r;
    }

    @Override
    public String toString() {
        java.io.CharArrayWriter cw = new java.io.CharArrayWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(cw, true);
        int nx = nameX.length;
        pw.println("Eigenvalues of the CorrelationBak : ");
        pw.println("      \tEigenvalue            \tProportion           \tCumulative");
        double sum = 0;
        for (int i = 0; i < p; i++) {
            double cur = lambda[i] / nx;
            sum += cur;
            pw.println("Prin" + (i + 1) + " \t" + lambda[i] + " \t" + cur + " \t" + sum);
        }
        pw.println("Principle Components : ");
        for (int i = 0; i < p; i++) {
            pw.print("Prin" + (i + 1) + " = " + coef[i][0] + " * " + nameX[0]);
            for (int j = 1; j < nx; j++) {
                pw.print(" + " + coef[i][j] + " * " + nameX[j]);
            }
            pw.println();
        }
        return cw.toString();
    }

}
