package com.alibaba.alink.operator.common.dataproc;

/**
 * scaler util.
 */
public class ScalerUtil {
    /**
     * change val into [minV, maxV).
     *
     * @param val  value to be scaled.
     * @param eMin min value of vector in this column.
     * @param eMax max value of vector in this column.
     * @param maxV max value user defined.
     * @param minV min value user defined.
     * @return scaled value.
     */
    public static double minMaxScaler(double val, double eMin, double eMax, double maxV, double minV) {
        double valOut;
        if (eMin != eMax) {
            valOut = (val - eMin) / (eMax - eMin) * (maxV - minV) + minV;
        } else {
            valOut = 0.5 * (maxV + minV);
        }
        return valOut;
    }

    /**
     * change val divide maxAbs.
     *
     * @param maxAbs max value of vectors in this column.
     * @param val    value to be scaled.
     * @return scaled value.
     */
    public static double maxAbsScaler(double maxAbs, double val) {
        return maxAbs == 0 ? val : val / maxAbs;
    }
}
