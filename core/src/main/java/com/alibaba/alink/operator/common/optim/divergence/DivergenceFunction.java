package com.alibaba.alink.operator.common.optim.divergence;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;

public class DivergenceFunction {
    //这边做法和alink不一样。这边是过一轮数据，计算dir中的一些参数，然后reduce，然后更新grad。
    public static double[] calcLossDivergence(Iterable<Tuple3<Double, Double, Vector>> labelVectors,
                                              DenseVector[] coefVectors, int retryNum) {
        double[] scoreLeft = new double[retryNum];
        double[] scoreRight = new double[retryNum];
        double[] scoreSquareLeft = new double[retryNum];
        double[] scoreSquareRight = new double[retryNum];
        double[] nLeft = new double[retryNum];
        double[] nRight = new double[retryNum];
        double score;

        for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
            for (int i = 0; i < retryNum; i++) {
                score = labelVector.f2.dot(coefVectors[i]);
                double neg = (1 - labelVector.f1) / 2 * labelVector.f0;
                double pos = (1 + labelVector.f1) / 2 * labelVector.f0;
                double nScore = neg * score;
                double pScore = pos * score;
                scoreLeft[i] += nScore;
                scoreRight[i] += pScore;
                scoreSquareLeft[i] += nScore * score;
                scoreSquareRight[i] += pScore * score;
                nLeft[i] += neg;
                nRight[i] += pos;
            }
        }

        //updateMeanAndVariance
        //这边写在一起，为了line search retry.
        double[] res = new double[retryNum * 6];
        System.arraycopy(scoreLeft, 0, res, 0, retryNum);
        System.arraycopy(scoreRight, 0, res, retryNum, retryNum);
        System.arraycopy(scoreSquareLeft, 0, res, 2 * retryNum, retryNum);
        System.arraycopy(scoreSquareRight, 0, res, 3 * retryNum, retryNum);
        System.arraycopy(nLeft, 0, res, 4 * retryNum, retryNum);
        System.arraycopy(nRight, 0, res, 5 * retryNum, retryNum);
        return res;
    }

    public static double[] calcLossDivergenceAndGrad(Iterable<Tuple3<Double, Double, Vector>> labelVectors,
                                                     DenseVector coefVector) {
        int hessianDim = coefVector.size();//temp，看下面的似乎hessianDim就是feature的维度
        double scoreLeft = 0;
        double scoreRight = 0;
        double scoreSquareLeft = 0;
        double scoreSquareRight = 0;
        double nLeft = 0;
        double nRight = 0;

        double[] scoreXRight = new double[hessianDim];
        double[] scoreXLeft = new double[hessianDim];
        double[] eDirRight = new double[hessianDim];
        double[] eDirLeft = new double[hessianDim];
        double score;

        for (Tuple3<Double, Double, Vector> labelVector : labelVectors) {
            score = labelVector.f2.dot(coefVector);
            double pos = 0;
            double neg = 0;
            if (labelVector.f1 == 1.0) {
                pos = labelVector.f0;
                for (int i = 0; i < hessianDim; i++) {
                    double posVal = pos * labelVector.f2.get(i);
                    scoreXRight[i] += posVal * score;
                    eDirRight[i] += posVal;
                }
            } else {
                neg = labelVector.f0;
                for (int i = 0; i < hessianDim; i++) {
                    double negVal = neg * labelVector.f2.get(i);
                    scoreXLeft[i] += negVal * score;
                    eDirLeft[i] += negVal;
                }
            }

            double nScore = neg * score;
            double pScore = pos * score;
            scoreLeft += nScore;
            scoreRight += pScore;
            scoreSquareLeft += nScore * score;
            scoreSquareRight += pScore * score;
            nRight += pos;
            nLeft += neg;
        }
        //updateMeanAndVariance
        //这边需要将参数写在一起进行allReduce。

        double[] res = new double[6 + 4 * hessianDim];
        res[0] = scoreLeft;
        res[1] = scoreRight;
        res[2] = scoreSquareLeft;
        res[3] = scoreSquareRight;
        res[4] = nLeft;
        res[5] = nRight;

        System.arraycopy(scoreXLeft, 0, res, 6, hessianDim);
        System.arraycopy(scoreXRight, 0, res, 6 + hessianDim, hessianDim);
        System.arraycopy(eDirLeft, 0, res, 6 + 2 * hessianDim, hessianDim);
        System.arraycopy(eDirRight, 0, res, 6 + 3 * hessianDim, hessianDim);

        return res;
    }
}
