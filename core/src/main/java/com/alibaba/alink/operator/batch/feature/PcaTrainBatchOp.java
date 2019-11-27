package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.common.linalg.*;
import com.alibaba.alink.operator.common.feature.pca.PcaTypeEnum;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.feature.pca.PcaModelDataConverter;
import com.alibaba.alink.operator.common.feature.pca.PcaModelData;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;

import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.feature.PcaTrainParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * PCA is dimension reduction of discrete feature, projects vectors to a low-dimensional space.
 * PcaTrainBatchOp is train a model which can be used to batch predict and stream predict
 * The calculation is done using eigen on the correlation or covariance matrix.
 */
public final class PcaTrainBatchOp extends BatchOperator<PcaTrainBatchOp>
    implements PcaTrainParams<PcaTrainBatchOp> {

    /**
     * block size when transmit
     */
    private static int block = 1024 * 1024;

    /**
     * default constructor
     */
    public PcaTrainBatchOp() {
        super(null);
    }

    /**
     * this constructor has all parameter
     *
     * @param params 参数
     *               selectedColNames: compute col names. when input is table, not tensor.
     *               tensorColName: compute tensor col. when input is tensor.
     *               isSparse: true is sparse tensor, false is dense tensor. default is false.
     *               pcaType: compute type, be CORR, COV_SAMPLE, COVAR_POP.
     *               CORR is correlation matrix，COV_SAMPLE is covariance of sample,COVAR_POP is covariance of
     *               population.
     *               p: number of principal component
     */
    public PcaTrainBatchOp(Params params) {
        super(params);
    }

    /**
     * @param inputs: data
     * @return PcaTrainBatchOp
     */
    @Override
    public PcaTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        //get parameters
        String[] selectedColNames = getSelectedCols();
        String vectorColName = getVectorCol();
        String calcType = getCalculationType();
        int k = getK();

        //convert table, dense tensor or sparse tensor to dense vector
        DataSet<Vector> data = StatisticsHelper.transformToVector(in, selectedColNames, vectorColName);

        //split vector for broadcast
        VectorSplit vectorSplit = new VectorSplit();

        //combine vector
        VecCombine vecCombine = new VecCombine(calcType, k, selectedColNames, vectorColName);

        DataSet<Row> srt = data
            .mapPartition(new StatisticsHelper.VectorSummarizerPartition(true))
            .flatMap(vectorSplit)
            .mapPartition(vecCombine).setParallelism(1);

        //convert model to table
        this.setOutput(srt, new PcaModelDataConverter().getModelSchema());

        return this;
    }

    /**
     * split rowNum,sum, squareSum, dot vector
     */
    public static class VectorSplit extends RichFlatMapFunction<BaseVectorSummarizer, Tuple2<Integer, DenseVector>> {
        @Override
        public void flatMap(BaseVectorSummarizer srt, Collector<Tuple2<Integer, DenseVector>> collector)
            throws Exception {
            BaseVectorSummary summary = srt.toSummary();
            int colNum = summary.vectorSize();

            //rowNum
            {
                double[] count = new double[colNum];
                Arrays.fill(count, summary.count());
                collector.collect(new Tuple2<>(0, new DenseVector(count)));
            }

            //sum
            {
                DenseVector sumsVec = toDenseVector(summary.sum());
                collector.collect(new Tuple2<>(1, sumsVec));
            }

            //squareSum
            {
                DenseVector sum2sVec = toDenseVector(summary.normL2());
                for (int i = 0; i < sum2sVec.size(); i++) {
                    double v = sum2sVec.get(i);
                    sum2sVec.set(i, v * v);
                }
                collector.collect(new Tuple2<>(2, sum2sVec));
            }

            //dotProduction split by blockSize
            int totalDotNum = colNum * (colNum + 1) * 2;

            double[] vec = new double[PcaTrainBatchOp.block + 1];
            vec[0] = (double) colNum;
            int vecIdx = 1;
            int collectIdx = 3;
            for (int i = 0; i < colNum; i++) {
                for (int j = i; j < colNum; j++) {
                    vec[vecIdx] = srt.getOuterProduct().get(i, j);
                    vecIdx++;
                    if (vecIdx == PcaTrainBatchOp.block + 1) {
                        DenseVector dotVec = new DenseVector(vec.clone());
                        collector.collect(new Tuple2<>(collectIdx, dotVec));
                        collectIdx++;
                        vecIdx = 1;
                        vec = new double[PcaTrainBatchOp.block + 1];
                        vec[0] = (double) colNum;
                    }
                }
            }
            if (totalDotNum % PcaTrainBatchOp.block > 0) {
                DenseVector dotVec = new DenseVector(vec.clone());
                collector.collect(new Tuple2<>(collectIdx, dotVec));
            }
        }
    }

    /**
     * combine rowNum, sum, squareSum, dotProduction matrix which split by VecSplit
     * and build pca model
     */
    public static class VecCombine extends RichMapPartitionFunction<Tuple2<Integer, DenseVector>, Row> {

        protected String pcaType;
        protected int p;
        protected String[] featureColNames;
        protected String tensorColName;

        public VecCombine(String pcaType, int p, String[] featureColNames, String tensorColName) {
            this.pcaType = pcaType;
            this.p = p;
            this.featureColNames = featureColNames;
            this.tensorColName = tensorColName;
        }

        /**
         * get covariance matrix
         *
         * @param counts     rowNum of cols
         * @param sums       sum of cols
         * @param dotProduct matrix of colNum * collnum, sum(x_i* x_j)
         * @param colNum     col number
         * @return covariance matrix
         */
        public static double[][] getCov(double[] counts, double[] sums, double[] dotProduct,
                                        int colNum) {
            double[][] cov = new double[colNum][colNum];
            double d = 0;
            int idx = 0;
            for (int i = 0; i < colNum; i++) {
                for (int j = i; j < colNum; j++) {
                    d = (dotProduct[idx] - sums[i] * sums[j] / counts[i]) / (counts[i] - 1);
                    cov[i][j] = d;
                    cov[j][i] = d;
                    idx++;
                }
            }
            return cov;
        }

        /**
         * get correlation matrix
         *
         * @param counts     rowNum of cols
         * @param sums       sum of cols
         * @param sum2s      sum(x_i^2) of cols
         * @param dotProduct matrix of colNum * colNum, sum(x_i* x_j)
         * @param colNum     col number
         * @return correlation matrix
         */
        static double[][] getCorr(double[] counts, double[] sums, double[] sum2s, double[] dotProduct,
                                  int colNum) {
            double[][] cov = getCov(counts, sums, dotProduct, colNum);
            double sdi = 0;
            double sdj = 0;
            double d = 0;
            for (int i = 0; i < colNum; i++) {
                sdi = Math.sqrt(Math.max(0.0, (sum2s[i] - sums[i] * sums[i] / counts[i]) / (counts[i] - 1)));
                for (int j = i; j < colNum; j++) {
                    sdj = Math.sqrt(Math.max(0.0, (sum2s[j] - sums[j] * sums[j] / counts[j]) / (counts[j] - 1)));
                    d = cov[i][j] / sdi / sdj;
                    cov[i][j] = d;
                    cov[j][i] = d;
                }
                cov[i][i] = 1.0;
            }
            return cov;
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Integer, DenseVector>> splitVec, Collector<Row> model) throws Exception {
            int nx = -1;

            //combine split vector from VectorSplit
            double[] counts = null;
            double[] sums = null;
            double[] sum2s = null;
            double[] dotProduct = null;
            for (Tuple2<Integer, DenseVector> tuple2 : splitVec) {
                if (tuple2 == null) {
                    continue;
                }

                if (nx < 0) {
                    //init
                    if (tuple2.f0 < 3) {
                        nx = tuple2.f1.size();
                    } else {
                        nx = (int) Math.round(tuple2.f1.get(0));
                    }
                    counts = new double[nx];
                    sums = new double[nx];
                    sum2s = new double[nx];
                    dotProduct = new double[nx * (nx + 1) / 2];
                }

                //combine count
                if (tuple2.f0 == 0) {
                    for (int i = 0; i < nx; i++) {
                        counts[i] += tuple2.f1.get(i);
                    }
                    continue;
                }

                //combine sum
                if (tuple2.f0 == 1) {
                    for (int i = 0; i < nx; i++) {
                        sums[i] += tuple2.f1.get(i);
                    }
                    continue;
                }

                //combine sum2
                if (tuple2.f0 == 2) {
                    for (int i = 0; i < nx; i++) {
                        sum2s[i] += tuple2.f1.get(i);
                    }
                    continue;
                }

                //combine dotProduct
                for (int i = 1; i < tuple2.f1.size(); i++) {
                    int idx = (tuple2.f0 - 3) * PcaTrainBatchOp.block + i - 1;
                    if (idx < dotProduct.length) {
                        dotProduct[idx] += tuple2.f1.get(i);
                    }
                }
            }

            //deal with the whole column is the same value: squareSum = sum * sum
            List<Integer> nonEqualColIdx = new ArrayList<>();
            for (int i = 0; i < nx; i++) {
                if (Math.abs(sum2s[i] - sums[i] * sums[i] / counts[i]) > 1e-10) {
                    nonEqualColIdx.add(i);
                }
            }

            int nxNe = nonEqualColIdx.size();
            int nxAll = nx;
            if (nxNe != nx) {
                double[] countsNe = new double[nxNe];
                double[] sumsNe = new double[nxNe];
                double[] sum2sNe = new double[nxNe];
                double[] dotProductNe = new double[nxNe * (nxNe + 1) / 2];

                int i = 0;
                for (int idx : nonEqualColIdx) {
                    countsNe[i] = counts[idx];
                    sumsNe[i] = sums[idx];
                    sum2sNe[i] = sum2s[idx];
                    dotProductNe[i] = dotProduct[idx];
                    i++;
                }

                counts = countsNe;
                sums = sumsNe;
                sum2s = sum2sNe;
                dotProduct = dotProductNe;

                nx = nxNe;
            }

            PcaModelData pcr = new PcaModelData();

            //get correlation or covariance matrix
            PcaTypeEnum pcaTypeEnum = PcaTypeEnum.valueOf(pcaType.toUpperCase());

            double[][] corr = null;

            switch (pcaTypeEnum) {
                case CORR:
                    corr = getCorr(counts, sums, sum2s, dotProduct, nx);
                    break;
                case COVAR_POP:
                case COV_SAMPLE:
                    corr = getCov(counts, sums, dotProduct, nx);
                    break;
                default:
                    throw new IllegalArgumentException("pca type not supported yet!");
            }


            DenseMatrix calculateMatrix = new DenseMatrix(corr);
            if (pcaTypeEnum.equals(PcaTypeEnum.COVAR_POP)) {
                double cnt = counts[0];
                if (cnt > 1) {
                    calculateMatrix.scaleEqual(cnt / (cnt - 1));
                } else {
                    throw new RuntimeException("record num is less than 2!");
                }
            }

            //get mean and stddev
            pcr.means = new double[nx];
            pcr.stddevs = new double[nx];

            for (int i = 0; i < nx; i++) {
                pcr.means[i] = sums[i] / counts[i];
                pcr.stddevs[i] = Math.sqrt(Math.max(0.0, (sum2s[i] - sums[i] * sums[i] / counts[i]) / (counts[i] - 1)));
            }

            if (p >= calculateMatrix.numCols()) {
                throw new RuntimeException(
                    "k is larger than vector size. k: " + p + " vectorSize: " + calculateMatrix.numCols());
            }

            //get eig values and eig vectors
            scala.Tuple2<DenseVector, DenseMatrix> eigValueAndVector = EigenSolver.solve(calculateMatrix, p, 10e-8, 300);
            if (eigValueAndVector._1.size() < p) {
                throw new RuntimeException("Fail to converge when solving eig value problem.");
            }

            //set model
            pcr.p = p;
            pcr.lambda = new double[p];
            for (int i = 0; i < p; i++) {
                pcr.lambda[i] = eigValueAndVector._1.get(i);
            }
            pcr.coef = new double[p][nx];
            for (int i = 0; i < p; i++) {
                for (int j = 0; j < nx; j++) {
                    pcr.coef[i][j] = eigValueAndVector._2.get(j, i);
                }
            }

            buildModel(pcr, nonEqualColIdx, nxAll, model);
        }

        /**
         * build pca model.
         * @param modelData: modelData
         * @param nonEqualColIndices: col indices of variance not zero.
         * @param nxAll: number of col.
         * @param model: model.
         * @return model
         */
        protected void buildModel(PcaModelData modelData, List<Integer> nonEqualColIndices, int nxAll, Collector<Row> model) {
            modelData.idxNonEqual = nonEqualColIndices.toArray(new Integer[0]);
            modelData.nx = nxAll;
            modelData.featureColNames = featureColNames;
            modelData.vectorColName = tensorColName;
            modelData.pcaType = this.pcaType;

            new PcaModelDataConverter().save(modelData, model);
        }
    }


    /**
     * dense vector or sparse vector to dense vector.
     * @param vector: dense vector or sparse vector.
     * @return dense vector.
     */
    private static DenseVector toDenseVector(Vector vector) {
        if (vector instanceof DenseVector) {
            return (DenseVector) vector;
        } else {
            return ((SparseVector) vector).toDenseVector();
        }
    }

}
