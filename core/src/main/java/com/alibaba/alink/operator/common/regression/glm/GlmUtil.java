package com.alibaba.alink.operator.common.regression.glm;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.LinearSolver;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.regression.GlmModelData;
import com.alibaba.alink.operator.common.regression.GlmModelDataConverter;
import com.alibaba.alink.operator.common.regression.glm.famliy.Binomial;
import com.alibaba.alink.operator.common.regression.glm.famliy.Family;
import com.alibaba.alink.operator.common.regression.glm.famliy.Gaussian;
import com.alibaba.alink.operator.common.regression.glm.famliy.Poisson;
import com.alibaba.alink.operator.common.regression.glm.link.Identity;
import com.alibaba.alink.operator.common.regression.glm.link.Link;
import com.github.fommil.netlib.BLAS;
import com.github.fommil.netlib.F2jBLAS;
import com.github.fommil.netlib.LAPACK;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.netlib.util.intW;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * GLM util.
 */
public class GlmUtil {
    public static double EPSILON = 1E-16;
    public static double DELTA = 0.1;


    /**
     *
     * @param coefficients: coefficient of each feature
     * @param intercept
     * @param features: feature values
     * @return sum{coefficients_i * features_i} + intercept
     */
    public static double linearPredict(double[] coefficients, double intercept, double[] features) {
        double preval = 0;
        for (int i = 0; i < coefficients.length; i++) {
            preval += coefficients[i] * features[i];
        }
        return preval + intercept;
    }

    /**
     *
     * @param coefficients : coefficient of each feature
     * @param intercept
     * @param features: feature values
     * @param offset: offset
     * @param familyLink: family and link function.
     * @return link(sum{coefficients_i * features_i} + intercept)
     */
    public static double predict(double[] coefficients, double intercept,
                                 double[] features, double offset,
                                 FamilyLink familyLink) {
        double eta = linearPredict(coefficients, intercept, features) + offset;
        return familyLink.fitted(eta);
    }


    /**
     *
     * @param in: input batch operator,
     * @param featureColNames: feature col names.
     * @param offsetColName: offset col name.
     * @param weightColName: weight col name.
     * @param labelColName: label col name.
     * @return DataSet<Row>:  which is feature,label,weight,offset.
     *         if not have weight col, weight will be set 1.
     *         if not have offset col, offset will be set 0.
     */
    public static DataSet<Row> preProc(BatchOperator in, String[] featureColNames,
                                       String offsetColName, String weightColName, String labelColName) {
        if (featureColNames == null || featureColNames.length == 0) {
            throw new RuntimeException("featureColNames must be set.");
        }

        int numFeature = featureColNames.length;

        int[] featureColIndices = new int[numFeature];
        for (int i = 0; i < numFeature; i++) {
            featureColIndices[i] = TableUtil.findColIndex(in.getColNames(), featureColNames[i]);
            if (-1 == featureColIndices[i]) {
                throw new RuntimeException(featureColNames[i] + " is not exist.");
            }
        }

        if (labelColName == null) {
            throw new RuntimeException("labelColName must be set.");
        }

        int labelColIdx = TableUtil.findColIndex(in.getColNames(), labelColName);
        if (-1 == labelColIdx) {
            throw new RuntimeException(labelColName + " is not exist.");
        }

        int weightColIdx = -1;
        if (weightColName != null && !weightColName.isEmpty()) {
            weightColIdx = TableUtil.findColIndex(in.getColNames(), weightColName);
            if (-1 == weightColIdx) {
                throw new RuntimeException(weightColName + " is not exist.");
            }
        }

        int offsetColIdx = -1;
        if (offsetColName != null && !offsetColName.isEmpty()) {
            offsetColIdx = TableUtil.findColIndex(in.getColNames(), offsetColName);
            if (-1 == offsetColIdx) {
                throw new RuntimeException(offsetColName + " is not exist.");
            }
        }

        return in.getDataSet()
            .map(new PreProcMapFunc(featureColIndices, labelColIdx, weightColIdx, offsetColIdx))
            .name("PreProcMapFunc");
    }

    /**
     *
     * @param data: input DataSet, format by preProc.
     * @param numFeature: number of features.
     * @param familyLink: family and link function.
     * @param regParam: L2.
     * @param fitIntercept: If true, fit intercept. If false, not fit intercept.
     * @param numIter: number of iter.
     * @param epsilon: epsilon.
     * @return DataSet of WeightedLeastSquaresModel.
     */
    public static DataSet<WeightedLeastSquaresModel> train(DataSet<Row> data, int numFeature,
                                                           FamilyLink familyLink,
                                                           double regParam, boolean fitIntercept,
                                                           int numIter, double epsilon) {

        String familyName = familyLink.getFamilyName();
        String linkName = familyLink.getLinkName();

        DataSet<WeightedLeastSquaresModel> finalModel = null;
        if (familyName.toLowerCase().equals("gaussian")
            && linkName.toLowerCase().equals("identity")) {

            finalModel = data.map(new GaussianDataProc(numFeature))
                .mapPartition(new LocalWeightStat(numFeature)).name("init LocalWeightStat")
                .reduce(new GlobalWeightStat()).name("init GlobalWeightStat")
                .mapPartition(new WeightedLeastSquares(fitIntercept, regParam, true, true))
                .setParallelism(1).name("init WeightedLeastSquares");

        } else {

            DataSet<WeightedLeastSquaresModel> initModel = data
                .map(new InitData(familyLink, numFeature))
                .mapPartition(new LocalWeightStat(numFeature)).name("init LocalWeightStat")
                .reduce(new GlobalWeightStat()).name("init GlobalWeightStat")
                .mapPartition(new WeightedLeastSquares(fitIntercept, regParam, true, true))
                .setParallelism(1).name("init WeightedLeastSquares");

            IterativeDataSet<WeightedLeastSquaresModel> loop = initModel.iterate(numIter).name("loop");

            DataSet<WeightedLeastSquaresModel> updateIrlsModel = data
                .map(new UpdateData(familyLink, numFeature + 3)).name("UpdateData")
                .withBroadcastSet(loop, "model")
                .mapPartition(new LocalWeightStat(numFeature)).name("localWeightStat")
                .reduce(new GlobalWeightStat()).name("GlobalWeightStat")
                .mapPartition(new WeightedLeastSquares(fitIntercept, regParam, false, false))
                .setParallelism(1).name("WLS");

            //converge
            DataSet<Tuple2<WeightedLeastSquaresModel, WeightedLeastSquaresModel>> join =
                loop.map(new ModelAddId())
                    .join(updateIrlsModel.map(new ModelAddId()))
                    .where(0).equalTo(0).projectFirst(1).projectSecond(1);

            FilterFunction<Tuple2<WeightedLeastSquaresModel, WeightedLeastSquaresModel>>
                filterCriterion = new IterCriterion(epsilon);

            DataSet<Tuple2<WeightedLeastSquaresModel, WeightedLeastSquaresModel>> criterion
                = join.filter(filterCriterion);

            finalModel = loop.closeWith(updateIrlsModel, criterion);
        }

        return finalModel;
    }

    /**
     *
     * @param model: WeightedLeastSquaresModel model.
     * @param data: input dataSet which format by preProc
     * @param numFeature: number of features.
     * @param familyLink: family and link function,
     * @return DataSet: features, label, weight, offset, devianceResidual, pearResidual, workingResidual, responseResidual
     */
    public static DataSet<Row> residual(DataSet<WeightedLeastSquaresModel> model, DataSet<Row> data, int numFeature,
                                        FamilyLink familyLink) {
        return data.map(new Residual(numFeature, familyLink))
            .withBroadcastSet(model, "model");
    }


    /**
     *
     * @param residual: result of residual function.
     * @param model: WeightedLeastSquaresModel.
     * @param numFeature: number of features.
     * @param familyLink: family and link function.
     * @param regParam: l2
     * @param numIter: number of iter.
     * @param epsion: epsilon
     * @param fitIntercept:  If true, fit intercept. If false, not fit intercept.
     * @return GlmModelSummay
     */
    public static DataSet<Row> aggSummary(DataSet<Row> residual, DataSet<WeightedLeastSquaresModel> model,
                                          int numFeature, FamilyLink familyLink, double regParam,
                                          int numIter, double epsion, boolean fitIntercept) {

        String familyName = familyLink.getFamilyName();
        String linkName = familyLink.getLinkName();

        DataSet<Double> intercept;
        if (fitIntercept) {
            if (familyName.equals((new Gaussian()).name()) && linkName.equals((new Identity()).name())) {
                intercept = residual.map(new GaussianInterceptMap(numFeature))
                    .reduce(new GlobalSum2())
                    .mapPartition(new GaussianIntercept());
            } else {
                DataSet<Row> nullData = residual.map(new NullProcMapFunc(numFeature));

                intercept = train(nullData, 0, familyLink, regParam, true, numIter, epsion)
                    .mapPartition(new MapPartitionFunction<WeightedLeastSquaresModel, Double>() {
                        @Override
                        public void mapPartition(Iterable<WeightedLeastSquaresModel> iterable,
                                                 Collector<Double> result) {
                            for (WeightedLeastSquaresModel model : iterable) {
                                result.collect(model.intercept);
                            }
                        }
                    });
            }
        } else {
            intercept = residual.map(new MapFunction<Row, Double>() {
                @Override
                public Double map(Row row) {
                    return 0.0;
                }
            }).reduce(new GlobalSum());
        }

        DataSet<Tuple5<Double, Double, Double, Double, Double>> der = residual
            .map(new summayTransform(numFeature, familyLink))
            .withBroadcastSet(intercept, "intercept")
            .reduce(new GlobalSum5());

        DataSet<Double> aic = aic(residual, der, numFeature, familyLink);

        return aic.map(new AggSummary(fitIntercept, numFeature, familyLink.getFamilyName()))
            .withBroadcastSet(der, "deviance")
            .withBroadcastSet(aic, "aic")
            .withBroadcastSet(model, "model");
    }

    /**
     *
     * @param residual: result of function residual.
     * @param der: dataSet of row: nullDeviance, deviance, dispersion, weight, 1.0
     * @param numFeature: number of features.
     * @param familyLink: family and link function.
     * @return aic dataSet.
     */
    private static DataSet<Double> aic(
        DataSet<Row> residual,
        DataSet<Tuple5<Double, Double, Double, Double, Double>> der,
        int numFeature,
        FamilyLink familyLink) {

        String familyName = familyLink.getFamilyName();

        DataSet<Double> aic = null;
        if (familyName.equals("tweedie")) {
            aic = der.map(new MapFunction<Tuple5<Double, Double, Double, Double, Double>, Double>() {
                @Override
                public Double map(Tuple5<Double, Double, Double, Double, Double> doubleDoubleDoubleTuple3)
                    throws Exception {
                    return Double.MAX_VALUE;
                }
            });
        } else if (familyName.equals("binomial")) {
            aic = residual.map(new BinomialAicTransform(numFeature))
                .reduce(new GlobalSum())
                .mapPartition(new MapPartitionFunction<Double, Double>() {
                    @Override
                    public void mapPartition(Iterable<Double> iterable, Collector<Double> result) {
                        for (Double val : iterable) {
                            result.collect(-2.0 * val);
                        }
                    }
                });
        } else if (familyName.equals("gamma")) {
            aic = residual.map(new GammaAicTransform(numFeature))
                .withBroadcastSet(der, "deviance")
                .reduce(new GlobalSum())
                .mapPartition(new MapPartitionFunction<Double, Double>() {
                    @Override
                    public void mapPartition(Iterable<Double> iterable, Collector<Double> result) {
                        for (Double val : iterable) {
                            result.collect(-2.0 * val + 2.0);
                        }
                    }
                });

        } else if (familyName.equals("poisson")) {
            aic = residual.map(new PossionAicTransform(numFeature))
                .withBroadcastSet(der, "deviance")
                .reduce(new GlobalSum())
                .mapPartition(new MapPartitionFunction<Double, Double>() {
                    @Override
                    public void mapPartition(Iterable<Double> iterable, Collector<Double> result) {
                        for (Double val : iterable) {
                            result.collect(-2.0 * val);
                        }
                    }
                });
        } else if (familyName.equals("gaussian")) {
            aic = residual.map(new GaussionAicTransform1(numFeature))
                .reduce(new GlobalSum())
                .map(new GaussionAicTransform2())
                .withBroadcastSet(der, "deviance");
        } else {
            throw new RuntimeException("family name not support yet." + familyName);
        }

        return aic;
    }

    /**
     *
     * @param row: features, label, weight, offset.
     * @param numFeature: number of features.
     * @param familyLink: family and link function.
     * @param coefficients: coefficient of each features.
     * @param intercept: intercept.
     * @return features, label, weight, offset, devianceResidual, pearResidual, workingResidual, responseResidual
     */
    private static Row residualRow(Row row, int numFeature, FamilyLink familyLink,
                                   double[] coefficients, double intercept) {
        double[] features = new double[numFeature];
        for (int i = 0; i < numFeature; i++) {
            features[i] = (double) row.getField(i);
        }

        double label = (Double) row.getField(numFeature);
        double weight = (Double) row.getField(numFeature + 1);
        double offset = (Double) row.getField(numFeature + 2);
        double pred = GlmUtil.predict(coefficients, intercept, features, offset, familyLink);
        double dr = devianceResiduals(familyLink.getFamily(), label, pred, weight);
        double pr = pearResiduals(familyLink.getFamily(), label, pred, weight);
        double wr = workingResiduals(familyLink.getLink(), label, pred, weight);
        double rr = responseResiduals(label, pred);

        int numRow = row.getArity();
        Row outRow = new Row(numRow + 5);
        for (int i = 0; i < numRow; i++) {
            outRow.setField(i, row.getField(i));
        }
        outRow.setField(numRow, pred);
        outRow.setField(numRow + 1, dr);
        outRow.setField(numRow + 2, pr);
        outRow.setField(numRow + 3, wr);
        outRow.setField(numRow + 4, rr);

        return outRow;
    }

    /**
     *
     * @param family: family.
     * @param label: label value.
     * @param pred: predict value.
     * @param weight: weight value.
     * @return devianceResiduals
     */
    private static double devianceResiduals(Family family, double label, double pred, double weight) {
        double r = Math.sqrt(Math.max(family.deviance(label, pred, weight), 0.0));
        if (label <= pred) {
            r *= -1.0;
        }
        return r;
    }

    /**
     *
     * @param family: family.
     * @param label: label value.
     * @param pred: predict value.
     * @param weight: weight value.
     * @return pearResiduals
     */
    private static double pearResiduals(Family family, double label, double pred, double weight) {
        return (label - pred) * Math.sqrt(weight) / (Math.sqrt(family.variance(pred)));
    }


    /**
     *
     * @param link: link function.
     * @param label: label value.
     * @param pred: predict value.
     * @param weight: weight value.
     * @return workingResiduals
     */
    private static double workingResiduals(Link link, double label, double pred, double weight) {
        return (label - pred) * link.derivative(pred);
    }

    /**
     *
     * @param label: label value.
     * @param pred: predict value.
     * @return responseResiduals
     */
    private static double responseResiduals(double label, double pred) {
        return label - pred;
    }

    /**
     *
     * @param row: input data.
     * @param featureColIdxs:  feature col indices.
     * @param labelColIdx: label col idx.
     * @param weightColIdx: weight col idx.
     * @param offsetColIdx: offset col idx.
     * @return
     */
    private static Row preProcRow(Row row, int[] featureColIdxs, int labelColIdx, int weightColIdx, int offsetColIdx) {
        int n = featureColIdxs.length;

        Row outRow = new Row(n + 3);

        for (int i = 0; i < n; i++) {
            outRow.setField(i, row.getField(featureColIdxs[i]));
        }

        outRow.setField(n, row.getField(labelColIdx));

        if (weightColIdx == -1) {
            outRow.setField(n + 1, 1.0);
        } else {
            outRow.setField(n + 1, row.getField(weightColIdx));
        }

        if (offsetColIdx == -1) {
            outRow.setField(n + 2, 0.0);
        } else {
            outRow.setField(n + 2, row.getField(offsetColIdx));
        }
        return outRow;
    }

    /**
     * Glm model summary.
     */
    private static class GlmModelSummay implements Serializable {
        public int rank;
        public long degreeOfFreedom;
        public long residualDegreeOfFreeDom;
        public long residualDegreeOfFreedomNull;
        public double aic;
        public double dispersion;
        public double deviance;
        public double nullDeviance;
        public double[] coefficients;
        public double intercept;
        public double[] coefficientStandardErrors;
        public double[] tValues;
        public double[] pValues;
    }

    /**
     * weighted least square.
     */
    public static class WeightedLeastSquaresModel implements Serializable {
        public double[] coefficients;
        public double intercept = 0;
        public double[] diagInvAtWA;
        public boolean fitIntercept;
        public long numInstances;
    }

    /**
     * model add id.
     */
    private static class ModelAddId
        implements MapFunction<WeightedLeastSquaresModel, Tuple2<Integer, WeightedLeastSquaresModel>> {
        @Override
        public Tuple2<Integer, WeightedLeastSquaresModel> map(WeightedLeastSquaresModel model) throws Exception {
            return new Tuple2<>(0, model);
        }
    }

    /**
     * agg summary.
     */
    private static class AggSummary extends RichMapFunction<Double, Row> {
        private boolean fitIntercept;
        private int numFeature;
        private String familyName;

        private long count;
        private Double aic;
        private double nullDeviance;
        private double deviance;
        private double dispersion;
        private WeightedLeastSquaresModel model;

        /**
         *
         * @param fitIntercept: If true, fit intercept. If false, not fit intercept.
         * @param numFeature: number of feature.
         * @param familyName: family.
         */
        public AggSummary(boolean fitIntercept, int numFeature, String familyName) {
            this.fitIntercept = fitIntercept;
            this.numFeature = numFeature;
            this.familyName = familyName;
        }

        public void open(Configuration configuration) {
            //nullDeviance, deviance, dispersion, weightsum, count
            Tuple5<Double, Double, Double, Double, Double> der =
                (Tuple5<Double, Double, Double, Double, Double>) getRuntimeContext().getBroadcastVariable("deviance")
                    .get(0);
            count = Math.round(der.f4);
            nullDeviance = der.f0;
            deviance = der.f1;
            dispersion = der.f2;

            aic = (Double) getRuntimeContext().getBroadcastVariable("aic").get(0);

            model = (WeightedLeastSquaresModel) getRuntimeContext().getBroadcastVariable("model").get(0);
        }

        @Override
        public Row map(Double aDouble) throws Exception {
            GlmModelSummay summay = new GlmModelSummay();
            summay.rank = rank();
            summay.degreeOfFreedom = degreeOfFreedom();
            summay.residualDegreeOfFreeDom = residualDegreeOfFreeDom();
            summay.residualDegreeOfFreedomNull = residualDegreeOfFreedomNull();
            summay.aic = aic();
            summay.dispersion = dispersion();
            summay.deviance = deviance();
            summay.nullDeviance = nullDeviance();
            summay.coefficients = model.coefficients;
            summay.intercept = model.intercept;
            summay.coefficientStandardErrors = coefficientStandardErrors();
            summay.tValues = tValues();
            summay.pValues = pValues();

            Row row = new Row(1);
            row.setField(0, JsonConverter.gson.toJson(summay));

            return row;
        }

        /**
         * @return rank.
         */
        public int rank() {
            return fitIntercept ? numFeature + 1 : numFeature;
        }

        /**
         * @return degree of freedom.
         */
        public long degreeOfFreedom() {
            return count - rank();
        }

        /**
         *
         * @return residual degree
         */
        public long residualDegreeOfFreeDom() {
            return degreeOfFreedom();
        }


        /**
         *
         * @return residual degree.
         */
        public long residualDegreeOfFreedomNull() {
            return fitIntercept ? count - 1 : count;
        }

        /**
         *
         * @return aic.
         */
        public double aic() {
            if (aic == null) {
                return Double.MIN_VALUE;
            }
            return aic + 2 * rank();
        }

        /**
         *
         * @return dispersion.
         */
        public double dispersion() {
            if (familyName.equals((new Binomial()).name()) || familyName.equals((new Poisson()).name())) {
                return 1.0;
            } else {
                return dispersion / degreeOfFreedom();
            }
        }

        /**
         *
         * @return deviance.
         */
        public double deviance() {
            return deviance;
        }

        /**
         *
         * @return null deviance.
         */
        public double nullDeviance() {
            if (Double.isNaN(nullDeviance)) {
                return Double.MIN_VALUE;
            } else {
                return nullDeviance;
            }
        }

        /**
         *
         * @return standard error of coefficient.
         */
        public double[] coefficientStandardErrors() {
            double[] stderr = new double[model.diagInvAtWA.length];
            double dispersion = dispersion();
            for (int i = 0; i < stderr.length; i++) {
                stderr[i] = Math.sqrt(model.diagInvAtWA[i] * dispersion);
            }
            return stderr;
        }

        /**
         *
         * @return t value.
         */
        public double[] tValues() {
            double[] tVals = new double[model.diagInvAtWA.length];
            double[] stderr = coefficientStandardErrors();
            for (int i = 0; i < numFeature; i++) {
                tVals[i] = model.coefficients[i] / stderr[i];
            }
            if (fitIntercept) {
                tVals[numFeature] = model.intercept / stderr[numFeature];
            }
            return tVals;
        }

        /**
         *
         * @return p value.
         */
        public double[] pValues() {
            double[] tVals = tValues();
            double[] pVals = new double[tVals.length];

            if (familyName.equals((new Binomial()).name()) || familyName.equals((new Poisson()).name())) {
                NormalDistribution distribution = new NormalDistribution();
                for (int i = 0; i < tVals.length; i++) {
                    pVals[i] = 2.0 * (1.0 - distribution.cumulativeProbability(Math.abs(tVals[i])));
                }
            } else {
                TDistribution distribution = new TDistribution(degreeOfFreedom());
                for (int i = 0; i < tVals.length; i++) {
                    pVals[i] = 2.0 * (1.0 - distribution.cumulativeProbability(Math.abs(tVals[i])));
                }
            }

            return pVals;
        }
    }

    /**
     * iter criterion.
     */
    public static class IterCriterion
        implements FilterFunction<Tuple2<WeightedLeastSquaresModel, WeightedLeastSquaresModel>> {
        private double epsilon;

        private IterCriterion(double epsilon) {
            this.epsilon = epsilon;
        }

        @Override
        public boolean filter(Tuple2<WeightedLeastSquaresModel, WeightedLeastSquaresModel> tuple2) {
            WeightedLeastSquaresModel oldModel = tuple2.f0;
            WeightedLeastSquaresModel newModel = tuple2.f1;
            double maxTol = Math.abs(newModel.intercept - oldModel.intercept);
            for (int i = 0; i < newModel.coefficients.length; i++) {
                double coefTol = Math.abs(newModel.coefficients[i] - oldModel.coefficients[i]);
                if (coefTol > maxTol) {
                    maxTol = coefTol;
                }
            }
            System.out.println("maxTol: " + maxTol);
            return maxTol <= epsilon ? false : true;
        }
    }

    /**
     * Gaussion Aic.
     */
    private static class GaussionAicTransform1 extends RichMapFunction<Row, Double> {
        private int numFeature;

        public GaussionAicTransform1(int numFeature) {
            this.numFeature = numFeature;
        }

        @Override
        public Double map(Row row) {
            double weight = (Double) row.getField(numFeature + 1);
            return Math.log(weight);
        }
    }

    /**
     * Gaussion Aic.
     */
    private static class GaussionAicTransform2 extends RichMapFunction<Double, Double> {
        private double deviance;
        private double weightSum;
        private double count;

        public GaussionAicTransform2() {
        }

        public void open(Configuration configuration) {
            Tuple5<Double, Double, Double, Double, Double> der =
                (Tuple5<Double, Double, Double, Double, Double>) getRuntimeContext().getBroadcastVariable("deviance")
                    .get(0);
            deviance = der.f1;
            weightSum = der.f3;
            count = der.f4;

        }

        @Override
        public Double map(Double wt) {
            return count * (Math.log(deviance / count * 2.0 * Math.PI) + 1.0) + 2.0 - wt;
        }
    }

    /**
     * Possion Aic.
     */
    private static class PossionAicTransform extends RichMapFunction<Row, Double> {
        private int numFeature;
        private double disp;

        public PossionAicTransform(int numFeature) {
            this.numFeature = numFeature;
        }

        @Override
        public Double map(Row row) throws Exception {
            double label = (Double) row.getField(numFeature);
            double weight = (Double) row.getField(numFeature + 1);
            double pred = (Double) row.getField(numFeature + 3);

            PoissonDistribution distribution = new PoissonDistribution(pred);
            return weight * Math.log(distribution.probability((int) label));
        }
    }

    /**
     * Gamma Aic.
     */
    private static class GammaAicTransform extends RichMapFunction<Row, Double> {
        private int numFeature;
        private double disp;

        public GammaAicTransform(int numFeature) {
            this.numFeature = numFeature;
        }

        public void open(Configuration configuration) {
            Tuple5<Double, Double, Double, Double, Double> der =
                (Tuple5<Double, Double, Double, Double, Double>) getRuntimeContext().getBroadcastVariable("deviance")
                    .get(0);
            double deviance = der.f1;
            double weightSum = der.f3;
            disp = deviance / weightSum;
        }

        @Override
        public Double map(Row row) throws Exception {
            double label = (Double) row.getField(numFeature);
            double weight = (Double) row.getField(numFeature + 1);
            double pred = (Double) row.getField(numFeature + 3);

            GammaDistribution distribution = new GammaDistribution(1.0 / disp, 1 / (pred * disp));
            return weight * Math.log(distribution.density(label));
        }
    }

    /**
     * Binomial Aic.
     */
    private static class BinomialAicTransform implements MapFunction<Row, Double> {
        private int numFeature;

        BinomialAicTransform(int numFeature) {
            this.numFeature = numFeature;
        }

        @Override
        public Double map(Row row) throws Exception {
            double label = (Double) row.getField(numFeature);
            double weight = (Double) row.getField(numFeature + 1);
            double pred = (Double) row.getField(numFeature + 3);

            int wt = (int) Math.round(weight);
            if (wt == 0) {
                return 0.0;
            } else {
                return logProbability(wt, pred, (int) Math.round(label * weight));
            }
        }

        private double logI(Boolean b) {
            return b ? 0.0 : Double.NEGATIVE_INFINITY;
        }

        private double logProbability(int n, double p, int k) {
            if (p == 0) {
                return logI(k == 0);
            } else if (p == 1) {
                return logI(k == n);
            } else {
                return lnGamma(n + 1) - lnGamma(k + 1) - lnGamma(n - k + 1) + k * Math.log(p)
                    + (n - k) * Math.log(1 - p);
            }
        }
    }


    private static final double gLanczos = 4.7421875;
    private static final double[] ckLanczos = new double[]{
        0.99999999999999709182,
        57.156235665862923517,
        -59.597960355475491248,
        14.136097974741747174,
        -0.49191381609762019978,
        .33994649984811888699e-4,
        .46523628927048575665e-4,
        -.98374475304879564677e-4,
        .15808870322491248884e-3,
        -.21026444172410488319e-3,
        .21743961811521264320e-3,
        -.16431810653676389022e-3,
        .84418223983852743293e-4,
        -.26190838401581408670e-4,
        .36899182659531622704e-5};

    private static double lnGamma(double x) {
        if (x <= 0) {
            throw new RuntimeException("para is out of range!");
        }
        double t = ckLanczos[0];
        for (int i = 1; i < ckLanczos.length; i++) {
            t += ckLanczos[i] / (x + i - 1);
        }
        double s = x + gLanczos - 0.5;
        return (x - 0.5) * Math.log(s) - s + Math.log(Math.sqrt(2 * Math.PI) * t);
    }

    /**
     * calculate nullDeviance, deviance, dispersion, weight, count.
     */
    private static class summayTransform
        extends RichMapFunction<Row, Tuple5<Double, Double, Double, Double, Double>> {
        private double intercept;
        private FamilyLink familyLink;
        private int numFeature;

        public summayTransform(int numFeature, FamilyLink familyLink) {
            this.familyLink = familyLink;
            this.numFeature = numFeature;
        }

        public void open(Configuration configuration) {
            intercept = (Double) getRuntimeContext().getBroadcastVariable("intercept").get(0);
        }

        @Override
        public Tuple5<Double, Double, Double, Double, Double> map(Row row) {
            double label = (Double) row.getField(numFeature);
            double weight = (Double) row.getField(numFeature + 1);
            double offset = (Double) row.getField(numFeature + 2);
            double pred = (Double) row.getField(numFeature + 3);
            double pr = (Double) row.getField(numFeature + 5);

            double nullDeviance = familyLink.getFamily().deviance(label,
                familyLink.getLink().unlink(intercept + offset), weight);
            double deviance = familyLink.getFamily().deviance(label, pred, weight);
            double dispersion = 1.0;

            if (!familyLink.getFamilyName().equals("binomial") && !familyLink.getFamilyName().equals("poisson")) {
                dispersion = pr * pr;
            }

            return new Tuple5<>(nullDeviance, deviance, dispersion, weight, 1.0);
        }
    }

    /**
     * global tuole 5.
     */
    private static class GlobalSum5 implements ReduceFunction<Tuple5<Double, Double, Double, Double, Double>> {
        @Override
        public Tuple5<Double, Double, Double, Double, Double> reduce(
            Tuple5<Double, Double, Double, Double, Double> left,
            Tuple5<Double, Double, Double, Double, Double> right) {
            return new Tuple5<>(left.f0 + right.f0,
                left.f1 + right.f1,
                left.f2 + right.f2,
                left.f3 + right.f3,
                left.f4 + right.f4);
        }
    }

    /**
     * global tuple 2.
     */
    private static class GlobalSum2 implements ReduceFunction<Tuple2<Double, Double>> {
        @Override
        public Tuple2<Double, Double> reduce(Tuple2<Double, Double> left, Tuple2<Double, Double> right)
            throws Exception {
            return new Tuple2<>(left.f0 + right.f0,
                left.f1 + right.f1);
        }
    }

    /**
     * global tuple1.
     */
    private static class GlobalSum implements ReduceFunction<Double> {
        @Override
        public Double reduce(Double left, Double right) throws Exception {
            return left + right;
        }
    }

    /**
     * Gaussian Intercept.
     */
    private static class GaussianInterceptMap implements MapFunction<Row, Tuple2<Double, Double>> {
        private int numFeature;

        GaussianInterceptMap(int numFeature) {
            this.numFeature = numFeature;
        }

        @Override
        public Tuple2<Double, Double> map(Row row) {
            double label = (Double) row.getField(numFeature);
            double weight = (Double) row.getField(numFeature + 1);
            double offset = (Double) row.getField(numFeature + 2);

            return new Tuple2<>(weight * (label - offset), weight);
        }
    }

    /**
     * Gaussian Intercept.
     */
    private static class GaussianIntercept implements MapPartitionFunction<Tuple2<Double, Double>, Double> {
        @Override
        public void mapPartition(Iterable<Tuple2<Double, Double>> iterable, Collector<Double> result)
            throws Exception {
            for (Tuple2<Double, Double> tuple2 : iterable) {
                result.collect(tuple2.f0 / tuple2.f1);
            }

        }
    }

    /**
     * calculate devianceResiduals, pearResiduals,workingResiduals,responseResiduals.
     */
    private static class Residual extends RichMapFunction<Row, Row> {
        private int numFeature;
        private FamilyLink familyLink;
        private double[] coefficients;
        private double intercept;

        public Residual(int numFeature, FamilyLink familyLink) {
            this.numFeature = numFeature;
            this.familyLink = familyLink;
        }

        public void open(Configuration configuration) {
            WeightedLeastSquaresModel model = (WeightedLeastSquaresModel) getRuntimeContext().getBroadcastVariable(
                "model").get(0);
            coefficients = model.coefficients;
            intercept = model.intercept;
        }

        @Override
        public Row map(Row row) {
            return residualRow(row, numFeature, familyLink, coefficients, intercept);
        }
    }

    /**
     * data proc for gaussian family.
     * return feature,label,weight,offset.
     */
    private static class GaussianDataProc implements MapFunction<Row, Row> {
        private int numFeature;

        public GaussianDataProc(int numFeature) {
            this.numFeature = numFeature;
        }

        @Override
        public Row map(Row row) {
            Row outRow = new Row(row.getArity());
            for (int i = 0; i < numFeature; i++) {
                outRow.setField(i, row.getField(i));
            }
            double label = (Double) row.getField(numFeature);
            double weight = (Double) row.getField(numFeature + 1);
            double offset = (Double) row.getField(numFeature + 2);

            outRow.setField(numFeature, label - offset);
            outRow.setField(numFeature + 1, weight);
            outRow.setField(numFeature + 2, 0.0);

            return outRow;
        }
    }

    /**
     * Update Data.
     */
    private static class UpdateData extends RichMapFunction<Row, Row> {
        private FamilyLink familyLink;
        private double[] features;
        private double[] coefficients;
        private double intercept;

        public UpdateData(FamilyLink familyLink,
                          int rowSize) {
            this.familyLink = familyLink;
            this.features = new double[rowSize];
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            WeightedLeastSquaresModel model =
                (WeightedLeastSquaresModel) getRuntimeContext().getBroadcastVariable("model").get(0);

            this.coefficients = model.coefficients;
            this.intercept = model.intercept;
        }

        @Override
        public Row map(Row row) throws Exception {
            for (int i = 0; i < features.length; i++) {
                features[i] = (double) row.getField(i);
            }
            features = familyLink.calcWeightAndLabel(this.coefficients, this.intercept, features);

            Row outRow = new Row(features.length);
            for (int i = 0; i < features.length; i++) {
                outRow.setField(i, features[i]);
            }
            return outRow;
        }
    }

    /**
     * init data.
     */
    private static class InitData extends RichMapFunction<Row, Row> {
        private FamilyLink familyLink;
        private int numFeature;

        InitData(FamilyLink familyLink, int numFeature) {
            this.familyLink = familyLink;
            this.numFeature = numFeature;
        }

        @Override
        public Row map(Row row) throws Exception {
            double label = (Double) row.getField(numFeature);
            double weight = (Double) row.getField(numFeature + 1);
            double offset = (Double) row.getField(numFeature + 2);

            double mu = familyLink.getFamily().initialize(label, weight);
            double eta = familyLink.predict(mu) - offset;

            Row outRow = Row.copy(row);
            outRow.setField(numFeature, eta);
            return outRow;
        }
    }

    /**
     * pre proc for null features.
     * return  feature,label,weight,offset.
     */
    private static class NullProcMapFunc implements MapFunction<Row, Row> {
        private int numFeature;

        public NullProcMapFunc(int numFeature) {
            this.numFeature = numFeature;
        }

        @Override
        public Row map(Row row) throws Exception {
            Row outRow = new Row(3);

            outRow.setField(0, row.getField(numFeature));
            outRow.setField(1, row.getField(numFeature + 1));
            outRow.setField(2, row.getField(numFeature + 2));

            return outRow;
        }
    }

    /**
     * local weight stat.
     */
    private static class LocalWeightStat extends RichMapPartitionFunction<Row, WeightStat> {
        private int featureSize;
        private double[] features;

        public LocalWeightStat(int featureSize) {
            this.featureSize = featureSize;
            this.features = new double[featureSize];
        }

        @Override
        public void mapPartition(Iterable<Row> iterable, Collector<WeightStat> result) throws Exception {
            WeightStat weightStat = new WeightStat(featureSize);

            double label;
            double weight;
            for (Row row : iterable) {
                for (int i = 0; i < featureSize; i++) {
                    features[i] = (Double) row.getField(i);
                }
                label = (Double) row.getField(featureSize);
                weight = (Double) row.getField(featureSize + 1);

                weightStat.add(features, label, weight);
            }

            result.collect(weightStat);
        }
    }

    /**
     * global weight stat.
     */
    private static class GlobalWeightStat implements ReduceFunction<WeightStat> {
        @Override
        public WeightStat reduce(WeightStat left, WeightStat right) throws Exception {
            return WeightStat.merge(left, right);
        }
    }

    /**
     * weighted least squares.
     */
    private static class WeightedLeastSquares extends RichMapPartitionFunction<WeightStat,
        WeightedLeastSquaresModel> {
        private boolean fitIntercept;
        private double regParam;
        private boolean standardizeFeatures;
        private boolean standardizeLabel;
        private LAPACK lapack = null;

        WeightedLeastSquares(boolean fitIntercept,
                             double regParam,
                             boolean standardizeFeatures,
                             boolean standardizeLabel) {
            this.fitIntercept = fitIntercept;
            this.regParam = regParam;
            this.standardizeFeatures = standardizeFeatures;
            this.standardizeLabel = standardizeLabel;
        }

        public void open(Configuration config) {
            this.lapack = LAPACK.getInstance();
        }

        @Override
        public void mapPartition(Iterable<WeightStat> iterable, Collector<WeightedLeastSquaresModel> result)
            throws Exception {
            for (WeightStat summary : iterable) {
                int k = fitIntercept ? summary.k + 1 : summary.k;
                int numFeatures = summary.k;
                int triK = summary.triK;
                double wSum = summary.wSum;
                double rawBStdDeviation = summary.bStdDeviation();
                double rawBMean = summary.bMean();
                double bStdDeviation = rawBStdDeviation == 0.0 ? Math.abs(rawBMean) : rawBStdDeviation;

                double bMean = summary.bMean() / bStdDeviation;

                double[] aStdDeviations = summary.aStdDeviation();
                double[] aMeans = summary.aMean();
                for (int i = 0; i < numFeatures; i++) {
                    if (0.0 == aStdDeviations[i]) {
                        aMeans[i] = 0;
                    } else {
                        aMeans[i] /= aStdDeviations[i];
                    }
                }
                double[] abMeans = summary.abMean();
                for (int i = 0; i < numFeatures; i++) {
                    if (0.0 == aStdDeviations[i]) {
                        abMeans[i] = 0;
                    } else {
                        abMeans[i] /= (aStdDeviations[i] * bStdDeviation);
                    }
                }

                double[] aaMeans = summary.aaMean();

                int j = 0;
                int p = 0;

                while (j < numFeatures) {
                    double aStdJ = aStdDeviations[j];
                    int i = 0;
                    while (i <= j) {
                        double aStdI = aStdDeviations[i];
                        if (aStdJ == 0.0 || aStdI == 0.0) {
                            aaMeans[p] = 0.0;
                        } else {
                            aaMeans[p] /= (aStdI * aStdJ);
                        }
                        p += 1;
                        i += 1;
                    }
                    j += 1;
                }

                double effectiveL2RegParam = regParam / bStdDeviation;

                // add L2 regularization to diagonals
                int i = 0;
                j = 2;
                while (i < triK) {
                    double lambda = effectiveL2RegParam;
                    if (!standardizeFeatures) {
                        double std = aStdDeviations[j - 2];
                        if (std != 0.0) {
                            lambda /= (std * std);
                        } else {
                            lambda = 0.0;
                        }
                    }
                    if (!standardizeLabel) {
                        lambda *= bStdDeviation;
                    }

                    aaMeans[i] += lambda;
                    i += j;
                    j += 1;
                }

                double[] ata = getATA(aaMeans, aMeans);
                double[] atb = getATB(abMeans, bMean);

                double[] x = solve(ata, atb);
                double[] aaInv = inverse(ata, atb.length);

                WeightedLeastSquaresModel model = new WeightedLeastSquaresModel();
                model.coefficients = new double[numFeatures];
                for (i = 0; i < numFeatures; i++) {
                    model.coefficients[i] = x[i];
                }
                if (fitIntercept) {
                    model.intercept = x[numFeatures] * bStdDeviation;
                }

                for (int q = 0; q < numFeatures; q++) {
                    if (aStdDeviations[q] != 0.0) {
                        model.coefficients[q] *= bStdDeviation / aStdDeviations[q];
                    } else {
                        model.coefficients[q] = 0.0;
                    }
                }

                model.diagInvAtWA = new double[k];
                for (i = 1; i <= k; i++) {
                    double multiplier;
                    if (fitIntercept && i == k) {
                        multiplier = 1.0;
                    } else {
                        multiplier = aStdDeviations[i - 1] * aStdDeviations[i - 1];
                    }

                    model.diagInvAtWA[i - 1] = aaInv[i + (i - 1) * i / 2 - 1] / (wSum * multiplier);
                }

                model.numInstances = summary.count;
                model.fitIntercept = fitIntercept;

                result.collect(model);
            }
        }

        private double[] getATA(double[] aaBar, double[] aBar) {
            if (fitIntercept) {
                double[] aa = new double[aaBar.length + aBar.length + 1];
                System.arraycopy(aaBar, 0, aa, 0, aaBar.length);
                System.arraycopy(aBar, 0, aa, aaBar.length, aBar.length);
                aa[aaBar.length + aBar.length] = 1.0;
                return aa;
            } else {
                return aaBar.clone();
            }
        }

        private double[] getATB(double[] abBar, double bBar) {
            if (fitIntercept) {
                double[] bb = new double[abBar.length + 1];
                System.arraycopy(abBar, 0, bb, 0, abBar.length);
                bb[abBar.length] = bBar;
                return bb;
            } else {
                return abBar.clone();
            }
        }

        //a will be change
        private double[] inverse(double[] a, int k) {
            intW info = new intW(0);
            lapack.dpptri("U", k, a, info);

            if (info.val == 0) {
                DenseMatrix dm = toMatrix(a, k);
                DenseMatrix one = DenseMatrix.eye(k, k);
                LinearSolver.underDeterminedSolve(dm, one);
                int idx = 0;
                for (int i = 0; i < k; i++) {
                    for (int j = 0; j < k; j++) {
                        if (i <= j) {
                            a[idx] = one.get(i, j);
                            idx++;
                        }
                    }
                }
            }
            return a;
        }

        private DenseMatrix inverse(DenseMatrix a) {
            int N = a.numCols();
            double[] ainv = new double[N * N];
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < N; j++) {
                    ainv[i * N + j] = a.get(i, j);
                }
            }

            int n = N;
            int[] ipiv = new int[N + 1];
            intW info = new intW(0);

            lapack.dgetrf(n, n, ainv, n, ipiv, info);

            int lwork = N * N;
            double[] work = new double[lwork];
            lapack.dgetri(n, ainv, n, ipiv, work, lwork, info);

            return new DenseMatrix(N, n, ainv, true);
        }

        //a will be change
        private double[] solve(double[] a, double[] bx) {
            double[] bxClone = bx.clone();
            int k = bx.length;
            intW info = new intW(0);
            lapack.dppsv("U", k, 1, a, bxClone, k, info);

            if (info.val != 0) {
                DenseMatrix dm = toMatrix(a, k);
                DenseMatrix inv = inverse(dm.transpose().multiplies(dm));
                DenseMatrix bm = dm.transpose().multiplies(new DenseMatrix(new double[][]{bx}).transpose());
                bm = inv.multiplies(bm);
                for (int i = 0; i < k; i++) {
                    bxClone[i] = bm.get(i, 0);
                }
            }
            return bxClone;
        }

        private DenseMatrix toMatrix(double[] a, int k) {
            DenseMatrix dm = new DenseMatrix(k, k);
            int rowId = 0;
            int colId = 0;

            int collen = k;
            for (double anA : a) {
                dm.set(rowId, colId, anA);
                dm.set(colId, rowId, anA);
                colId++;
                if (colId == k) {
                    colId = k - collen + 1;
                    collen--;
                    rowId++;
                }
            }
            return dm;
        }
    }

    /**
     * weight stat.
     */
    private static class WeightStat implements Serializable {
        private int k;
        private long count;
        private int triK;
        private double wSum;
        private double wwSum;
        private double bSum;
        private double bbSum;
        private double[] aSum;
        private double[] abSum;
        private double[] aaSum;
        private BLAS blas;

        WeightStat(int k) {
            this.k = k;
            this.triK = k * (k + 1) / 2;
            this.count = 0;
            this.wSum = 0.0;
            this.wwSum = 0.0;
            this.bSum = 0.0;
            this.bbSum = 0.0;
            this.aSum = new double[k];
            this.abSum = new double[k];
            this.aaSum = new double[triK];
            this.blas = F2jBLAS.getInstance();
        }

        public static WeightStat merge(WeightStat left, WeightStat right) {
            WeightStat out = new WeightStat(left.k);
            out.merge(left);
            out.merge(right);
            return out;
        }

        public void add(double[] features, double label, double weight) {
            this.count += 1;
            this.wSum += weight;
            this.wwSum += weight * weight;
            this.bSum += weight * label;
            this.bbSum += weight * label * label;
            axpy(weight, features, aSum);
            axpy(weight * label, features, abSum);
            spr(weight, features, aaSum);
        }

        public void merge(WeightStat other) {
            count += other.count;
            wSum += other.wSum;
            wwSum += other.wwSum;
            bSum += other.bSum;
            bbSum += other.bbSum;
            axpy(1.0, other.aSum, aSum);
            axpy(1.0, other.abSum, abSum);
            axpy(1.0, other.aaSum, aaSum);

        }

        double[] aMean() {
            double[] output = aSum.clone();
            scal(1.0 / wSum, output);
            return output;
        }

        double bMean() {
            return bSum / wSum;
        }

        double bStdDeviation() {
            double variance = Math.max(bbSum / wSum - bMean() * bMean(), 0.0);
            return Math.sqrt(variance);
        }

        double[] abMean() {
            double[] output = abSum.clone();
            scal(1.0 / wSum, output);
            return output;
        }

        double[] aaMean() {
            double[] output = aaSum.clone();
            scal(1.0 / wSum, output);
            return output;
        }

        double[] aStdDeviation() {
            double[] variance = aVariance();
            for (int i = 0; i < variance.length; i++) {
                variance[i] = Math.sqrt(variance[i]);
            }
            return variance;
        }

        double[] aVariance() {
            double[] variance = new double[k];
            int i = 0;
            int j = 2;
            while (i < triK) {
                int l = j - 2;
                double aw = aSum[l] / wSum;
                variance[l] = Math.max(aaSum[i] / wSum - aw * aw, 0.0);
                i += j;
                j += 1;
            }
            return variance;
        }

        // y += a
        private void axpy(double a, double[] x, double[] y) {
            this.blas.daxpy(x.length, a, x, 1, y, 1);
        }

        //a*a upper triangular part of matrix
        private void spr(double alpha, double[] v, double[] U) {
            this.blas.dspr("U", v.length, alpha, v, 1, U);
        }

        //a * x
        private void scal(double a, double[] x) {
            this.blas.dscal(x.length, a, x, 1);
        }
    }

    /**
     * pre proc, return features, label, weight, offset.
     */
    private static class PreProcMapFunc implements MapFunction<Row, Row> {
        private int[] featureColIdxs;
        private int labelColIdx;
        private int weightColIdx;
        private int offsetColIdx;

        PreProcMapFunc(int featureColIdxs[], int labelColIdx, int weightColIdx, int offsetColIdx) {
            this.featureColIdxs = featureColIdxs;
            this.labelColIdx = labelColIdx;
            this.weightColIdx = weightColIdx;
            this.offsetColIdx = offsetColIdx;
        }

        @Override
        public Row map(Row row) throws Exception {
            return preProcRow(row, featureColIdxs, labelColIdx, weightColIdx, offsetColIdx);
        }
    }

    /**
     * glm model to wls model.
     */
    public static class GlmModelToWlsModel implements MapPartitionFunction<Row, WeightedLeastSquaresModel> {
        @Override
        public void mapPartition(Iterable<Row> iterable, Collector<WeightedLeastSquaresModel> collector) {
            List<Row> rows = new ArrayList<>();
            for (Row row : iterable) {
                rows.add(row);
            }
            GlmModelData modelData = new GlmModelDataConverter().load(rows);

            WeightedLeastSquaresModel wlsModel = new WeightedLeastSquaresModel();
            wlsModel.coefficients = modelData.coefficients;
            wlsModel.diagInvAtWA = modelData.diagInvAtWA;
            wlsModel.fitIntercept = modelData.fitIntercept;
            wlsModel.intercept = modelData.intercept;
            wlsModel.numInstances = 0;

            collector.collect(wlsModel);
        }
    }

}
