package com.alibaba.alink.operator.batch.regression;

import java.util.Arrays;

import com.alibaba.alink.common.linalg.VectorIterator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.FeatureLabelUtil;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.regression.AftRegTrainParams;

import com.alibaba.alink.params.shared.linear.LinearTrainParams;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
public class AftSurvivalRegTrainBatchOp extends BatchOperator<AftSurvivalRegTrainBatchOp>
        implements AftRegTrainParams<AftSurvivalRegTrainBatchOp> {

    /**
     * Constructor.
     */
    public AftSurvivalRegTrainBatchOp() {
        this(null);
    }

    /**
     * Constructor.
     * @param params the params of the algorithm.
     */
    public AftSurvivalRegTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public AftSurvivalRegTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String labelName = this.getLabelCol();
        String censorColName = this.getCensorCol();
        String vectorColName = this.getVectorCol();
        String[] featureColNames = this.getFeatureCols();
        boolean hasInterceptItem = this.getWithIntercept();
        DataSet<double[]> std = getStd(in, featureColNames, vectorColName);
        TypeInformation labelType = Types.DOUBLE;
        DataSet<Object> labelValues = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromElements(new Object());

        String[] keepColNames = new String[]{censorColName, labelName};
        Tuple2<DataSet<Tuple2<Vector, Row>>, DataSet<BaseVectorSummary>> dataSrt
                = StatisticsHelper.summaryHelper(in, featureColNames, vectorColName, keepColNames);

        DataSet<BaseVectorSummary> srt = dataSrt.f1;
        DataSet<Integer> vectorSize = srt.map(new MapFunction<BaseVectorSummary, Integer>() {
            @Override
            public Integer map(BaseVectorSummary value) {
                return value.vectorSize();
            }
        });
        //censor/label, log(time), feature/std.
        DataSet<Tuple3<Double, Double, Vector>> trainData = dataSrt.f0.rebalance()
                .mapPartition(new FormatLabeledVector(hasInterceptItem))
                .withBroadcastSet(std, "std");
        DataSet<Params> meta = labelValues
                .mapPartition(new BaseLinearModelTrainBatchOp.CreateMeta("AFTSurvivalRegTrainBatchOp",
                        LinearModelType.AFT, true, getParams()))
                .setParallelism(1);

        DataSet<Tuple2<DenseVector, double[]>>
                coefVectorSet = BaseLinearModelTrainBatchOp.optimize(this.getParams(), vectorSize,
                trainData, LinearModelType.AFT, MLEnvironmentFactory.get(getMLEnvironmentId()));

        DataSet<Tuple2<DenseVector, double[]>> coef = coefVectorSet
                .map(new FormatCoef(hasInterceptItem))
                .withBroadcastSet(std, "std");

        DataSet<Row> modelRows = coef
                .mapPartition(new BaseLinearModelTrainBatchOp.BuildModelFromCoefs(labelType, featureColNames, false, false, null))
                .withBroadcastSet(meta, "meta")
                .setParallelism(1);

        this.setOutput(modelRows, new LinearModelDataConverter(labelType).getModelSchema());
        return this;
    }

    /**
     * Extract the label, weight and vector from input row, and standardize the train data.
     */
    public static class FormatLabeledVector extends AbstractRichFunction
            implements MapPartitionFunction<Tuple2<Vector, Row>, Tuple3<Double, Double, Vector>> {
        private boolean hasInterceptItem;
        private double[] std;

        FormatLabeledVector(boolean hasInterceptItem) {
            this.hasInterceptItem = hasInterceptItem;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.std = (double[]) (getRuntimeContext().getBroadcastVariable("std").get(0));
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Vector, Row>> rows, Collector<Tuple3<Double, Double, Vector>> out)
                throws Exception {
            for (Tuple2<Vector, Row> row : rows) {
                double weight = Double.parseDouble(row.f1.getField(0).toString());
                Vector tmpVector = row.f0;
                //this is the current time, and it cannot be negative.
                double val = FeatureLabelUtil.getLabelValue(row.f1, true, 1, null);
                if (val <= 0) {
                    throw new IllegalArgumentException("Survival Time must be greater than 0!");
                }
                //weight judges whether happened or not.
                if (weight != 0.0 && weight != 1.0) {
                    throw new IllegalArgumentException("Censor must be 1.0 or 0.0!");
                }
                Vector aVector;
                if (tmpVector instanceof SparseVector) {
                    if (hasInterceptItem) {
                        aVector = svStd(tmpVector, std, 1);
                    } else {
                        aVector = svStd(tmpVector, std, 0);
                    }
                } else {
                    aVector = dvStd(tmpVector, std, hasInterceptItem);
                }
                out.collect(new Tuple3<>(weight, Math.log(val), aVector));
            }
        }
    }

    private static Vector svStd(Vector tmpVector, double[] std, int pos) {
        int vectorLength = ((SparseVector) tmpVector).getIndices().length + pos;
        int[] newIndices = new int[vectorLength];
        double[] newValues = new double[vectorLength];
        VectorIterator svIterator = tmpVector.iterator();
        int initialPos = pos;
        if (pos == 1) {
            newIndices[0] = 0;
            newValues[0] = 1.0;
        }
        while (svIterator.hasNext()) {
            if (std[svIterator.getIndex()] > 0) {
                newIndices[pos] = svIterator.getIndex() + initialPos;
                newValues[pos++] = svIterator.getValue() / std[svIterator.getIndex()];
            }
            svIterator.next();
        }
        return new SparseVector(tmpVector.size(), Arrays.copyOf(newIndices, pos),
                Arrays.copyOf(newValues, pos));
    }

    private static Vector dvStd(Vector tmpVector, double[] std, boolean hasInterceptItem) {
        double[] aVectorData = ((DenseVector) tmpVector).getData();
        for (int i = 0; i < tmpVector.size(); ++i) {
            if (std[i] > 0) {
                aVectorData[i] = tmpVector.get(i) / std[i];
            } else {
                aVectorData[i] = 0.0;
            }
        }
        if (hasInterceptItem) {
            tmpVector = tmpVector.prefix(1.0);
        }
        return tmpVector;
    }

    /**
     * Regulate the coefficients from standardized data.
     */
    public static class FormatCoef extends AbstractRichFunction
            implements MapFunction<Tuple2<DenseVector, double[]>, Tuple2<DenseVector, double[]>> {
        private double[] std;
        private boolean hasInterceptItem;

        FormatCoef(boolean hasInterceptItem) {
            this.hasInterceptItem = hasInterceptItem;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.std = (double[]) (getRuntimeContext().getBroadcastVariable("std").get(0));
        }

        @Override
        public Tuple2<DenseVector, double[]> map(Tuple2<DenseVector, double[]> aVector) throws Exception {
            DenseVector vec = aVector.f0.clone();
            double[] vecData = vec.getData();
            double[] aVectorData = aVector.f0.getData();
            if (hasInterceptItem) {
                int size = aVectorData.length - 1;
                for (int i = 1; i < size; ++i) {
                    if (std[i - 1] > 0) {
                        vecData[i] = aVectorData[i] / std[i - 1];
                    } else {
                        vecData[i] = 0.0;
                    }
                }
                vecData[size] = Math.exp(aVectorData[size]);
            } else {
                int size = aVectorData.length - 1;
                for (int i = 0; i < size; ++i) {
                    if (std[i] > 0) {
                        vecData[i] = aVectorData[i] / std[i];
                    } else {
                        vecData[i] = 0.0;
                    }
                }
            }
            return Tuple2.of(vec, aVector.f1);
        }
    }

    private static DataSet<double[]> getStd(BatchOperator in, String[] featureColNames, String vectorColName) {
        return StatisticsHelper.summaryHelper(in, featureColNames, vectorColName).f1
                .flatMap(
                        new FlatMapFunction<BaseVectorSummary, double[]>() {
                            @Override
                            public void flatMap(BaseVectorSummary srt, Collector<double[]> out) {
                                double[] stdv;
                                if (srt.standardDeviation() instanceof DenseVector) {
                                    stdv = ((DenseVector) srt.standardDeviation()).getData();
                                } else {
                                    stdv = ((SparseVector) srt.standardDeviation()).toDenseVector().getData();
                                }
                                out.collect(stdv);
                            }
                        });
    }
}
