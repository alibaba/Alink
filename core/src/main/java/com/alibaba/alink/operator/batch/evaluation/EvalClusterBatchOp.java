package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.evaluation.ClusterMetrics;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.LongMatrix;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.ClusterMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.ClusterEvaluationUtil;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.evaluation.EvalClusterParams;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Calculate the cluster evaluation metrics for clustering.
 * <p>
 * PredictionCol is required for evaluation. LabelCol is optional, if given, NMI/Purity/RI/ARI will be calcuated.
 * VectorCol is also optional, if given, SilhouetteCoefficient/SSB/SSW/Compactness/SP/DB
 * /VRC will be calculated. If only predictionCol is given, only K/ClusterArray/CountArray will be
 * calculated.
 */
public final class EvalClusterBatchOp extends BatchOperator<EvalClusterBatchOp>
    implements EvalClusterParams<EvalClusterBatchOp>, EvaluationMetricsCollector<ClusterMetrics, EvalClusterBatchOp> {
    public static final String SILHOUETTE_COEFFICIENT = "silhouetteCoefficient";
    private static final String METRICS_SUMMARY = "metricsSummary";
    private static final String EVAL_RESULT = "cluster_eval_result";
    private static final String LABELS = "labels";
    private static final String PREDICTIONS = "predictions";

    public EvalClusterBatchOp() {
        super(null);
    }

    public EvalClusterBatchOp(Params params) {
        super(params);
    }

    @Override
    public ClusterMetrics createMetrics(List<Row> rows){
        return new ClusterMetrics(rows.get(0));
    }

    @Override
    public EvalClusterBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator in = checkAndGetFirst(inputs);
        String labelColName = this.getLabelCol();
        String predResultColName = this.getPredictionCol();
        String vectorColName = this.getVectorCol();
        DistanceType distanceType = getDistanceType();
        FastDistance distance = distanceType.getFastDistance();

        DataSet<Params> empty = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromElements(
            new Params());
        DataSet<Params> labelMetrics = empty, vectorMetrics;

        if (null != labelColName) {
            DataSet<Row> data = in.select(new String[] {labelColName, predResultColName}).getDataSet();
            labelMetrics = calLocalPredResult(data)
                .reduce(new ReduceFunction<LongMatrix>() {
                    @Override
                    public LongMatrix reduce(LongMatrix value1, LongMatrix value2) {
                        value1.plusEqual(value2);
                        return value1;
                    }
                })
                .map(new MapFunction<LongMatrix, Params>() {
                    @Override
                    public Params map(LongMatrix value) {
                        return ClusterEvaluationUtil.extractParamsFromConfusionMatrix(value);
                    }
                });
        }
        if (null != vectorColName) {
            DataSet<Row> data = in.select(new String[] {predResultColName, vectorColName}).getDataSet();
            DataSet<BaseMetricsSummary> metricsSummary = data
                .groupBy(0)
                .reduceGroup(new CalcClusterMetricsSummary(distance))
                .reduce(new EvaluationUtil.ReduceBaseMetrics());
            DataSet<Tuple1<Double>> silhouetteCoefficient = data.map(
                new RichMapFunction<Row, Tuple1<Double>>() {
                    @Override
                    public Tuple1<Double> map(Row value) {
                        List<BaseMetricsSummary> list = getRuntimeContext().getBroadcastVariable(METRICS_SUMMARY);
                        return ClusterEvaluationUtil.calSilhouetteCoefficient(value,
                            (ClusterMetricsSummary)list.get(0));
                    }
                }).withBroadcastSet(metricsSummary, METRICS_SUMMARY)
                .aggregate(Aggregations.SUM, 0);

            vectorMetrics = metricsSummary.map(new ClusterEvaluationUtil.SaveDataAsParams()).withBroadcastSet(
                silhouetteCoefficient, SILHOUETTE_COEFFICIENT);
        } else {
            vectorMetrics = in.select(predResultColName)
                .getDataSet()
                .reduceGroup(new BasicClusterParams());
        }

        DataSet<Row> out = labelMetrics
            .union(vectorMetrics)
            .reduceGroup(new GroupReduceFunction<Params, Row>() {
                @Override
                public void reduce(Iterable<Params> values, Collector<Row> out) {
                    Params params = new Params();
                    for (Params p : values) {
                        params.merge(p);
                    }
                    out.collect(Row.of(params.toJson()));
                }
            });

        this.setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(),
            out, new TableSchema(new String[] {EVAL_RESULT}, new TypeInformation[] {Types.STRING})
        ));
        return this;
    }

    public static class CalcClusterMetricsSummary implements GroupReduceFunction<Row, BaseMetricsSummary> {
        private FastDistance distance;

        public CalcClusterMetricsSummary(FastDistance distance) {
            this.distance = distance;
        }

        @Override
        public void reduce(Iterable<Row> rows, Collector<BaseMetricsSummary> collector) {
            collector.collect(ClusterEvaluationUtil.getClusterStatistics(rows, distance));
        }
    }

    public static class BasicClusterParams implements GroupReduceFunction<Row, Params> {
        @Override
        public void reduce(Iterable<Row> rows, Collector<Params> collector) {
            collector.collect(ClusterEvaluationUtil.getBasicClusterStatistics(rows));
        }
    }

    private static DataSet<LongMatrix> calLocalPredResult(DataSet<Row> data) {

        DataSet<Tuple1<Map<Object, Integer>>> labels = data.flatMap(new FlatMapFunction<Row, Object>() {
            @Override
            public void flatMap(Row row, Collector<Object> collector) {
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    collector.collect(row.getField(0));
                }
            }
        }).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(false, null, null)).project(0);

        DataSet<Tuple1<Map<Object, Integer>>> predictions = data.flatMap(new FlatMapFunction<Row, Object>() {
            @Override
            public void flatMap(Row row, Collector<Object> collector) {
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    collector.collect(row.getField(1));
                }
            }
        }).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(false, null, null)).project(0);

        // Build the confusion matrix.
        DataSet<LongMatrix> statistics = data
            .rebalance()
            .mapPartition(new CalLocalPredResult())
            .withBroadcastSet(labels, LABELS)
            .withBroadcastSet(predictions, PREDICTIONS);

        return statistics;
    }

    static class CalLocalPredResult extends RichMapPartitionFunction<Row, LongMatrix> {
        private Map<Object, Integer> labels, predictions;

        @Override
        public void open(Configuration parameters) throws Exception {
            List<Tuple1<Map<Object, Integer>>> list = getRuntimeContext().getBroadcastVariable(LABELS);
            this.labels = list.get(0).f0;
            list = getRuntimeContext().getBroadcastVariable(PREDICTIONS);
            this.predictions = list.get(0).f0;
        }

        @Override
        public void mapPartition(Iterable<Row> rows, Collector<LongMatrix> collector) {
            long[][] matrix = new long[predictions.size()][labels.size()];
            for (Row r : rows) {
                if (EvaluationUtil.checkRowFieldNotNull(r)) {
                    int label = labels.get(r.getField(0));
                    int pred = predictions.get(r.getField(1));
                    matrix[pred][label] += 1;
                }
            }
            collector.collect(new LongMatrix(matrix));
        }
    }
}
