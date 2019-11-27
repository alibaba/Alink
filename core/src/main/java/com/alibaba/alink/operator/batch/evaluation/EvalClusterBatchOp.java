package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.distance.*;
import com.alibaba.alink.operator.common.evaluation.*;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.evaluation.EvalClusterParams;
import com.google.common.base.Preconditions;
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
 * VectorCol is also optional, if given, SilhouetteCoefficient/SSB/SSW/Compactness/SEPERATION/DAVIES_BOULDIN
 * /CALINSKI_HARABAZ will be calculated. If only predictionCol is given, only K/ClusterArray/CountArray will be
 * calculated.
 */
public final class EvalClusterBatchOp extends BatchOperator<EvalClusterBatchOp>
    implements EvalClusterParams<EvalClusterBatchOp>, EvaluationMetricsCollector<ClusterMetrics> {
    public static final String SILHOUETTE_COEFFICIENT = "silhouetteCoefficient";

    public EvalClusterBatchOp() {
        super(null);
    }

    public EvalClusterBatchOp(Params params) {
        super(params);
    }

    @Override
    public ClusterMetrics collectMetrics() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please provide the dataset to evaluate!");
        return new ClusterMetrics(this.collect().get(0));
    }

    @Override
    public EvalClusterBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator in = checkAndGetFirst(inputs);
        String labelColName = this.getLabelCol();
        String predResultColName = this.getPredictionCol();
        String vectorColName = this.getVectorCol();
        DistanceType distanceType = DistanceType.valueOf(this.getDistanceType().toUpperCase());
        Preconditions.checkArgument(distanceType == DistanceType.COSINE || distanceType == DistanceType.EUCLIDEAN,
            "Not support " + distanceType.name());
        ContinuousDistance distance = distanceType.getContinuousDistance();

        DataSet<Params> empty = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromElements(
            new Params());
        DataSet<Params> labelMetrics = empty, vectorMetrics;

        if (null != labelColName) {
            DataSet<Row> data = in.select(new String[] {labelColName, predResultColName}).getDataSet();
            labelMetrics = calLocalPredResult(data)
                .reduce(new ReduceFunction<long[][]>() {
                    @Override
                    public long[][] reduce(long[][] value1, long[][] value2) {
                        for (int i = 0; i < value1.length; i++) {
                            for (int j = 0; j < value1[0].length; j++) {
                                value1[i][j] += value2[i][j];
                            }
                        }
                        return value1;
                    }
                })
                .map(new MapFunction<long[][], Params>() {
                    @Override
                    public Params map(long[][] value) {
                        return ClusterEvaluationUtil.MatrixToParams(value);
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
                        List<BaseMetricsSummary> list = getRuntimeContext().getBroadcastVariable("metricsSummary");
                        return ClusterEvaluationUtil.calSilhouetteCoefficient(value,
                            (ClusterMetricsSummary)list.get(0));
                    }
                }).withBroadcastSet(metricsSummary, "metricsSummary")
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
            out, new TableSchema(new String[] {"cluster_eval_result"}, new TypeInformation[] {Types.STRING})
        ));
        return this;
    }

    public static class CalcClusterMetricsSummary implements GroupReduceFunction<Row, BaseMetricsSummary> {
        private ContinuousDistance distance;

        public CalcClusterMetricsSummary(ContinuousDistance distance) {
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

    private static DataSet<long[][]> calLocalPredResult(DataSet<Row> data) {

        DataSet<Tuple1<Map<String, Integer>>> labels = data.flatMap(new FlatMapFunction<Row, String>() {
            @Override
            public void flatMap(Row row, Collector<String> collector) {
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    collector.collect(row.getField(0).toString());
                }
            }
        }).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(false, null)).project(0);

        DataSet<Tuple1<Map<String, Integer>>> predictions = data.flatMap(new FlatMapFunction<Row, String>() {
            @Override
            public void flatMap(Row row, Collector<String> collector) {
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    collector.collect(row.getField(1).toString());
                }
            }
        }).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(false, null)).project(0);

        // Build the confusion matrix.
        DataSet<long[][]> statistics = data
            .rebalance()
            .mapPartition(new CalLocalPredResult())
            .withBroadcastSet(labels, "labels")
            .withBroadcastSet(predictions, "predictions");

        return statistics;
    }

    static class CalLocalPredResult extends RichMapPartitionFunction<Row, long[][]> {
        private Map<String, Integer> labels, predictions;

        @Override
        public void open(Configuration parameters) throws Exception {
            List<Tuple1<Map<String, Integer>>> list = getRuntimeContext().getBroadcastVariable("labels");
            this.labels = list.get(0).f0;
            list = getRuntimeContext().getBroadcastVariable("predictions");
            this.predictions = list.get(0).f0;
        }

        @Override
        public void mapPartition(Iterable<Row> rows, Collector<long[][]> collector) {
            long[][] matrix = new long[predictions.size()][labels.size()];
            for (Row r : rows) {
                if (EvaluationUtil.checkRowFieldNotNull(r)) {
                    int label = labels.get(r.getField(0).toString());
                    int pred = predictions.get(r.getField(1).toString());
                    matrix[pred][label] += 1;
                }
            }
            collector.collect(matrix);
        }
    }
}
