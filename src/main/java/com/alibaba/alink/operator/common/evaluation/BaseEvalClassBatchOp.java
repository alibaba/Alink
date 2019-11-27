package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.evaluation.BinaryEvaluationParams;
import com.alibaba.alink.params.evaluation.MultiEvaluationParams;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getDetailStatistics;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getMultiClassMetrics;

/**
 * Base class for EvalBinaryClassBatchOp and EvalMultiClassBatchOp. The variable "binary" controls whether it's binary
 * classification evaluation or multi classification evaluation.
 * <p>
 * Calculate the evaluation metrics for binary classifiction and multi classification.
 * <p>
 * You can either give label column and predResult column or give label column and predDetail column. Once predDetail
 * column is given, the predResult column is ignored.
 */
public class BaseEvalClassBatchOp<T extends BaseEvalClassBatchOp<T>> extends BatchOperator<T> {
    private static final String LABELS = "labels";
    private static final String DATA_OUTPUT = "Data";
    private boolean binary;

    public BaseEvalClassBatchOp(Params params, boolean binary) {
        super(params);
        this.binary = binary;
    }

    @Override
    public T linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String labelColName = this.get(MultiEvaluationParams.LABEL_COL);
        String positiveValue = this.get(BinaryEvaluationParams.POS_LABEL_VAL_STR);

        // Judge the evaluation type from params.
        ClassificationEvaluationUtil.Type type = ClassificationEvaluationUtil.judgeEvaluationType(this.getParams());

        DataSet<BaseMetricsSummary> res;
        switch (type) {
            case PRED_RESULT: {
                String predResultColName = this.get(MultiEvaluationParams.PREDICTION_COL);
                Preconditions.checkArgument(
                    TableUtil.findColIndex(in.getColNames(), labelColName) >= 0 || TableUtil.findColIndex(
                        in.getColNames(), predResultColName) >= 0, "Can not find given column names!");

                DataSet<Row> data = in.select(new String[] {labelColName, predResultColName}).getDataSet();
                res = calLabelPredictionLocal(data, positiveValue, binary);
                break;
            }
            case PRED_DETAIL: {
                String predDetailColName = this.get(MultiEvaluationParams.PREDICTION_DETAIL_COL);
                Preconditions.checkArgument(
                    TableUtil.findColIndex(in.getColNames(), labelColName) >= 0 && TableUtil.findColIndex(
                        in.getColNames(), predDetailColName) >= 0, "Can not find given column names!");

                DataSet<Row> data = in.select(new String[] {labelColName, predDetailColName}).getDataSet();
                res = calLabelPredDetailLocal(data, positiveValue, binary);
                break;
            }
            default: {
                throw new RuntimeException("Error Input");
            }
        }

        DataSet<BaseMetricsSummary> metrics = res
            .reduce(new EvaluationUtil.ReduceBaseMetrics());

        this.setOutput(metrics.flatMap(new EvaluationUtil.SaveDataAsParams()),
            new String[] {DATA_OUTPUT}, new TypeInformation[] {Types.STRING});

        return (T)this;
    }

    /**
     * Calculate the evaluation metrics of every partition in case of inputs are prediction result.
     */
    private static DataSet<BaseMetricsSummary> calLabelPredictionLocal(DataSet<Row> data, final String positiveValue,
                                                                       boolean binary) {

        DataSet<Tuple2<Map<String, Integer>, String[]>> labels = data.flatMap(new FlatMapFunction<Row, String>() {
            @Override
            public void flatMap(Row row, Collector<String> collector) {
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    collector.collect(row.getField(0).toString());
                    collector.collect(row.getField(1).toString());
                }
            }
        }).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(binary, positiveValue));

        // Build the confusion matrix.
        return data
            .rebalance()
            .mapPartition(new CalLabelPredictionLocal())
            .withBroadcastSet(labels, LABELS);
    }

    /**
     * Calculate the evaluation metrics of every partition in case of inputs are label and prediction detail.
     */
    private static DataSet<BaseMetricsSummary> calLabelPredDetailLocal(DataSet<Row> data, final String positiveValue,
                                                                       boolean binary) {
        DataSet<Tuple2<Map<String, Integer>, String[]>> labels = data.flatMap(new FlatMapFunction<Row, String>() {
            @Override
            public void flatMap(Row row, Collector<String> collector) {
                TreeMap<String, Double> labelProbMap;
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    labelProbMap = EvaluationUtil.extractLabelProbMap(row);
                    labelProbMap.keySet().forEach(collector::collect);
                    collector.collect(row.getField(0).toString());
                }
            }
        }).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(binary, positiveValue));

        return data
            .rebalance()
            .mapPartition(new CalLabelDetailLocal(binary))
            .withBroadcastSet(labels, LABELS);
    }

    /**
     * Calculate the confusion matrix based on the label and predResult.
     */
    static class CalLabelPredictionLocal extends RichMapPartitionFunction<Row, BaseMetricsSummary> {
        private Tuple2<Map<String, Integer>, String[]> map;

        @Override
        public void open(Configuration parameters) throws Exception {
            List<Tuple2<Map<String, Integer>, String[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
            Preconditions.checkArgument(list.size() > 0,
                "Please check the evaluation input! there is no effective row!");
            this.map = list.get(0);
        }

        @Override
        public void mapPartition(Iterable<Row> rows, Collector<BaseMetricsSummary> collector) {
            collector.collect(getMultiClassMetrics(rows, map));
        }
    }

    /**
     * Calculate the confusion matrix based on the label and predResult.
     */
    static class CalLabelDetailLocal extends RichMapPartitionFunction<Row, BaseMetricsSummary> {
        private Tuple2<Map<String, Integer>, String[]> map;
        private boolean binary;

        public CalLabelDetailLocal(boolean binary) {
            this.binary = binary;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            List<Tuple2<Map<String, Integer>, String[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
            Preconditions.checkArgument(list.size() > 0,
                "Please check the evaluation input! there is no effective row!");
            this.map = list.get(0);
        }

        @Override
        public void mapPartition(Iterable<Row> rows, Collector<BaseMetricsSummary> collector) {
            collector.collect(getDetailStatistics(rows, binary, map));
        }
    }
}
