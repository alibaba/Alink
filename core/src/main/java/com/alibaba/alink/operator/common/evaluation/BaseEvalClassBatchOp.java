package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;
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
public class BaseEvalClassBatchOp<T extends BaseEvalClassBatchOp<T>> extends BatchOperator<T>{
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
        String labelColName = this.get(EvalMultiClassParams.LABEL_COL);
        TypeInformation labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelColName);
        String positiveValue = this.get(EvalBinaryClassParams.POS_LABEL_VAL_STR);

        if(binary){
            Preconditions.checkArgument(getParams().contains(EvalBinaryClassParams.PREDICTION_DETAIL_COL),
                "Binary Evaluation must give predictionDetailCol!");
        }

        // Judge the evaluation type from params.
        ClassificationEvaluationUtil.Type type = ClassificationEvaluationUtil.judgeEvaluationType(this.getParams());
        DataSet<BaseMetricsSummary> res;
        switch (type) {
            case PRED_RESULT: {
                String predResultColName = this.get(EvalMultiClassParams.PREDICTION_COL);
                TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predResultColName);

                DataSet<Row> data = in.select(new String[] {labelColName, predResultColName}).getDataSet();
                res = calLabelPredictionLocal(data, positiveValue, binary, labelType);
                break;
            }
            case PRED_DETAIL: {
                String predDetailColName = this.get(EvalMultiClassParams.PREDICTION_DETAIL_COL);
                TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

                DataSet<Row> data = in.select(new String[] {labelColName, predDetailColName}).getDataSet();
                res = calLabelPredDetailLocal(data, positiveValue, binary, labelType);
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
    private static DataSet<BaseMetricsSummary> calLabelPredictionLocal(DataSet<Row> data,
                                                                       final String positiveValue,
                                                                       boolean binary,
                                                                       TypeInformation labelType) {

        DataSet<Tuple2<Map<Object, Integer>, Object[]>> labels = data.flatMap(new FlatMapFunction<Row, Object>() {
            @Override
            public void flatMap(Row row, Collector<Object> collector) {
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    collector.collect(row.getField(0));
                    collector.collect(row.getField(1));
                }
            }
        }).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(binary, positiveValue, labelType));

        // Build the confusion matrix.
        return data
            .rebalance()
            .mapPartition(new CalLabelPredictionLocal())
            .withBroadcastSet(labels, LABELS);
    }

    /**
     * Calculate the evaluation metrics of every partition in case of inputs are label and prediction detail.
     */
    private static DataSet<BaseMetricsSummary> calLabelPredDetailLocal(DataSet<Row> data,
                                                                       final String positiveValue,
                                                                       boolean binary,
                                                                       TypeInformation labelType) {
        DataSet<Tuple2<Map<Object, Integer>, Object[]>> labels = data.flatMap(new FlatMapFunction<Row, Object>() {
            @Override
            public void flatMap(Row row, Collector<Object> collector) {
                TreeMap<Object, Double> labelProbMap;
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    labelProbMap = EvaluationUtil.extractLabelProbMap(row, labelType);
                    labelProbMap.keySet().forEach(collector::collect);
                    collector.collect(row.getField(0));
                }
            }
        }).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(binary, positiveValue, labelType));

        return data
            .rebalance()
            .mapPartition(new CalLabelDetailLocal(binary, labelType))
            .withBroadcastSet(labels, LABELS);
    }

    /**
     * Calculate the confusion matrix based on the label and predResult.
     */
    static class CalLabelPredictionLocal extends RichMapPartitionFunction<Row, BaseMetricsSummary> {
        private Tuple2<Map<Object, Integer>, Object[]> map;

        @Override
        public void open(Configuration parameters) throws Exception {
            List<Tuple2<Map<Object, Integer>, Object[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
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
        private Tuple2<Map<Object, Integer>, Object[]> map;
        private boolean binary;
        private TypeInformation labelType;

        public CalLabelDetailLocal(boolean binary, TypeInformation labelType) {
            this.binary = binary;
            this.labelType = labelType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            List<Tuple2<Map<Object, Integer>, Object[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
            Preconditions.checkArgument(list.size() > 0,
                "Please check the evaluation input! there is no effective row!");
            this.map = list.get(0);
        }

        @Override
        public void mapPartition(Iterable<Row> rows, Collector<BaseMetricsSummary> collector) {
            collector.collect(getDetailStatistics(rows, binary, map, labelType));
        }
    }
}
