package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.evaluation.BinaryEvaluationStreamParams;
import com.alibaba.alink.params.evaluation.MultiEvaluationStreamParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.buildLabelIndexLabelArray;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getDetailStatistics;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getMultiClassMetrics;

/**
 * Base class for EvalBinaryClassStreamOp and EvalMultiClassStreamOp. Calculate the evaluation data within time windows
 * for binary classifiction and multi classification. You can either give label column and predResult column or give
 * label column and predDetail column. Once predDetail column is given, the predResult column is ignored.
 */
public class BaseEvalClassStreamOp<T extends BaseEvalClassStreamOp<T>> extends StreamOperator<T> {
    private static final String DATA_OUTPUT = "Data";
    private boolean binary;

    public BaseEvalClassStreamOp(Params params, boolean binary) {
        super(params);
        this.binary = binary;
    }

    @Override
    public T linkFrom(StreamOperator<?>... inputs) {
        StreamOperator<?> in = checkAndGetFirst(inputs);
        String labelColName = this.get(MultiEvaluationStreamParams.LABEL_COL);
        String positiveValue = this.get(BinaryEvaluationStreamParams.POS_LABEL_VAL_STR);
        Integer timeInterval = this.get(MultiEvaluationStreamParams.TIME_INTERVAL);

        ClassificationEvaluationUtil.Type type = ClassificationEvaluationUtil.judgeEvaluationType(this.getParams());

        DataStream<BaseMetricsSummary> statistics;

        switch (type) {
            case PRED_RESULT: {
                String predResultColName = this.get(MultiEvaluationStreamParams.PREDICTION_COL);
                Preconditions.checkArgument(
                    TableUtil.findColIndex(in.getColNames(), labelColName) >= 0 || TableUtil.findColIndex(
                        in.getColNames(), predResultColName) >= 0, "Can not find given column names!");

                LabelPredictionWindow predMultiWindowFunction = new LabelPredictionWindow(binary, positiveValue);
                statistics = in.select(new String[] {labelColName, predResultColName})
                    .getDataStream()
                    .timeWindowAll(Time.of(timeInterval, TimeUnit.SECONDS))
                    .apply(predMultiWindowFunction);
                break;
            }
            case PRED_DETAIL: {
                String predDetailColName = this.get(MultiEvaluationStreamParams.PREDICTION_DETAIL_COL);
                Preconditions.checkArgument(
                    TableUtil.findColIndex(in.getColNames(), labelColName) >= 0 && TableUtil.findColIndex(
                        in.getColNames(), predDetailColName) >= 0, "Can not find given column names!");

                PredDetailLabel eval = new PredDetailLabel(positiveValue, binary);

                statistics = in.select(new String[] {labelColName, predDetailColName})
                    .getDataStream()
                    .timeWindowAll(Time.of(timeInterval, TimeUnit.SECONDS))
                    .apply(eval);
                break;
            }
            default: {
                throw new RuntimeException("Error Input");
            }
        }
        DataStream<BaseMetricsSummary> totalStatistics = statistics
            .map(new EvaluationUtil.AllDataMerge())
            .setParallelism(1);

        DataStream<Row> windowOutput = statistics.map(
            new EvaluationUtil.SaveDataStream(ClassificationEvaluationUtil.WINDOW.f0));
        DataStream<Row> allOutput = totalStatistics.map(
            new EvaluationUtil.SaveDataStream(ClassificationEvaluationUtil.ALL.f0));

        DataStream<Row> union = windowOutput.union(allOutput);

        this.setOutput(union,
            new String[] {ClassificationEvaluationUtil.STATISTICS_OUTPUT, DATA_OUTPUT},
            new TypeInformation[] {Types.STRING, Types.STRING});

        return (T)this;
    }

    static class LabelPredictionWindow implements AllWindowFunction<Row, BaseMetricsSummary, TimeWindow> {
        private boolean binary;
        private String positiveValue;

        LabelPredictionWindow(boolean binary, String positiveValue) {
            this.binary = binary;
            this.positiveValue = positiveValue;
        }

        @Override
        public void apply(TimeWindow timeWindow, Iterable<Row> rows, Collector<BaseMetricsSummary> collector)
            throws Exception {
            HashSet<String> labels = new HashSet<>();
            for (Row row : rows) {
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    labels.add(row.getField(0).toString());
                    labels.add(row.getField(1).toString());
                }
            }
            if (labels.size() > 0) {
                collector.collect(getMultiClassMetrics(rows, buildLabelIndexLabelArray(labels, binary, positiveValue)));
            }
        }
    }

    static class PredDetailLabel implements AllWindowFunction<Row, BaseMetricsSummary, TimeWindow> {
        private String positiveValue;
        private Boolean binary;

        PredDetailLabel(String positiveValue, boolean binary) {
            this.positiveValue = positiveValue;
            this.binary = binary;
        }

        @Override
        public void apply(TimeWindow timeWindow, Iterable<Row> rows, Collector<BaseMetricsSummary> collector)
            throws Exception {
            HashSet<String> labels = new HashSet<>();
            for (Row row : rows) {
                if (EvaluationUtil.checkRowFieldNotNull(row)) {
                    labels.addAll(EvaluationUtil.extractLabelProbMap(row).keySet());
                    labels.add(row.getField(0).toString());
                }
            }
            if (labels.size() > 0) {
                collector.collect(
                    getDetailStatistics(rows, binary, buildLabelIndexLabelArray(labels, binary, positiveValue)));
            }
        }
    }

}
