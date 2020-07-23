package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.EqualWidthDiscretizerModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.feature.QuantileDiscretizerTrainParams;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

import static com.alibaba.alink.operator.common.dataproc.SortUtils.OBJECT_COMPARATOR;

/**
 * EqualWidth discretizer keeps every interval the same width, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
public final class EqualWidthDiscretizerTrainBatchOp extends BatchOperator<EqualWidthDiscretizerTrainBatchOp>
    implements QuantileDiscretizerTrainParams <EqualWidthDiscretizerTrainBatchOp>,
    WithModelInfoBatchOp<EqualWidthDiscretizerModelInfoBatchOp.EqualWidthDiscretizerModelInfo, EqualWidthDiscretizerTrainBatchOp, EqualWidthDiscretizerModelInfoBatchOp> {

    private static double MIN_MAX_EPSILON = 1e-15;

    public EqualWidthDiscretizerTrainBatchOp() {
    }

    public EqualWidthDiscretizerTrainBatchOp(Params params) {
        super(params);
    }


    @Override
    public EqualWidthDiscretizerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        if (getParams().contains(QuantileDiscretizerTrainParams.NUM_BUCKETS) && getParams().contains(
            QuantileDiscretizerTrainParams.NUM_BUCKETS_ARRAY)) {
            throw new RuntimeException("It can not set num_buckets and num_buckets_array at the same time.");
        }

        String[] quantileColNames = getSelectedCols();
        HashMap<String, Long> quantileNum = new HashMap<>();

        if (getParams().contains(QuantileDiscretizerTrainParams.NUM_BUCKETS)) {
            for(String s : quantileColNames){
                quantileNum.put(s, getNumBuckets().longValue());
            }
        } else {
            for(int i = 0; i < quantileColNames.length; i++){
                quantileNum.put(quantileColNames[i], getNumBucketsArray()[i].longValue());
            }
        }

        DataSet<Row> bucket = StatisticsHelper.summary(in, quantileColNames)
            .flatMap(new BuildBucketsFromTableSummary(quantileNum, quantileColNames));
        bucket = bucket.reduceGroup(
            new QuantileDiscretizerTrainBatchOp.SerializeModel(
                getParams(),
                quantileColNames,
                TableUtil.findColTypesWithAssertAndHint(in.getSchema(), quantileColNames)
            )
        );

        /* set output */
        setOutput(bucket, new QuantileDiscretizerModelDataConverter().getModelSchema());
        return this;
    }

    static class BuildBucketsFromTableSummary implements FlatMapFunction<TableSummary, Row>{
        private HashMap<String, Long> colNameBucketNumber;
        private String[] colNames;

        public BuildBucketsFromTableSummary(HashMap<String, Long> colNameBucketNumber, String[] colNames){
            this.colNameBucketNumber = colNameBucketNumber;
            this.colNames = colNames;
        }

        @Override
        public void flatMap(TableSummary tableSummary, Collector<Row> collector){
            for(String colName : tableSummary.getColNames()){
                double min = tableSummary.min(colName);
                double max = tableSummary.max(colName);
                collector.collect(Row.of(TableUtil.findColIndexWithAssertAndHint(colNames, colName), getSplitPointsFromMinMax(min, max,
                    colNameBucketNumber.get(colName))));
            }
        }
    }

    static Number[] getSplitPointsFromMinMax(double min, double max, long bucketNum){
        double distance = max - min;
        if(distance < MIN_MAX_EPSILON){
            return null;
        }
        TreeSet<Number> set = new TreeSet<>(new Comparator<Number>() {
            @Override
            public int compare(Number o1, Number o2) {
                return OBJECT_COMPARATOR.compare(o1, o2);
            }
        });

        for(int i = 0; i < bucketNum - 1; i++){
            set.add(min + (distance / bucketNum) * (i + 1));
        }
        return set.toArray(new Number[0]);
    }

    @Override
    public EqualWidthDiscretizerModelInfoBatchOp getModelInfoBatchOp(){
        return new EqualWidthDiscretizerModelInfoBatchOp().linkFrom(this);
    }
}
