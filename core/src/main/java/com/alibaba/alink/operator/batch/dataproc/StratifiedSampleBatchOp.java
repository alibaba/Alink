package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.StrafiedSampleParams;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.PoissonSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * StratifiedSample with given ratio with or without replacement.
 */
public final class StratifiedSampleBatchOp extends BatchOperator<StratifiedSampleBatchOp>
        implements StrafiedSampleParams<StratifiedSampleBatchOp>{

    static String COLUMN_DELIMITER = ",";
    static String KEY_VALUE_DELIMITER = ":";

    static class GetFirstIterator<E> implements Iterator<E>{
        private Iterator<E> originIterator;
        private E first;

        public GetFirstIterator(Iterator<E> iterator){
            this.originIterator = iterator;
            if(this.originIterator.hasNext()){
                first = this.originIterator.next();
            }
        }

        public E getFirst(){
            return first;
        }

        @Override
        public boolean hasNext(){
            return (null != first || originIterator.hasNext());
        }

        @Override
        public E next(){
            if(null != first){
                E tmp = first;
                first = null;
                return tmp;
            }else{
                return originIterator.next();
            }
        }
    }

    public StratifiedSampleBatchOp() {
        this(new Params());
    }

    private StratifiedSampleBatchOp(Params params){
        super(params);
    }

    @Override
    public StratifiedSampleBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        // compute index of group key
        int index = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), getGroupCol());
        Double ratio = getRatio();
        String ratios = getRatios();
        Preconditions.checkArgument(null == ratio ^ null == ratios,
            "You can either set Ratio or Ratios!");

        long seed = new Random().nextLong();
        DataSet<Row> res = in
            .getDataSet()
            .groupBy(index)
            .reduceGroup(new StratifiedSampleReduce(getWithReplacement(), seed, index, ratio, ratios));

        this.setOutput(res, in.getSchema());
        return this;
    }

    private class StratifiedSampleReduce <T> extends RichGroupReduceFunction<T, T> {
        private static final long serialVersionUID = 2257608997204962490L;
        private boolean withReplacement;
        private long seed;
        private int index;
        private Double sampleRatio;
        private Map<Object, Double> fractionMap;

        public StratifiedSampleReduce(boolean withReplacement, long seed, int index, Double ratio, String ratios) {
            this.withReplacement = withReplacement;
            this.seed = seed;
            this.index = index;
            if (null != ratio) {
                //step2.1 if ratio is number, there is same ratio for each group, so we do nothing but sample directly
                sampleRatio = ratio;
            } else {
                //step2.2 :the string ratio defines different ratio per group， we should
                fractionMap = new HashMap<>();
                String[] keyRatios = ratios.split(COLUMN_DELIMITER);
                for (String keyRatio : keyRatios) {
                    String[] ratioArray = keyRatio.split(KEY_VALUE_DELIMITER);
                    Double groupRatio = new Double(ratioArray[1]);
                    Preconditions.checkArgument(groupRatio >= 0.0 && groupRatio <= 1.0,
                        "Ratio must be in range [0, 1].");
                    fractionMap.put(ratioArray[0], groupRatio);
                }
            }
        }

        @Override
        public void reduce(Iterable<T> values, Collector<T> out) throws Exception {
            GetFirstIterator iterator = new GetFirstIterator(values.iterator());
            Double fraction = sampleRatio;
            if (null == fraction) {
                Row first = (Row)iterator.getFirst();
                if (null != first) {
                    Object key = first.getField(index);
                    fraction = fractionMap.get(key);
                    Preconditions.checkNotNull(fraction, key + " is not contained in map!");
                } else {
                    return;
                }
            }

            RandomSampler<T> sampler;
            long seedAndIndex = this.seed + (long)this.getRuntimeContext().getIndexOfThisSubtask();
            if (withReplacement) {
                sampler = new PoissonSampler<T>(fraction, seedAndIndex);
            } else {
                sampler = new BernoulliSampler<T>(fraction, seedAndIndex);
            }
            Iterator<T> sampled = sampler.sample(iterator);
            while (sampled.hasNext()) {
                out.collect(sampled.next());
            }
        }
    }

}
