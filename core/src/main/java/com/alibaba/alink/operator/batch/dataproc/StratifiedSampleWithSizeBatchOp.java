package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.StrafiedSampleWithSizeParams;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.sampling.DistributedRandomSampler;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithReplacement;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithoutReplacement;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * StratifiedSample with given size with or without replacement.
 */
public final class StratifiedSampleWithSizeBatchOp extends BatchOperator<StratifiedSampleWithSizeBatchOp>
        implements StrafiedSampleWithSizeParams<StratifiedSampleWithSizeBatchOp>{

    public StratifiedSampleWithSizeBatchOp() {
        this(new Params());
    }

    public StratifiedSampleWithSizeBatchOp(Params params){
        super(params);
    }

    @Override
    public StratifiedSampleWithSizeBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        // compute index of group key
        int index = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), getGroupCol());

        Integer size = getSize();
        String sizes = getSizes();
        Preconditions.checkArgument(null == size ^ null == sizes,
            "You can either set size or sizes!");

        long seed = new Random().nextLong();
        DataSet<Row> res = in
            .getDataSet()
            .groupBy(index)
            .reduceGroup(new StratifiedSampleWithSizeReduce(getWithReplacement(), seed, index, size, sizes));

        this.setOutput(res, in.getSchema());
        return this;
    }

    private class StratifiedSampleWithSizeReduce<T> implements GroupReduceFunction<T, T> {

        private static final long serialVersionUID = -7029204080463866157L;
        private boolean withReplacement;
        private long seed;
        private int keyIndex;
        private Integer sampleSize;
        private Map<Object, Integer> sampleNumsMap;

        public StratifiedSampleWithSizeReduce(boolean withReplacement,
                                              long seed,
                                              int keyIndex,
                                              Integer size,
                                              String sizes) {
            this.withReplacement = withReplacement;
            this.seed = seed;
            this.keyIndex = keyIndex;
            if (null != size) {
                //step2.1 if ratio is number, there is same ratio for each group, so we do nothing but sample directly
                sampleSize = size;
            } else {
                //step2.2 :the string ratio defines different ratio per group， we should
                sampleNumsMap = new HashMap<>();
                String[] keyRatios = sizes.split(StratifiedSampleBatchOp.COLUMN_DELIMITER);
                for (String keyRatio : keyRatios) {
                    String[] sizeArray = keyRatio.split(StratifiedSampleBatchOp.KEY_VALUE_DELIMITER);
                    int groupSize = new Integer(sizeArray[1]);
                    Preconditions.checkArgument(groupSize >= 0, "SampleSize must be non-negative!");
                    sampleNumsMap.put(sizeArray[0], groupSize);
                }
            }
        }

        @Override
        public void reduce(Iterable<T> values, Collector<T> out) throws Exception {
            StratifiedSampleBatchOp.GetFirstIterator iterator = new StratifiedSampleBatchOp.GetFirstIterator(values.iterator());
            Integer numSample = sampleSize;
            if (null == numSample) {
                Row first = (Row)iterator.getFirst();
                if (null != first) {
                    Object key = first.getField(keyIndex);
                    numSample = sampleNumsMap.get(key);
                    Preconditions.checkNotNull(numSample, key + "is not contained in map!");
                } else {
                    return;
                }
            }

            DistributedRandomSampler<T> sampler;
            if (withReplacement) {
                sampler = new ReservoirSamplerWithReplacement<>(numSample, seed);
            } else {
                sampler = new ReservoirSamplerWithoutReplacement<>(numSample, seed);
            }

            Iterator<T> sampled = sampler.sample(iterator);
            while (sampled.hasNext()) {
                out.collect(sampled.next());
            }
        }
    }
}
