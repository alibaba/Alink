package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.StratifiedSampleWithSizeParams;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.XORShiftRandom;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @auth：baijingjing
 * @createDatetime: 2020/7/7
 * @desc：fencengcaiyang main class
 */
public final class StratifiedSampleWithSizeBatchOp extends BatchOperator<StratifiedSampleWithSizeBatchOp>
        implements StratifiedSampleWithSizeParams<StratifiedSampleWithSizeBatchOp> {

    public StratifiedSampleWithSizeBatchOp() {
        this(new Params());
    }

    public StratifiedSampleWithSizeBatchOp(Params params){
        super(params.set(SEED, new Random().nextLong()));
    }

    public StratifiedSampleWithSizeBatchOp(String groupKey, String size) {
        this(groupKey, size, false);
    }
    public StratifiedSampleWithSizeBatchOp(String groupKey, String size, boolean withReplacement) {
        this(new Params()
             .set(GROUP_KEY, groupKey)
             .set(SIZE, size)
             .set(WITH_REPLACEMENT, withReplacement));
    }

    @Override
    public StratifiedSampleWithSizeBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        // compute index of group key
        int index = computeGroupKeyIndex(in.getColNames(), getGroupKey());

        UnsortedGrouping<Row> groupingOperator = in.getDataSet().groupBy(index);

        String size = getSize();
        //step2: count total number of records per group
        try {
            List<Tuple2<Object, Long>> totalNumberPerGroupList = groupingOperator.reduceGroup(new CountPerGroupFunction(index)).collect();
            Map<Object, Integer> groupSampleSize = new HashMap<>();
            if (StringUtils.isNumeric(size)) {
                for (Tuple2<Object, Long> tuple2 : totalNumberPerGroupList) {
                    groupSampleSize.put(tuple2.f0, Integer.valueOf(size));
                }
            } else {
                //step2.2 :the string ratio defines different ratio per group
                String[] keySizes= size.split(",");
                List<Object> dataValue = totalNumberPerGroupList.stream().map(t -> t.f0).collect(Collectors.toList());
                List<Object> difference = new ArrayList<>(dataValue);
                List<String> defineValue = Arrays.asList(keySizes).stream().map(ks -> ks.split(":")[0]).collect(Collectors.toList());
                difference.removeAll(defineValue);
                if (!difference.isEmpty() || totalNumberPerGroupList.size() != keySizes.length) {
                    throw new IllegalStateException("data set value is :" + Joiner.on(",").join(dataValue) +
                            ", but define value is :" + Joiner.on(",").join(defineValue));
                }
                for (String keySize : keySizes) {
                    String[] sizes = keySize.split(":");
                    for (Tuple2<Object, Long> tuple2 : totalNumberPerGroupList) {
                        if (tuple2.f0.equals(sizes[0])) {
                            groupSampleSize.put(tuple2.f0, Integer.valueOf(sizes[1]));
                            break;
                        }
                    }
                }
            }

            final GroupReduceOperator result = groupingOperator.reduceGroup(new StratifiedSampleWithSizeReduce(getWithReplacement(), getSeed(), index, groupSampleSize));

            this.setOutput(result, in.getSchema());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    private int computeGroupKeyIndex(String[] schema, String groupKey) {
        for (int i = 0; i < schema.length; i++) {
            if (groupKey.equals(schema[i])){
                return i;
            }
        }
        return -1;
    }

    private class CountPerGroupFunction implements GroupReduceFunction<Row, Tuple2<Object, Long>> {

        int index;

        public CountPerGroupFunction(int index) {
            this.index = index;
        }

        @Override
        public void reduce(Iterable<Row> iterable, Collector<Tuple2<Object, Long>> collector) throws Exception {
            Iterator<Row> iterator = iterable.iterator();
            long count = 1L;
            Row next = iterator.next();
            Object key = next.getField(index);
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            collector.collect(new Tuple2<Object, Long>(key, count));
        }

    }

    private class StratifiedSampleWithSizeReduce<T> implements GroupReduceFunction<T, T> {

        private static final long serialVersionUID = -7029204080463866157L;
        private boolean withReplacement;
        private long seed;
        private int keyIndex;
        private Map<Object, Integer> sampleNumsMap;

        public StratifiedSampleWithSizeReduce(boolean withReplacement, long seed, int keyIndex, Map<Object, Integer> sampleNumsMap) {
            this.withReplacement = withReplacement;
            this.seed = seed;
            this.keyIndex = keyIndex;
            this.sampleNumsMap = sampleNumsMap;
        }

        @Override
        public void reduce(Iterable<T> values, Collector<T> out) throws Exception {

            Iterator<IntermediateSampleData<T>> sampled;
            Iterator<T> iterator = values.iterator();
            if (iterator.hasNext()){
                sampled = reservoirSample(iterator);
                while (sampled.hasNext()) {
                    out.collect(sampled.next().getElement());
                }
            }
        }

        protected Iterator<IntermediateSampleData<T>> reservoirSample(Iterator<T> input){

            T next = input.next();
            Row row = (Row)next;
            Object keys = row.getField(keyIndex);
            int numSamples = sampleNumsMap.get(keys).intValue();
            if (numSamples == 0) {
                return new ArrayList<IntermediateSampleData<T>>().iterator();
            }
            long seedAndIndex = seed + keys.hashCode();
            Random random = new XORShiftRandom(seedAndIndex);
            // This queue holds a fixed number of elements with the top K weight for current partition.
            PriorityQueue<IntermediateSampleData<T>> queue = new PriorityQueue<IntermediateSampleData<T>>(numSamples);
            IntermediateSampleData<T> smallest = null;

            if (withReplacement) {
                for (int i = 0; i < numSamples; i++) {
                    queue.add(new IntermediateSampleData<T>(random.nextDouble(), next));
                    smallest = queue.peek();
                }
            } else {
                int index = 0;
                while (input.hasNext()) {
                    T element = input.next();
                    if (index == numSamples) {
                        break;
                    }
                    queue.add(new IntermediateSampleData<T>(random.nextDouble(), element));
                    smallest = queue.peek();

                    index++;
                }
            }
            while (input.hasNext()) {
                T element = input.next();
                // To sample with replacement, we generate K random weights for each element, so that it's
                // possible to be selected multi times.
                for (int i = 0; i < numSamples; i++) {
                    // If current element weight is larger than the smallest one in queue, remove the element
                    // with the smallest weight, and append current element into the queue.
                    double rand = random.nextDouble();
                    if (rand > smallest.getWeight()) {
                        queue.remove();
                        queue.add(new IntermediateSampleData<T>(rand, element));
                        smallest = queue.peek();
                    }
                }
            }
            return queue.iterator();
        }
    }
}
