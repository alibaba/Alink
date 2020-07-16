package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.SampleSeedParam;
import com.alibaba.alink.params.dataproc.SampleGroupColumnParam;
import com.alibaba.alink.params.dataproc.SampleRatioParam;
import com.alibaba.alink.params.dataproc.SampleWithReplacementParams;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.PoissonSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.XORShiftRandom;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;


public final class StratifiedSampleBatchOp extends BatchOperator<StratifiedSampleBatchOp>
        implements SampleGroupColumnParam<StratifiedSampleBatchOp>,
                   SampleRatioParam<StratifiedSampleBatchOp>,
                   SampleSeedParam<StratifiedSampleBatchOp>,
                   SampleWithReplacementParams<StratifiedSampleBatchOp> {

    public StratifiedSampleBatchOp() {
        this(new Params());
    }
    private StratifiedSampleBatchOp(Params params){
        super(params.set(SEED, new Random().nextLong()));
    }

    public StratifiedSampleBatchOp(String groupKey, String ratio) {
        this(groupKey, ratio, false);
    }
    public StratifiedSampleBatchOp(String groupKey, String ratio, boolean withReplacement) {
        this(new Params()
             .set(GROUP_COL, groupKey)
             .set(RATIO, ratio)
             .set(WITH_REPLACEMENT, withReplacement));
    }

    @Override
    public StratifiedSampleBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        // compute index of group key
        int index = computeGroupCloIndex(in.getColNames(), getGroupCol());

        UnsortedGrouping<Row> groupingOperator = in.getDataSet().groupBy(index);

        String ratio = getRatio();
        GroupReduceOperator result = null;
        Long seed = getSeed();
        try {

            if (compile.matcher(ratio).find()) {
                //step2.1 if ratio is number, there is same ratio for each group, so we do nothing but sample directly
                double r = Double.valueOf(ratio);
                result = groupingOperator.reduceGroup(new ConsistentRatioSamplesReduce(getWithReplacement(), r, seed));

            } else {
                //step2.2 :the string ratio defines different ratio per group， we should
                Map<Object, Double> fractionMap = new HashMap<>();
                String[] keyRatios= ratio.split(",");
                for (String keyRatio : keyRatios) {
                    String[] ratios = keyRatio.split(":");
                    fractionMap.put(ratios[0], new Double(ratios[1]));
                }
                result = groupingOperator.reduceGroup(new StratifiedSampleReduce(getWithReplacement(), seed, index, fractionMap));
            }

            this.setOutput(result, in.getSchema());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    private class ConsistentRatioSamplesReduce<T> implements GroupReduceFunction<T, T>{
        private boolean withReplacement;
        private double fraction;
        private long seed;

        public ConsistentRatioSamplesReduce(boolean withReplacement, double fraction, long seed) {
            this.withReplacement = withReplacement;
            this.fraction = fraction;
            this.seed = seed;
        }

        @Override
        public void reduce(Iterable<T> values, Collector<T> out) throws Exception {

            RandomSampler<T> sampler;
            if (withReplacement) {
                sampler = new PoissonSampler<>(fraction, seed);
            } else {
                sampler = new BernoulliSampler<>(fraction, seed);
            }

            Iterator<T> sampled = sampler.sample(values.iterator());
            while (sampled.hasNext()) {
                out.collect(sampled.next());
            }
        }
    }



    private class StratifiedSampleReduce <T> implements GroupReduceFunction<T, T>{


        private static final long serialVersionUID = 2257608997204962490L;
        private boolean withReplacement;
        private long seed;
        private int index;
        private Map<Object, Double> fractionMap;
        private Map<Object, Random> randomMap = new HashMap<>();

        public StratifiedSampleReduce(boolean withReplacement, long seed, int index, Map<Object, Double> fractionMap) {
            this.withReplacement = withReplacement;
            this.seed = seed;
            this.index = index;
            this.fractionMap = fractionMap;
        }

        @Override
        public void reduce(Iterable<T> values, Collector<T> out) throws Exception {
            RandomSampler<T> sampler = null;
            if (withReplacement) {
                sampler = new CustomerPoissonSampler();
            } else {
                sampler = new CustomerBernoulliSampler<T>();
            }
            Iterator<T> sampled = sampler.sample(values.iterator());
            while (sampled.hasNext()) {
                out.collect(sampled.next());
            }
        }
        private final class CustomerPoissonSampler<T> extends RandomSampler<T>{
            private static final double THRESHOLD = 0.4;

            @Override
            public Iterator<T> sample(Iterator<T> input) {
                return new Iterator<T>() {
                    T currentElement;
                    int currentCount = 0;
                    Double fraction = null;
                    PoissonDistribution poissonDistribution = null;
                    Random random = null;
                    @Override
                    public boolean hasNext() {
                        if (currentCount > 0) {
                            return true;
                        } else {
                            samplingProcess();
                            if (currentCount > 0) {
                                return true;
                            } else {
                                fraction = null;
                                poissonDistribution = null;
                                random = null;
                                return false;
                            }
                        }
                    }

                    @Override
                    public T next() {
                        if (currentCount <= 0) {
                            samplingProcess();
                        }
                        currentCount--;
                        return currentElement;
                    }

                    public int poisson_ge1(double p) {
                        // sample 'k' from Poisson(p), conditioned to k >= 1.
                        double q = Math.pow(Math.E, -p);
                        // simulate a poisson trial such that k >= 1.
                        double t = q + (1 - q) * random.nextDouble();
                        int k = 1;
                        // continue standard poisson generation trials.
                        t = t * random.nextDouble();
                        while (t > q) {
                            k++;
                            t = t * random.nextDouble();
                        }
                        return k;
                    }

                    private void skipGapElements(int num) {
                        // skip the elements that occurrence number is zero.
                        int elementCount = 0;
                        while (input.hasNext() && elementCount < num) {
                            currentElement = input.next();
                            elementCount++;
                        }
                    }

                    private void samplingProcess() {
                        if (input.hasNext()) {

                            // NOTE: the fraction is always null when the first iterator, so we should get it from fractionMap,
                            // hasNext() return  false ,it should set fraction is null ,this means a full life iterator is finished
                            if (fraction == null) {
                                currentElement = input.next();
                                Row row = ((Row) currentElement);
                                Object key = row.getField(index);
                                if (!fractionMap.containsKey(key)) {
                                    throw new IllegalArgumentException(key + " not define fraction");
                                }
                                fraction = fractionMap.get(key);
                                if (!randomMap.containsKey(key)) {
                                    randomMap.put(key, new XORShiftRandom(seed + key.hashCode()));
                                }
                                random = randomMap.get(key);

                            }

                            if (fraction <= THRESHOLD) {
                                double u = Math.max(random.nextDouble(), EPSILON);
                                int gap = (int) (Math.log(u) / -fraction);
                                skipGapElements(gap);
                                if (input.hasNext()) {
                                    currentElement = input.next();
                                    currentCount = poisson_ge1(fraction);
                                }
                            } else {
                                // NOTE: the poissonDistribution is always null when the first iterator, so we should create a new Poisson,
                                // hasNext() return  false ,it should set poissonDistribution to null ,this means a full life iterator is finished
                                if (poissonDistribution == null) {
                                    poissonDistribution = new PoissonDistribution(fraction);
                                    poissonDistribution.reseedRandomGenerator(seed);
                                }
                                while (input.hasNext()) {
                                    currentElement = input.next();
                                    currentCount = poissonDistribution.sample();
                                    if (currentCount > 0) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                };
            }
        }

        private final class CustomerBernoulliSampler<T> extends RandomSampler<T>{
            private static final double THRESHOLD = 0.33;
            @Override
            public Iterator<T> sample(final Iterator<T> input) {
                return new Iterator<T>() {
                    T current = null;
                    Random random = null;
                    Double fraction = null;
                    @Override
                    public boolean hasNext() {
                        if (current == null) {
                            current = getNextSampledElement();
                        }
                        if (current == null) {
                            random = null;
                            fraction = null;
                        }

                        return current != null;
                    }

                    @Override
                    public T next() {
                        if (current == null) {
                            return getNextSampledElement();
                        } else {
                            T result = current;
                            current = null;

                            return result;
                        }
                    }

                    private T getNextSampledElement() {
                        if (input.hasNext()) {
                            T next = input.next();
                            if (fraction == null) {
                                Row row = (Row)next;
                                Object key = row.getField(index);
                                if (!fractionMap.containsKey(key)) {
                                    throw new IllegalArgumentException(key + " not define fraction");
                                }
                                fraction = fractionMap.get(key);
                                if (!randomMap.containsKey(key)) {
                                    randomMap.put(key, new XORShiftRandom(seed + key.hashCode()));
                                }
                                random = randomMap.get(key);

                            }

                            if (fraction <= THRESHOLD) {
                                double rand = random.nextDouble();
                                double u = Math.max(rand, EPSILON);
                                int gap = (int) (Math.log(u) / Math.log(1 - fraction));
                                int elementCount = 0;

                                while (input.hasNext() && elementCount < gap) {
                                    next = input.next();
                                    elementCount++;
                                }
                                if (elementCount < gap) {
                                    return null;
                                } else {
                                    return next;
                                }

                            } else {

                                if (random.nextDouble() <= fraction) {
                                    return next;
                                }

                                while (input.hasNext()) {
                                    T element = input.next();

                                    if (random.nextDouble() <= fraction) {
                                        return element;
                                    }
                                }
                                return null;
                            }
                        }
                        return null;
                    }
                };
            }
        }
    }


}
