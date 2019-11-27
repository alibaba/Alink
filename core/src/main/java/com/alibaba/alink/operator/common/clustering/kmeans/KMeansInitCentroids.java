package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.params.clustering.KMeansTrainParams;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/**
 * Initialize a set of cluster centers using the k-means|| algorithm by Bahmani et al. (Bahmani et al., Scalable
 * K-Means++, VLDB 2012). This is a variant of k-means++ that tries to find dissimilar cluster centers by starting with
 * a random center and then doing passes where more centers are chosen with probability proportional to their squared
 * distance to the current cluster set. It results in a provable approximation to an optimal clustering.
 * <p>
 * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
 */
public class KMeansInitCentroids {
    private static final Logger LOG = LoggerFactory.getLogger(KMeansInitCentroids.class);
    private static final String CENTER = "centers";
    private static final String SUM_COSTS = "sumCosts";
    private static final String VECTOR_SIZE = "vectorSize";

    private enum InitMode {
        /**
         * Random init.
         */
        RANDOM,
        /**
         * KMeansPlusPlus init.
         */
        K_MEANS_PARALLEL
    }

    public static DataSet<FastDistanceMatrixData> initKmeansCentroids(DataSet<FastDistanceVectorData> data,
                                                                      FastDistance distance, Params params,
                                                                      DataSet<Integer> vectorSize) {
        final String initMode = params.get(KMeansTrainParams.INIT_MODE);
        final int initSteps = params.get(KMeansTrainParams.INIT_STEPS);
        final int k = params.get(KMeansTrainParams.K);

        DataSet<FastDistanceMatrixData> initCentroid;
        switch (InitMode.valueOf(initMode.toUpperCase())){
            case RANDOM:
            {
                initCentroid = randomInit(data, k, distance, vectorSize);
                break;
            }
            case K_MEANS_PARALLEL:{
                initCentroid = kMeansPlusPlusInit(data, k, initSteps, distance, vectorSize);
                break;
            }
            default:{
                throw new IllegalArgumentException("Unknown init mode: " + initMode);
            }
        }

        return initCentroid;
    }

    /**
     * Initialize a set of cluster centers by sampling without replacement from samples.
     */
    private static DataSet<FastDistanceMatrixData> randomInit(DataSet<FastDistanceVectorData> data,
                                                              final int k,
                                                              final FastDistance distance,
                                                              DataSet<Integer> vectorSize) {
        return DataSetUtils.sampleWithSize(data, false, k)
            .mapPartition(new RichMapPartitionFunction<FastDistanceVectorData, FastDistanceMatrixData>() {
                @Override
                public void mapPartition(Iterable<FastDistanceVectorData> values,
                                         Collector<FastDistanceMatrixData> out) {
                    List<Integer> list = this.getRuntimeContext().getBroadcastVariable(VECTOR_SIZE);
                    int vectorSize = list.get(0);
                    List<FastDistanceVectorData> vectorList = new ArrayList<>();
                    values.forEach(vectorList::add);
                    out.collect(KMeansUtil.buildCentroidsMatrix(vectorList, distance, vectorSize));
                }
            })
            .withBroadcastSet(vectorSize, VECTOR_SIZE)
            .setParallelism(1);
    }

    private static DataSet<FastDistanceMatrixData> kMeansPlusPlusInit(DataSet<FastDistanceVectorData> data,
                                                                      final int k,
                                                                      final int initSteps,
                                                                      final FastDistance distance,
                                                                      DataSet<Integer> vectorSize) {
        DataSet<Tuple2<Long, FastDistanceVectorData>> dataWithId = DataSetUtils.zipWithUniqueId(data);
        //id, vectorData, nearestCenterId, nearestCenterDist, mark(center/data)
        DataSet<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> dataNeighborMark = dataWithId.map(
            new MapFunction<Tuple2<Long, FastDistanceVectorData>,
                Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>>() {
                @Override
                public Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> map(
                    Tuple2<Long, FastDistanceVectorData> value) {
                    return Tuple5.of(value.f0, value.f1, -1L, Double.MAX_VALUE, false);
                }
            }).withForwardedFields("f0;f1");

        DataSet<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> centers = DataSetUtils.sampleWithSize(
            dataNeighborMark, false, 1, 0L)
            .map(new TransformToCenter())
            .withForwardedFields("f0;f1");

        dataNeighborMark = dataNeighborMark
            .map(new CalWeight(distance))
            .withBroadcastSet(centers, CENTER)
            .withForwardedFields("f0;f1;f4");

        IterativeDataSet<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> loop =
            centers.union(dataNeighborMark).iterate(initSteps - 1);

        DataSet<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> dataOnly = loop.filter(new FilterData());

        DataSet<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> oldCenter = loop.filter(
            new FilterCenter());

        DataSet<Tuple1<Double>> sumCosts = dataOnly.<Tuple1<Double>>project(3).aggregate(SUM, 0);

        DataSet<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> newCenter = dataOnly
            .filter(new FilterNewCenter(k))
            .withBroadcastSet(sumCosts, SUM_COSTS)
            .name("kmeans_||_pick")
            .map(new TransformToCenter())
            .withForwardedFields("f0;f1");

        DataSet<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> updateData = dataOnly
            .map(new CalWeight(distance))
            .withBroadcastSet(newCenter, CENTER)
            .withForwardedFields("f0;f1;f4");

        DataSet<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> finalDataAndCenter = loop.closeWith(
            updateData.union(oldCenter).union(newCenter));

        DataSet<Tuple2<Long, FastDistanceVectorData>> finalCenters = finalDataAndCenter.filter(new FilterCenter())
            .project(0, 1);

        DataSet<Tuple2<Long, FastDistanceVectorData>> weight = finalDataAndCenter
            .filter(new FilterData())
            .map(new MapFunction<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>, Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Long, Long> map(Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> t) {
                    return Tuple2.of(t.f2, 1L);
                }
            })
            .withForwardedFields("f2->f0")
            .groupBy(0)
            .aggregate(SUM, 1)
            .join(finalCenters)
            .where(0)
            .equalTo(0)
            .projectFirst(1)
            .projectSecond(1);

        return weight
            .mapPartition(new LocalKmeans(k, distance))
            .withBroadcastSet(vectorSize, VECTOR_SIZE)
            .setParallelism(1);
    }

    private static class FilterData
        implements FilterFunction<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> {
        @Override
        public boolean filter(Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> value)
            throws Exception {
            return !value.f4;
        }
    }

    private static class FilterCenter
        implements FilterFunction<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> {
        @Override
        public boolean filter(Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> value)
            throws Exception {
            return value.f4;
        }
    }

    private static class FilterNewCenter
        extends RichFilterFunction<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> {
        private transient double costThre;
        private transient Random random;
        private int k;

        FilterNewCenter(int k) {
            this.k = k;
        }

        @Override
        public void open(Configuration parameters) {
            random = new Random(getRuntimeContext().getIndexOfThisSubtask());
            List<Tuple1<Double>> bcCostSum = getRuntimeContext().getBroadcastVariable(SUM_COSTS);
            costThre = 2.0 * k / bcCostSum.get(0).f0;
        }

        @Override
        public boolean filter(Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> value) {
            return random.nextDouble() < value.f3 * costThre;
        }
    }

    private static class TransformToCenter
        implements MapFunction<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>,
        Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> {
        @Override
        public Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> map(
            Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> value) throws Exception {
            value.f2 = -1L;
            value.f3 = Double.MAX_VALUE;
            value.f4 = true;
            return value;
        }
    }

    private static class CalWeight extends
        RichMapFunction<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>,
            Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> {
        private transient List<Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean>> centers;
        private FastDistance distance;

        CalWeight(FastDistance distance) {
            this.distance = distance;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            centers = getRuntimeContext().getBroadcastVariable(CENTER);
        }

        @Override
        public Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> map(
            Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> sample) throws Exception {
            for (Tuple5<Long, FastDistanceVectorData, Long, Double, Boolean> c : centers) {
                double d = distance.calc(c.f1, sample.f1).get(0, 0);
                if (d < sample.f3) {
                    sample.f2 = c.f0;
                    sample.f3 = d;
                }
            }
            return sample;
        }
    }

    private static class LocalKmeans
        extends RichMapPartitionFunction<Tuple2<Long, FastDistanceVectorData>, FastDistanceMatrixData> {
        private FastDistance distance;
        private int k;
        private transient int vectorSize;

        LocalKmeans(int k, FastDistance distance) {
            this.k = k;
            this.distance = distance;
        }

        @Override
        public void open(Configuration parameters) {
            LOG.info("TaskId {} Local Kmeans begins!",
                this.getRuntimeContext().getIndexOfThisSubtask());
            List<Integer> list = this.getRuntimeContext().getBroadcastVariable(VECTOR_SIZE);
            vectorSize = list.get(0);
        }

        @Override
        public void close() {
            LOG.info("TaskId {} Local Kmeans ends!",
                this.getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Long, FastDistanceVectorData>> values,
                                 Collector<FastDistanceMatrixData> out) throws
            Exception {
            List<Long> sampleWeightsList = new ArrayList<>();
            List<FastDistanceVectorData> samples = new ArrayList<>();
            values.forEach(v -> {
                sampleWeightsList.add(v.f0);
                samples.add(v.f1);
            });

            if (samples.size() <= k) {
                out.collect(KMeansUtil.buildCentroidsMatrix(samples, distance, vectorSize));
            } else {
                long[] sampleWeights = new long[sampleWeightsList.size()];
                for (int i = 0; i < sampleWeights.length; i++) {
                    sampleWeights[i] = sampleWeightsList.get(i);
                }
                out.collect(LocalKmeansFunc
                    .kmeans(k, sampleWeights, samples.toArray(new FastDistanceVectorData[0]), distance, vectorSize));
            }
        }
    }
}
