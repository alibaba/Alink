package com.alibaba.alink.operator.batch.clustering;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelData;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelData.ClusterSummary;
import com.alibaba.alink.operator.common.clustering.BisectingKMeansModelDataConverter;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.clustering.BisectingKMeansTrainParams;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Bisecting k-means is a kind of hierarchical clustering algorithm.
 * <p>
 * Bisecting k-means algorithm starts from a single cluster that contains all points. Iteratively it finds divisible
 * clusters on the bottom level and bisects each of them using k-means, until there are `k` leaf clusters in total or no
 * leaf clusters are divisible.
 *
 * @see <a href="http://glaros.dtc.umn.edu/gkhome/fetch/papers/docclusterKDDTMW00.pdf">
 * Steinbach, Karypis, and Kumar, A comparison of document clustering techniques, KDD Workshop on Text Mining,
 * 2000.</a>
 */
public final class BisectingKMeansTrainBatchOp extends BatchOperator<BisectingKMeansTrainBatchOp>
    implements BisectingKMeansTrainParams<BisectingKMeansTrainBatchOp> {

    public final static long ROOT_INDEX = 1;
    private static final Logger LOG = LoggerFactory.getLogger(BisectingKMeansTrainBatchOp.class);
    private static final String VECTOR_SIZE = "vectorSize";
    private static final String DIVISIBLE_INDICES = "divisibleIndices";
    private static final String ITER_INFO = "iterInfo";
    private static final String NEW_CLUSTER_CENTERS = "newClusterCenters";

    public BisectingKMeansTrainBatchOp() {
        this(new Params());
    }

    public BisectingKMeansTrainBatchOp(Params params) {
        super(params);
    }

    /**
     * Returns the left child index of the given node index.
     */
    public static long leftChildIndex(long index) {
        return 2 * index;
    }

    /**
     * Returns the right child index of the given node index.
     */
    public static long rightChildIndex(long index) {
        return 2 * index + 1;
    }

    private static DataSet<Tuple2<Long, DenseVector>>
    getNewClusterCenters(DataSet<Tuple3<Long, ClusterSummary, IterInfo>> allClusterSummaries) {
        return allClusterSummaries
            .flatMap(new FlatMapFunction<Tuple3<Long, ClusterSummary, IterInfo>,
                Tuple2<Long, DenseVector>>() {
                @Override
                public void flatMap(Tuple3<Long, ClusterSummary, IterInfo> value,
                                    Collector<Tuple2<Long, DenseVector>> out) {
                    if (value.f2.isNew) {
                        out.collect(Tuple2.of(value.f0, value.f1.center));
                    }
                }
            })
            .name("getNewClusterCenters");
    }

    private static DataSet<Long>
    getDivisibleClusterIndices(
        DataSet<Tuple3<Long, ClusterSummary, IterInfo>> allClusterSummaries,
        final int minDivisibleClusterSize) {
        return allClusterSummaries
            .flatMap(new FlatMapFunction<Tuple3<Long, ClusterSummary, IterInfo>, Long>() {
                @Override
                public void flatMap(Tuple3<Long, ClusterSummary, IterInfo> value,
                                    Collector<Long> out) throws Exception {
                    LOG.info("getDivisibleS {}", value);
                    if (value.f2.isDividing) {
                        if (value.f1.size >= minDivisibleClusterSize) {
                            out.collect(value.f0);
                        }
                    }
                }
            })
            .name("getDivisibleClusterIndices");
    }

    // If at the cluster dividing step, divide current active clusters;
    // Otherwise, just copy existing clusters.
    private static DataSet<Tuple3<Long, ClusterSummary, IterInfo>> getOrSplitClusters(
        DataSet<Tuple3<Long, ClusterSummary, IterInfo>> clustersSummariesAndIterInfo,
        final int k) {
        return clustersSummariesAndIterInfo
            .partitionCustom(
                new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return 0;
                    }
                }, new KeySelector<Tuple3<Long, ClusterSummary, IterInfo>, Integer>() {
                    @Override
                    public Integer getKey(Tuple3<Long, ClusterSummary, IterInfo> value) {
                        return 0;
                    }
                })
            .mapPartition(
                new RichMapPartitionFunction<Tuple3<Long, ClusterSummary, IterInfo>, Tuple3
                    <Long, ClusterSummary, IterInfo>>() {
                    private transient Random random;

                    @Override
                    public void open(Configuration parameters) {
                        if (random == null && getRuntimeContext().getIndexOfThisSubtask() == 0) {
                            random = new Random(getRuntimeContext().getIndexOfThisSubtask());
                        }
                    }

                    @Override
                    public void mapPartition(
                        Iterable<Tuple3<Long, ClusterSummary, IterInfo>> summaries,
                        Collector<Tuple3<Long, ClusterSummary, IterInfo>> out) {
                        if (getRuntimeContext().getIndexOfThisSubtask() > 0) {
                            return;
                        }

                        List<Tuple3<Long, ClusterSummary, IterInfo>> clustersAndIterInfo = new ArrayList<>();
                        summaries.forEach(clustersAndIterInfo::add);

                        if (clustersAndIterInfo.get(0).f2.doBisectionInStep()) {
                            // find all splitable clusters
                            Set<Long> splitableClusters = findSplitableClusters(clustersAndIterInfo, k);
                            boolean shouldStopSplit = (splitableClusters.size() + getNumLeaf(clustersAndIterInfo)) >= k;

                            // split clusters
                            clustersAndIterInfo.forEach(t -> {
                                assert (!t.f2.isDividing);
                                assert (!t.f2.isNew);
                                if (splitableClusters.contains(t.f0)) {
                                    ClusterSummary summary = t.f1;
                                    Tuple2<DenseVector, DenseVector> newCenters = splitCenter(summary.center, random);
                                    long leftChildIndex = leftChildIndex(t.f0);
                                    long rightChildIndex = rightChildIndex(t.f0);
                                    ClusterSummary leftChildSummary = new ClusterSummary();
                                    leftChildSummary.center = newCenters.f0;
                                    IterInfo leftChildIterInfo = new IterInfo(
                                        t.f2.maxIter,
                                        t.f2.bisectingStepNo,
                                        t.f2.innerIterStepNo,
                                        false,
                                        true,
                                        shouldStopSplit);
                                    ClusterSummary rightChildSummary = new ClusterSummary();
                                    rightChildSummary.center = newCenters.f1;
                                    IterInfo rightChildIterInfo = new IterInfo(
                                        t.f2.maxIter,
                                        t.f2.bisectingStepNo,
                                        t.f2.innerIterStepNo,
                                        false,
                                        true,
                                        shouldStopSplit);
                                    t.f2.isDividing = true;
                                    t.f2.shouldStopSplit = shouldStopSplit;
                                    out.collect(t);
                                    out.collect(Tuple3.of(leftChildIndex, leftChildSummary, leftChildIterInfo));
                                    out.collect(Tuple3.of(rightChildIndex, rightChildSummary, rightChildIterInfo));
                                } else {
                                    t.f2.shouldStopSplit = shouldStopSplit;
                                    out.collect(t);
                                }
                            });
                        } else {
                            // copy existing clusters
                            clustersAndIterInfo.forEach(out::collect);
                        }
                    }
                })
            .name("get_or_split_clusters");
    }

    private static Tuple2<DenseVector, DenseVector> splitCenter(DenseVector center, Random random) {
        int dim = center.size();
        double norm = Math.sqrt(BLAS.dot(center, center));
        double level = 1.0e-4 * norm;
        DenseVector noise = new DenseVector(dim);
        for (int i = 0; i < dim; i++) {
            noise.set(i, level * random.nextDouble());
        }
        return Tuple2.of(center.minus(noise), center.plus(noise));
    }

    private static Set<Long> findSplitableClusters(List<Tuple3<Long, ClusterSummary, IterInfo>> allClusterSummaries,
                                                   int k) {
        Set<Long> clusterIds = new HashSet<>();
        List<Long> leafs = new ArrayList<>();
        List<Tuple3<Long, ClusterSummary, IterInfo>> splitableClusters = new ArrayList<>();
        allClusterSummaries.forEach(t -> clusterIds.add(t.f0));
        LOG.info("existingClusterIds {}", JsonConverter.toJson(clusterIds));
        allClusterSummaries.forEach(t -> {
            boolean isLeaf = !clusterIds.contains(leftChildIndex(t.f0)) && !clusterIds.contains(rightChildIndex(t.f0));
            if (isLeaf) {
                leafs.add(t.f0);
            }
            if (isLeaf && t.f1.size > 1) {
                splitableClusters.add(t);
            }
        });
        int numClusterToSplit = k - leafs.size();
        Integer[] order = new Integer[splitableClusters.size()];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        Arrays.sort(order, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return -Double.compare(splitableClusters.get(o1).f1.cost, splitableClusters.get(o2).f1.cost);
            }
        });
        List<Long> splitableClusterIds = new ArrayList<>();
        for (int i = 0; i < Math.min(numClusterToSplit, splitableClusters.size()); i++) {
            splitableClusterIds.add(splitableClusters.get(order[i]).f0);
        }
        LOG.info("toSplitClusterIds {}", JsonConverter.toJson(splitableClusterIds));
        return new HashSet<>(splitableClusterIds);
    }

    private static int getNumLeaf(List<Tuple3<Long, ClusterSummary, IterInfo>> allClusterSummaries) {
        Set<Long> clusterIds = new HashSet<>();
        allClusterSummaries.forEach(t -> clusterIds.add(t.f0));
        int n = 0;
        for (Tuple3<Long, ClusterSummary, IterInfo> t : allClusterSummaries) {
            boolean isLeaf = !clusterIds.contains(leftChildIndex(t.f0)) && !clusterIds.contains(rightChildIndex(t.f0));
            if (isLeaf) {
                n++;
            }
        }
        return n;
    }

    /**
     * Update the assignment of each samples.
     *
     * Note that we keep the updated assignment of each samples in memory, instead of putting it
     * to a looped dataset.
     *
     * @param data Initial assignment of each samples.
     * @param divisibleIndices
     * @param newClusterCenters
     * @param distance
     * @param iterInfo
     * @return Updated assignment of each samples.
     */
    private static DataSet<Tuple3<Long, DenseVector, Long>> updateAssignment(
        DataSet<Tuple3<Long, DenseVector, Long>> data,
        DataSet<Long> divisibleIndices,
        DataSet<Tuple2<Long, DenseVector>> newClusterCenters,
        final ContinuousDistance distance,
        DataSet<Tuple1<IterInfo>> iterInfo) {

        return data
            .map(new RichMapFunction<Tuple3<Long, DenseVector, Long>,
                Tuple4<Integer, Long, DenseVector, Long>>() {
                private transient int taskId;

                @Override
                public void open(Configuration parameters) {
                    this.taskId = getRuntimeContext().getIndexOfThisSubtask();
                }

                @Override
                public Tuple4<Integer, Long, DenseVector, Long> map(Tuple3<Long, DenseVector, Long> value) {
                    return Tuple4.of(taskId, value.f0, value.f1, value.f2);
                }
            })
            .withForwardedFields("f0->f1;f1->f2;f2->f3")
            .name("append_partition_id")
            .groupBy(0)
            .sortGroup(1, Order.ASCENDING)
            .withPartitioner(new Partitioner<Integer>() {
                @Override
                public int partition(Integer key, int numPartitions) {
                    return key % numPartitions;
                }
            })
            .reduceGroup(
                new RichGroupReduceFunction<Tuple4<Integer, Long, DenseVector, Long>, Tuple3<Long, DenseVector,
                    Long>>() {
                    transient Set<Long> divisibleIndices;
                    transient Map<Long, DenseVector> newClusterCenters;

                    transient boolean shouldInitState;
                    transient boolean shouldUpdateState;
                    transient List<Tuple2<Long, Long>> assignmentInState; // sampleId -> clusterId

                    // In euclidean case, find closer center out of two by checking which
                    // side of the middle plane the point lies in.
                    transient Map<Long, Tuple2<DenseVector, Double>> middlePlanes;

                    @Override
                    public void open(Configuration parameters) {
                        List<Long> bcDivisibleIndices = getRuntimeContext().getBroadcastVariable(DIVISIBLE_INDICES);
                        divisibleIndices = new HashSet<>(bcDivisibleIndices);
                        List<Tuple1<IterInfo>> bcIterInfo = getRuntimeContext().getBroadcastVariable(ITER_INFO);
                        shouldUpdateState = bcIterInfo.get(0).f0.atLastInnerIterStep();
                        shouldInitState = getIterationRuntimeContext().getSuperstepNumber() == 1;
                        List<Tuple2<Long, DenseVector>> bcNewClusterCenters = getRuntimeContext().getBroadcastVariable(
                            NEW_CLUSTER_CENTERS);
                        newClusterCenters = new HashMap<>(0);
                        bcNewClusterCenters.forEach(t -> newClusterCenters.put(t.f0, t.f1));
                        if (distance instanceof EuclideanDistance) {
                            middlePlanes = new HashMap<>(0);
                            divisibleIndices.forEach(parentIndex -> {
                                long lchild = leftChildIndex(parentIndex);
                                long rchild = rightChildIndex(parentIndex);
                                DenseVector m = newClusterCenters.get(rchild).plus(newClusterCenters.get(lchild));
                                DenseVector v = newClusterCenters.get(rchild).minus(newClusterCenters.get(lchild));
                                BLAS.scal(0.5, m);
                                double length = BLAS.dot(m, v);
                                middlePlanes.put(parentIndex, Tuple2.of(v, length));
                            });
                        }
                        if (shouldInitState) {
                            assignmentInState = new ArrayList<>();
                        }
                    }

                    @Override
                    public void reduce(Iterable<Tuple4<Integer, Long, DenseVector, Long>> samples,
                                       Collector<Tuple3<Long, DenseVector, Long>> out) {
                        int pos = 0;
                        for (Tuple4<Integer, Long, DenseVector, Long> sample : samples) {
                            long parentClusterId = sample.f3;
                            if (shouldInitState) {
                                assignmentInState.add(Tuple2.of(sample.f1, sample.f3));
                            } else {
                                if (!sample.f1.equals(assignmentInState.get(pos).f0)) {
                                    throw new RuntimeException("Data out of order.");
                                }
                                parentClusterId = assignmentInState.get(pos).f1;
                            }
                            if (divisibleIndices.contains(parentClusterId)) {
                                long leftChildIdx = leftChildIndex(parentClusterId);
                                long rightChildIdx = rightChildIndex(parentClusterId);
                                long clusterId;
                                if (distance instanceof EuclideanDistance) {
                                    Tuple2<DenseVector, Double> plane = middlePlanes.get(parentClusterId);
                                    double d = BLAS.dot(sample.f2, plane.f0);
                                    clusterId = d < plane.f1 ? leftChildIdx : rightChildIdx;
                                } else {
                                    List<Tuple3<Long, Double, DenseVector>> newChildrenCenters = new ArrayList<>(2);
                                    newChildrenCenters.add(
                                        Tuple3.of(leftChildIdx, 0., newClusterCenters.get(leftChildIdx)));
                                    newChildrenCenters.add(
                                        Tuple3.of(rightChildIdx, 0., newClusterCenters.get(rightChildIdx)));
                                    clusterId = findClusterV2(newChildrenCenters, sample.f2, distance);
                                }
                                out.collect(Tuple3.of(sample.f1, sample.f2, clusterId));

                                if (shouldUpdateState) {
                                    assignmentInState.set(pos, Tuple2.of(sample.f1, clusterId));
                                }
                            }
                            pos++;
                        }
                    }
                })
            .withBroadcastSet(divisibleIndices, DIVISIBLE_INDICES)
            .withBroadcastSet(newClusterCenters, NEW_CLUSTER_CENTERS)
            .withBroadcastSet(iterInfo, ITER_INFO)
            .name("update_assignment");
    }

    public static long findClusterV2(Iterable<Tuple3<Long, Double, DenseVector>> centroids, DenseVector sample,
                                     ContinuousDistance distance) {
        long clusterId = -1;
        double d = Double.MAX_VALUE;

        for (Tuple3<Long, Double, DenseVector> c : centroids) {
            double cost = distance.calc(sample, c.f2);
            if (cost < d) {
                clusterId = c.f0;
                d = cost;
            }
        }
        return clusterId;
    }

    /**
     * According to the current sample distribution, get the cluster summary.
     *
     * @param assignment   <ClusterId, Vector> sample pair.
     * @param dim          vectorSize.
     * @param distanceType distance.
     * @return <ClusterId, ClusterSummary> pair.
     */
    private static DataSet<Tuple2<Long, ClusterSummary>>
    summary(DataSet<Tuple2<Long, DenseVector>> assignment, DataSet<Integer> dim, final DistanceType distanceType) {
        return assignment
            .mapPartition(
                new RichMapPartitionFunction<Tuple2<Long, DenseVector>,
                    Tuple2<Long, ClusterSummaryAggregator>>() {

                    @Override
                    public void mapPartition(Iterable<Tuple2<Long, DenseVector>> values,
                                             Collector<Tuple2<Long, ClusterSummaryAggregator>> out) {
                        Map<Long, ClusterSummaryAggregator> aggregators = new HashMap(0);
                        final int dim = (Integer)(getRuntimeContext().getBroadcastVariable(VECTOR_SIZE).get(0));
                        values.forEach(v -> {
                            ClusterSummaryAggregator aggregator = aggregators.getOrDefault(v.f0,
                                new ClusterSummaryAggregator(dim, distanceType));
                            aggregator.add(v.f1);
                            aggregators.putIfAbsent(v.f0, aggregator);
                        });
                        aggregators.forEach((k, v) -> out.collect(Tuple2.of(k, v)));
                    }
                })
            .name("local_aggregate_cluster_summary")
            .withBroadcastSet(dim, VECTOR_SIZE)
            .groupBy(0)
            .reduce(new ReduceFunction<Tuple2<Long, ClusterSummaryAggregator>>() {
                @Override
                public Tuple2<Long, ClusterSummaryAggregator> reduce(Tuple2<Long, ClusterSummaryAggregator> value1,
                                                                     Tuple2<Long, ClusterSummaryAggregator> value2) {
                    value1.f1.merge(value2.f1);
                    return value1;
                }
            })
            .name("global_aggregate_cluster_summary")
            .map(
                new MapFunction<Tuple2<Long, ClusterSummaryAggregator>, Tuple2<Long, ClusterSummary>>() {
                    @Override
                    public Tuple2<Long, ClusterSummary> map(
                        Tuple2<Long, ClusterSummaryAggregator> value) {
                        ClusterSummary summary = value.f1.toClusterSummary();
                        return Tuple2.of(value.f0, summary);
                    }
                })
            .withForwardedFields("f0")
            .name("make_cluster_summary");
    }

    private static DataSet<Tuple3<Long, ClusterSummary, IterInfo>>
    updateClusterSummariesAndIterInfo(
        DataSet<Tuple3<Long, ClusterSummary, IterInfo>> oldClusterSummariesWithIterInfo,
        DataSet<Tuple2<Long, ClusterSummary>> newClusterSummaries) {
        return oldClusterSummariesWithIterInfo
            .leftOuterJoin(newClusterSummaries)
            .where(0).equalTo(0)
            .with(
                new RichJoinFunction<Tuple3<Long, ClusterSummary, IterInfo>, Tuple2<Long,
                    ClusterSummary>,
                    Tuple3<Long, ClusterSummary, IterInfo>>() {

                    @Override
                    public Tuple3<Long, ClusterSummary, IterInfo> join(
                        Tuple3<Long, ClusterSummary, IterInfo> oldSummary,
                        Tuple2<Long, ClusterSummary> newSummary) {
                        if (newSummary == null) {
                            if (oldSummary.f2.isNew) {
                                throw new RuntimeException("Encounter an empty cluster: " + oldSummary);
                            }
                            oldSummary.f2.updateIterInfo();
                            return oldSummary;
                        } else {
                            IterInfo iterInfo = oldSummary.f2;
                            iterInfo.updateIterInfo();
                            return Tuple3.of(newSummary.f0, newSummary.f1, iterInfo);
                        }
                    }
                })
            .name("update_model");
    }

    @Override
    public BisectingKMeansTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        // get the input parameter's value
        final DistanceType distanceType = DistanceType.valueOf(this.getDistanceType().toUpperCase());
        Preconditions.checkArgument( distanceType == DistanceType.COSINE || distanceType == DistanceType.EUCLIDEAN,
            "distanceType not support!");
        final int k = this.getK();
        final int maxIter = this.getMaxIter();
        final String vectorColName = this.getVectorCol();
        final int minDivisibleClusterSize = this.getMinDivisibleClusterSize();
        ContinuousDistance distance = (FastDistance)distanceType.getContinuousDistance();

        Tuple2<DataSet<Vector>, DataSet<BaseVectorSummary>> vectorsAndStat =
            StatisticsHelper.summaryHelper(in, null, vectorColName);

        DataSet<Integer> dim = vectorsAndStat.f1.map(new MapFunction<BaseVectorSummary, Integer>() {
            @Override
            public Integer map(BaseVectorSummary value) {
                Preconditions.checkArgument(value.count() > 0, "The train dataset is empty!");
                return value.vectorSize();
            }
        });

        // tuple: sampleId, features, assignment
        DataSet<Tuple3<Long, DenseVector, Long>> initialAssignment = DataSetUtils.zipWithUniqueId(vectorsAndStat.f0)
            .map(
                new MapFunction<Tuple2<Long, Vector>, Tuple3<Long, DenseVector, Long>>() {
                    @Override
                    public Tuple3<Long, DenseVector, Long> map(Tuple2<Long, Vector> value) {
                        return Tuple3.of(value.f0, (DenseVector)value.f1, ROOT_INDEX);
                    }
                });

        DataSet<Tuple2<Long, ClusterSummary>> clustersSummaries = summary(
            initialAssignment.project(2, 1), dim, distanceType);

        /**
         * The bisecting kmeans algorithm has nested loops. In the outer loop, cluster centers
         * are splited. In the inner loop, the splited centers are iteratively refined.
         * However, there lacks nested loop semantic in Flink, so we have to flatten the nested loop
         * in our implementation.
         */
        DataSet<Tuple3<Long, ClusterSummary, IterInfo>> clustersSummariesAndIterInfo
            = clustersSummaries
            .map(new MapFunction<Tuple2<Long, ClusterSummary>, Tuple3<Long, ClusterSummary, IterInfo>>() {
                @Override
                public Tuple3<Long, ClusterSummary, IterInfo> map(Tuple2<Long, ClusterSummary> value) {
                    return Tuple3.of(value.f0, value.f1, new IterInfo(maxIter));
                }
            })
            .withForwardedFields("f0;f1");

        IterativeDataSet<Tuple3<Long, ClusterSummary, IterInfo>> loop
            = clustersSummariesAndIterInfo.iterate(Integer.MAX_VALUE);
        DataSet<Tuple1<IterInfo>> iterInfo = loop.<Tuple1<IterInfo>>project(2).first(1);

        /**
         * Get all cluster summaries. Split clusters if at the first step of inner iterations.
         */
        DataSet<Tuple3<Long, ClusterSummary, IterInfo>> allClusters = getOrSplitClusters(loop, k);

        DataSet<Long> divisibleClusterIndices = getDivisibleClusterIndices(allClusters, minDivisibleClusterSize);
        DataSet<Tuple2<Long, DenseVector>> newClusterCenters = getNewClusterCenters(allClusters);

        DataSet<Tuple3<Long, DenseVector, Long>> newAssignment = updateAssignment(
            initialAssignment, divisibleClusterIndices, newClusterCenters, distance, iterInfo);

        DataSet<Tuple2<Long, ClusterSummary>> newClusterSummaries = summary(
            newAssignment.project(2, 1), dim, distanceType);

        DataSet<Tuple3<Long, ClusterSummary, IterInfo>> updatedClusterSummariesWithIterInfo =
            updateClusterSummariesAndIterInfo(allClusters, newClusterSummaries);

        DataSet<Integer> stopCriterion = iterInfo
            .flatMap(new FlatMapFunction<Tuple1<IterInfo>, Integer>() {
                @Override
                public void flatMap(Tuple1<IterInfo> value, Collector<Integer> out) {
                    if (!(value.f0.atLastInnerIterStep() && value.f0.atLastBisectionStep())) {
                        out.collect(0);
                    }
                }
            });

        DataSet<Tuple2<Long, ClusterSummary>> finalClusterSummaries = loop
            .closeWith(updatedClusterSummariesWithIterInfo, stopCriterion)
            .project(0, 1);

        DataSet<Row> modelRows = finalClusterSummaries
            .mapPartition(new RichMapPartitionFunction<Tuple2<Long, ClusterSummary>, Row>() {
                @Override
                public void mapPartition(Iterable<Tuple2<Long, ClusterSummary>> values,
                                         Collector<Row> out) {
                    if (getRuntimeContext().getNumberOfParallelSubtasks() > 1) {
                        throw new RuntimeException("parallelism greater than one when saving model.");
                    }
                    final int dim = (Integer)(getRuntimeContext().getBroadcastVariable(VECTOR_SIZE).get(0));
                    BisectingKMeansModelData modelData = new BisectingKMeansModelData();
                    modelData.summaries = new HashMap<>(0);
                    modelData.vectorSize = dim;
                    modelData.distanceType = distanceType;
                    modelData.vectorColName = vectorColName;
                    modelData.k = k;
                    values.forEach(t -> modelData.summaries.put(t.f0, t.f1));
                    new BisectingKMeansModelDataConverter().save(modelData, out);
                }
            })
            .withBroadcastSet(dim, VECTOR_SIZE)
            .setParallelism(1);

        this.setOutput(modelRows, new BisectingKMeansModelDataConverter().getModelSchema());
        return this;
    }

    public static class ClusterSummaryAggregator implements Serializable {
        /**
         * Cluster sample number.
         */
        private long count;

        /**
         * Sum of cluster sample vector.
         */
        private DenseVector sum;

        /**
         * Sum of Cluster sample vector square.
         */
        private double sumSqured;

        private DistanceType distanceType;

        /**
         * The empty constructor is a 'must' to make it a POJO type.
         */
        ClusterSummaryAggregator() {
        }

        ClusterSummaryAggregator(int dim, DistanceType distanceType) {
            sum = new DenseVector(dim);
            this.distanceType = distanceType;
        }

        public void add(DenseVector v) {
            count++;
            double norm = BLAS.dot(v, v);
            sumSqured += norm;

            switch (distanceType) {
                case EUCLIDEAN: {
                    BLAS.axpy(1., v, sum);
                    break;
                }
                case COSINE: {
                    Preconditions.checkArgument(norm > 0, "Cosine Distance is not defined for zero-length vectors.");
                    BLAS.axpy(1. / Math.sqrt(norm), v, sum);
                    break;
                }
                default: {
                    throw new RuntimeException("distanceType not support:" + distanceType);
                }
            }
        }

        public void merge(ClusterSummaryAggregator other) {
            count += other.count;
            sumSqured += other.sumSqured;
            switch (distanceType) {
                case EUCLIDEAN: {
                    BLAS.axpy(1.0, other.sum, sum);
                    break;
                }
                case COSINE: {
                    double norm = Math.sqrt(BLAS.dot(other.sum, other.sum));
                    Preconditions.checkArgument(norm > 0, "Cosine Distance is not defined for zero-length vectors.");
                    BLAS.axpy(1. / norm, other.sum, sum);
                    break;
                }
                default: {
                    throw new RuntimeException("distanceType not support:" + distanceType);
                }
            }
        }

        public ClusterSummary toClusterSummary() {
            ClusterSummary summary = new ClusterSummary();
            switch (distanceType) {
                case EUCLIDEAN: {
                    summary.center = sum.scale(1.0 / count);
                    break;
                }
                case COSINE: {
                    summary.center = sum.scale(1.0 / count);
                    summary.center.scaleEqual(1.0 / Math.sqrt(BLAS.dot(summary.center, summary.center)));
                    break;
                }
                default: {
                    throw new RuntimeException("distanceType not support:" + distanceType);
                }
            }
            summary.cost = calcClusterCost(distanceType, summary.center, sum, count, sumSqured);
            summary.size = count;
            return summary;
        }

        private static double calcClusterCost(DistanceType distanceType,
                                              DenseVector center,
                                              DenseVector sum,
                                              long count,
                                              double sumSquared) {
            switch (distanceType) {
                case EUCLIDEAN: {
                    double centerL2NormSquared = BLAS.dot(center, center);
                    double cost = sumSquared - count * centerL2NormSquared;
                    return Math.max(cost, 0.);
                }
                case COSINE: {
                    double centerL2Norm = Math.sqrt(BLAS.dot(center, center));
                    return Math.max(count - BLAS.dot(center, sum) / centerL2Norm, 0.0);
                }
                default:
                    throw new RuntimeException("distanceType not support:" + distanceType);
            }
        }
    }

    public static class IterInfo implements Serializable {
        public int bisectingStepNo;
        public int innerIterStepNo;
        // maxIter of inner steps
        public int maxIter;
        public boolean isDividing = false;
        public boolean isNew = false;
        public boolean shouldStopSplit = false;

        // The empty constructor is a 'must' to make it a POJO type.
        public IterInfo() {
        }

        public IterInfo(int maxIter) {
            this.maxIter = maxIter;
            this.bisectingStepNo = 0;
            this.innerIterStepNo = 0;
        }

        public IterInfo(int maxIter, int bisectingStepNo, int innerIterStepNo, boolean isDividing, boolean isNew,
                        boolean shouldStopSplit) {
            this.bisectingStepNo = bisectingStepNo;
            this.innerIterStepNo = innerIterStepNo;
            this.maxIter = maxIter;
            this.isDividing = isDividing;
            this.isNew = isNew;
            this.shouldStopSplit = shouldStopSplit;
        }

        @Override
        public String toString() {
            return JsonConverter.toJson(this);
        }

        public void updateIterInfo() {
            innerIterStepNo++;
            if (innerIterStepNo >= maxIter) {
                bisectingStepNo++;
                innerIterStepNo = 0;
                isDividing = false;
                isNew = false;
            }
        }

        public boolean doBisectionInStep() {
            return innerIterStepNo == 0;
        }

        public boolean atLastInnerIterStep() {
            return innerIterStepNo == maxIter - 1;
        }

        public boolean atLastBisectionStep() {
            return shouldStopSplit;
        }
    }
}
