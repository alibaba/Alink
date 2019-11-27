package com.alibaba.alink.operator.common.recommendation;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.NormalEquation;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * The implementation of parallel ALS algorithm.
 * reference:
 * 1. explicit feedback: Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007
 * 2. implicit feedback: Collaborative Filtering for Implicit Feedback Datasets, 2008
 */
public class AlsTrain {

    final private int numFactors;
    final private int numIters;
    final private double lambda;
    final private boolean implicitPrefs;
    final private double alpha;
    final private int numMiniBatches;
    final private boolean nonnegative;

    private static final Logger LOG = LoggerFactory.getLogger(AlsTrain.class);

    /**
     * The constructor.
     *
     * @param numFactors     Number of factors.
     * @param numIters       Number of iterations.
     * @param lambda         The regularization term.
     * @param implicitPrefs  Flag indicating whether to use implicit feedback model.
     * @param alpha          The implicit feedback param.
     * @param numMiniBatches Number of mini-batches.
     * @param nonNegative    Whether to enforce non-negativity constraint.
     */
    public AlsTrain(int numFactors, int numIters, double lambda,
                    boolean implicitPrefs, double alpha, int numMiniBatches, boolean nonNegative) {
        this.numFactors = numFactors;
        this.numIters = numIters;
        this.lambda = lambda;
        this.implicitPrefs = implicitPrefs;
        this.alpha = alpha;
        this.numMiniBatches = numMiniBatches;
        this.nonnegative = nonNegative;
    }

    /**
     * All ratings of a user or an item.
     */
    private static class Ratings implements Serializable {
        public byte identity; // 0->user, 1->item
        public long nodeId; // userId or itemId
        public long[] neighbors;
        public float[] ratings;
    }

    /**
     * Factors of a user or an item.
     */
    private static class Factors implements Serializable {
        public byte identity; // 0->user, 1->item
        public long nodeId;// userId or itemId
        public float[] factors;

        /**
         * Since we use double precision to solve the least square problem, we need
         * to convert the factors to double array.
         */
        void getFactorsAsDoubleArray(double[] buffer) {
            for (int i = 0; i < factors.length; i++) {
                buffer[i] = factors[i];
            }
        }

        void copyFactorsFromDoubleArray(double[] buffer) {
            if (factors == null) {
                factors = new float[buffer.length];
            }
            for (int i = 0; i < buffer.length; i++) {
                factors[i] = (float) buffer[i];
            }
        }
    }

    /**
     * Calculate users' and items' factors.
     *
     * @param ratings a dataset of user-item-rating tuples
     * @return a dataset of user factors and item factors
     */
    public DataSet<Tuple3<Byte, Long, float[]>> fit(DataSet<Tuple3<Long, Long, Float>> ratings) {
        final int numTasks = ratings.getExecutionEnvironment().getParallelism();
        DataSet<Ratings> graphData = initGraph(ratings);
        DataSet<Factors> factors = initFactors(graphData, numFactors);

        // Iteratively update factors. In each iteration, we update a 'mini-batch' of nodes,
        // so there should be "numIter * numMiniBatch * 2" iterations.
        int nit = numIters * numMiniBatches * 2;
        IterativeDataSet<Factors> loop = factors.iterate(nit);
        DataSet<Factors> updatedFactors = updateFactors(loop, graphData, numTasks, numMiniBatches, numFactors, nonnegative);
        factors = loop.closeWith(updatedFactors);

        return factors.map(new MapFunction<Factors, Tuple3<Byte, Long, float[]>>() {
            @Override
            public Tuple3<Byte, Long, float[]> map(Factors value) throws Exception {
                return Tuple3.of(value.identity, value.nodeId, value.factors);
            }
        });
    }

    /**
     * Group users and items ratings from user-item-rating tuples.
     */
    private DataSet<Ratings>
    initGraph(DataSet<Tuple3<Long, Long, Float>> ratings) {
        return ratings
            .<Tuple4<Long, Long, Float, Byte>>flatMap(new FlatMapFunction<Tuple3<Long, Long, Float>,
                Tuple4<Long, Long, Float, Byte>>() {
                @Override
                public void flatMap(Tuple3<Long, Long, Float> value, Collector<Tuple4<Long, Long, Float, Byte>> out) throws Exception {
                    out.collect(Tuple4.of(value.f0, value.f1, value.f2, (byte) 0));
                    out.collect(Tuple4.of(value.f1, value.f0, value.f2, (byte) 1));
                }
            })
            .groupBy(3, 0)
            .sortGroup(1, Order.ASCENDING)
            .reduceGroup(new GroupReduceFunction<Tuple4<Long, Long, Float, Byte>, Ratings>() {
                @Override
                public void reduce(Iterable<Tuple4<Long, Long, Float, Byte>> values, Collector<Ratings> out) throws Exception {
                    byte identity = -1;
                    long srcNodeId = -1L;
                    List<Long> neighbors = new ArrayList<>();
                    List<Float> ratings = new ArrayList<>();

                    for (Tuple4<Long, Long, Float, Byte> v : values) {
                        identity = v.f3;
                        srcNodeId = v.f0;
                        neighbors.add(v.f1);
                        ratings.add(v.f2);
                    }

                    Ratings r = new Ratings();
                    r.nodeId = srcNodeId;
                    r.identity = identity;
                    r.neighbors = new long[neighbors.size()];
                    r.ratings = new float[neighbors.size()];

                    for (int i = 0; i < r.neighbors.length; i++) {
                        r.neighbors[i] = neighbors.get(i);
                        r.ratings[i] = ratings.get(i);
                    }

                    out.collect(r);
                }
            })
            .name("init_graph");
    }

    /**
     * Initialize user factors and item factors randomly.
     *
     * @param graph      Users' and items' ratings.
     * @param numFactors Number of factors.
     * @return Randomly initialized users' and items' factors.
     */
    private DataSet<Factors> initFactors(DataSet<Ratings> graph, final int numFactors) {
        return graph
            .map(new RichMapFunction<Ratings, Factors>() {
                transient Random random;
                transient Factors reusedFactors;

                @Override
                public void open(Configuration parameters) throws Exception {
                    random = new Random(getRuntimeContext().getIndexOfThisSubtask());
                    reusedFactors = new Factors();
                    reusedFactors.factors = new float[numFactors];
                }

                @Override
                public Factors map(Ratings value) throws Exception {
                    reusedFactors.identity = value.identity;
                    reusedFactors.nodeId = value.nodeId;
                    for (int i = 0; i < numFactors; i++) {
                        reusedFactors.factors[i] = random.nextFloat();
                    }
                    return reusedFactors;
                }
            })
            .name("InitFactors");
    }

    /**
     * Update user factors or item factors in an iteration step. Only a mini-batch of users' or items'
     * factors are updated at one step.
     *
     * @param userAndItemFactors Users' and items' factors at the beginning of the step.
     * @param graphData          Users' and items' ratings.
     * @param numTasks           Number of tasks.
     * @param numMiniBatch       Number of mini-batches.
     * @param numFactors         Number of factors.
     * @param nonnegative        Whether to enforce non-negativity constraint.
     * @return Updated users' and items' factors.
     */
    private DataSet<Factors> updateFactors(
        DataSet<Factors> userAndItemFactors,
        DataSet<Ratings> graphData,
        final int numTasks,
        final int numMiniBatch,
        final int numFactors,
        final boolean nonnegative) {

        // Just to inherit the iteration env
        DataSet<Factors> empty = userAndItemFactors.mapPartition(new MapPartitionFunction<Factors, Factors>() {
            @Override
            public void mapPartition(Iterable<Factors> values, Collector<Factors> out) throws Exception {
            }
        });

        // Get the mini-batch
        DataSet<Tuple2<Integer, Ratings>> miniBatch = graphData
            .filter(new RichFilterFunction<Ratings>() {
                private transient int stepNo;
                private transient int userOrItem;
                private transient int batchId;

                @Override
                public void open(Configuration parameters) throws Exception {
                    stepNo = getIterationRuntimeContext().getSuperstepNumber() - 1;
                    userOrItem = (stepNo / numMiniBatch) % 2;
                    batchId = stepNo % numMiniBatch;
                }

                @Override
                public boolean filter(Ratings value) throws Exception {
                    return value.identity == userOrItem && Math.abs(value.nodeId) % numMiniBatch == batchId;
                }
            })
            .name("createMiniBatch")
            .withBroadcastSet(empty, "empty")
            .map(new RichMapFunction<Ratings, Tuple2<Integer, Ratings>>() {
                transient int partitionId;

                @Override
                public void open(Configuration parameters) throws Exception {
                    this.partitionId = getRuntimeContext().getIndexOfThisSubtask();
                }

                @Override
                public Tuple2<Integer, Ratings> map(Ratings value) throws Exception {
                    return Tuple2.of(partitionId, value);
                }
            });

        // Generate the request.
        // Tuple: srcPartitionId, targetIdentity, targetNodeId
        DataSet<Tuple3<Integer, Byte, Long>> request = miniBatch // Tuple: partitionId, ratings
            .flatMap(new FlatMapFunction<Tuple2<Integer, Ratings>, Tuple3<Integer, Byte, Long>>() {
                @Override
                public void flatMap(Tuple2<Integer, Ratings> value,
                                    Collector<Tuple3<Integer, Byte, Long>> out) throws Exception {
                    int targetIdentity = 1 - value.f1.identity;
                    int srcPartitionId = value.f0;
                    long[] neighbors = value.f1.neighbors;
                    for (long neighbor : neighbors) {
                        out.collect(Tuple3.of(srcPartitionId, (byte) targetIdentity, neighbor));
                    }
                }
            })
            .name("GenerateRequest");

        // Generate the response
        // Tuple: srcPartitionId, targetFactors
        DataSet<Tuple2<Integer, Factors>> response = request // Tuple: srcPartitionId, targetIdentity, targetNodeId
            .coGroup(userAndItemFactors) // Factors
            .where(new KeySelector<Tuple3<Integer, Byte, Long>, Tuple2<Byte, Long>>() {
                @Override
                public Tuple2<Byte, Long> getKey(Tuple3<Integer, Byte, Long> value) throws Exception {
                    return Tuple2.of(value.f1, value.f2);
                }
            })
            .equalTo(new KeySelector<Factors, Tuple2<Byte, Long>>() {
                @Override
                public Tuple2<Byte, Long> getKey(Factors value) throws Exception {
                    return Tuple2.of(value.identity, value.nodeId);
                }
            })
            .with(new RichCoGroupFunction<Tuple3<Integer, Byte, Long>, Factors, Tuple2<Integer, Factors>>() {
                private transient int[] flag = null;
                private transient int[] partitionsIds = null;

                @Override
                public void open(Configuration parameters) throws Exception {
                    flag = new int[numTasks];
                    partitionsIds = new int[numTasks];
                }

                @Override
                public void close() throws Exception {
                    flag = null;
                    partitionsIds = null;
                }

                @Override
                public void coGroup(Iterable<Tuple3<Integer, Byte, Long>> request, Iterable<Factors> factorsStore,
                                    Collector<Tuple2<Integer, Factors>> out) throws Exception {
                    if (request == null) {
                        return;
                    }

                    int numRequests = 0;
                    byte targetIdentity = -1;
                    long targetNodeId = Long.MIN_VALUE;
                    int numPartitionsIds = 0;
                    Arrays.fill(flag, 0);

                    // loop over request: srcBlockId, targetIdentity, targetNodeId
                    for (Tuple3<Integer, Byte, Long> v : request) {
                        numRequests++;
                        targetIdentity = v.f1;
                        targetNodeId = v.f2;
                        int partId = v.f0;
                        if (flag[partId] == 0) {
                            partitionsIds[numPartitionsIds++] = partId;
                            flag[partId] = 1;
                        }
                    }

                    if (numRequests == 0) {
                        return;
                    }

                    for (Factors factors : factorsStore) {
                        assert (factors.identity == targetIdentity && factors.nodeId == targetNodeId);
                        for (int i = 0; i < numPartitionsIds; i++) {
                            int b = partitionsIds[i];
                            out.collect(Tuple2.of(b, factors));
                        }
                    }
                }
            })
            .name("GenerateResponse");

        DataSet<Factors> updatedBatchFactors;

        // Calculate factors
        if (implicitPrefs) {
            DataSet<double[]> YtY = computeYtY(userAndItemFactors, numFactors, numMiniBatch);

            // Tuple: identity, nodeId, factors
            updatedBatchFactors = miniBatch // Tuple: partitioId, Ratings
                .coGroup(response) // Tuple: partitionId, Factors
                .where(0)
                .equalTo(0)
                .withPartitioner(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                    }
                })
                .with(new UpdateFactorsFunc(false, numFactors, lambda, alpha, nonnegative))
                .withBroadcastSet(YtY, "YtY")
                .name("CalculateNewFactorsImplicit");
        } else {
            // Tuple: identity, nodeId, factors
            updatedBatchFactors = miniBatch // Tuple: partitioId, Ratings
                .coGroup(response) // Tuple: partitionId, Factors
                .where(0)
                .equalTo(0)
                .withPartitioner(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                    }
                })
                .with(new UpdateFactorsFunc(true, numFactors, lambda, nonnegative))
                .name("CalculateNewFactorsExplicit");
        }

        return userAndItemFactors
            .coGroup(updatedBatchFactors)
            .where(new KeySelector<Factors, Tuple2<Byte, Long>>() {
                @Override
                public Tuple2<Byte, Long> getKey(Factors value) throws Exception {
                    return Tuple2.of(value.identity, value.nodeId);
                }
            })
            .equalTo(new KeySelector<Factors, Tuple2<Byte, Long>>() {
                @Override
                public Tuple2<Byte, Long> getKey(Factors value) throws Exception {
                    return Tuple2.of(value.identity, value.nodeId);
                }
            })
            .with(new RichCoGroupFunction<Factors, Factors, Factors>() {
                @Override
                public void coGroup(Iterable<Factors> old, Iterable<Factors> updated, Collector<Factors> out) throws Exception {

                    assert (old != null);
                    Iterator<Factors> iterator;

                    if (updated == null || !(iterator = updated.iterator()).hasNext()) {
                        for (Factors oldFactors : old) {
                            out.collect(oldFactors);
                        }
                    } else {
                        Factors newFactors = iterator.next();
                        for (Factors oldFactors : old) {
                            assert (oldFactors.identity == newFactors.identity && oldFactors.nodeId == newFactors.nodeId);
                            out.collect(newFactors);
                        }
                    }
                }
            })
            .name("UpdateFactors");
    }

    /**
     * Update users' or items' factors in the local partition, after all depending remote factors
     * have been collected to the local partition.
     */
    private static class UpdateFactorsFunc extends RichCoGroupFunction<Tuple2<Integer, Ratings>, Tuple2<Integer, Factors>, Factors> {
        final int numFactors;
        final double lambda;
        final double alpha;
        final boolean explicit;
        final boolean nonnegative;

        private int numNodes = 0;
        private long numEdges = 0L;
        private long numNeighbors = 0L;

        private transient double[] YtY = null;

        UpdateFactorsFunc(boolean explicit, int numFactors, double lambda, boolean nonnegative) {
            this.explicit = explicit;
            this.numFactors = numFactors;
            this.lambda = lambda;
            this.alpha = 0.;
            this.nonnegative = nonnegative;
        }

        UpdateFactorsFunc(boolean explicit, int numFactors, double lambda, double alpha, boolean nonnegative) {
            this.explicit = explicit;
            this.numFactors = numFactors;
            this.lambda = lambda;
            this.alpha = alpha;
            this.nonnegative = nonnegative;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            numNodes = 0;
            numEdges = 0;
            numNeighbors = 0L;
            if (!explicit) {
                this.YtY = (double[]) (getRuntimeContext().getBroadcastVariable("YtY").get(0));
            }
        }

        @Override
        public void close() throws Exception {
            LOG.info("Updated factors, num nodes {}, num edges {}, recv neighbors {}",
                numNodes, numEdges, numNeighbors);
        }

        @Override
        public void coGroup(Iterable<Tuple2<Integer, Ratings>> rows,
                            Iterable<Tuple2<Integer, Factors>> factors,
                            Collector<Factors> out) throws Exception {
            assert (rows != null && factors != null);
            List<Tuple2<Integer, Factors>> cachedFactors = new ArrayList<>();
            Map<Long, Integer> index2pos = new HashMap<>();

            // loop over received factors
            for (Tuple2<Integer, Factors> factor : factors) {
                cachedFactors.add(factor);
                index2pos.put(factor.f1.nodeId, (int) numNeighbors);
                numNeighbors++;
            }

            NormalEquation ls = new NormalEquation(numFactors);
            DenseVector x = new DenseVector(numFactors); // the solution buffer
            DenseVector buffer = new DenseVector(numFactors); // buffers for factors

            // loop over local nodes
            for (Tuple2<Integer, Ratings> row : rows) {
                numNodes++;
                numEdges += row.f1.neighbors.length;

                // solve an lease square problem
                ls.reset();

                if (explicit) {
                    long[] nb = row.f1.neighbors;
                    float[] rating = row.f1.ratings;
                    for (int i = 0; i < nb.length; i++) {
                        long index = nb[i];
                        Integer pos = index2pos.get(index);
                        cachedFactors.get(pos).f1.getFactorsAsDoubleArray(buffer.getData());
                        ls.add(buffer, rating[i], 1.0);
                    }
                    ls.regularize(nb.length * lambda);
                    ls.solve(x, nonnegative);
                } else {
                    ls.merge(new DenseMatrix(numFactors, numFactors, YtY)); // put the YtY

                    int numExplicit = 0;
                    long[] nb = row.f1.neighbors;
                    float[] rating = row.f1.ratings;
                    for (int i = 0; i < nb.length; i++) {
                        long index = nb[i];
                        Integer pos = index2pos.get(index);
                        float r = rating[i];
                        double c1 = 0.;
                        if (r > 0) {
                            numExplicit++;
                            c1 = alpha * r;
                        }
                        cachedFactors.get(pos).f1.getFactorsAsDoubleArray(buffer.getData());
                        ls.add(buffer, ((r > 0.0) ? (1.0 + c1) : 0.0), c1);
                    }
                    ls.regularize(numExplicit * lambda);
                    ls.solve(x, nonnegative);
                }

                Factors updated = new Factors();
                updated.identity = row.f1.identity;
                updated.nodeId = row.f1.nodeId;
                updated.copyFactorsFromDoubleArray(x.getData());
                out.collect(updated);
            }
        }
    }

    private DataSet<double[]> computeYtY(DataSet<Factors> factors, final int numFactors, final int numMiniBatch) {
        return factors
            .mapPartition(new RichMapPartitionFunction<Factors, double[]>() {
                @Override
                public void mapPartition(Iterable<Factors> values, Collector<double[]> out) throws Exception {
                    int stepNo = getIterationRuntimeContext().getSuperstepNumber() - 1;
                    int identity = (stepNo / numMiniBatch) % 2; // updating 'identity'
                    int dst = 1 - identity;

                    double[] blockYtY = new double[numFactors * numFactors];
                    Arrays.fill(blockYtY, 0.);

                    for (Factors v : values) {
                        if (v.identity != dst) {
                            continue;
                        }

                        float[] factors1 = v.factors;
                        for (int i = 0; i < numFactors; i++) {
                            for (int j = 0; j < numFactors; j++) {
                                blockYtY[i * numFactors + j] += factors1[i] * factors1[j];
                            }
                        }
                    }
                    out.collect(blockYtY);
                }
            })
            .reduce(new ReduceFunction<double[]>() {
                @Override
                public double[] reduce(double[] value1, double[] value2) throws Exception {
                    int n2 = numFactors * numFactors;
                    double[] sum = new double[n2];
                    for (int i = 0; i < n2; i++) {
                        sum[i] = value1[i] + value2[i];
                    }
                    return sum;
                }
            })
            .name("YtY");
    }
}