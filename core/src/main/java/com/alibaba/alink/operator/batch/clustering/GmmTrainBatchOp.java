package com.alibaba.alink.operator.batch.clustering;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.clustering.GmmModelData;
import com.alibaba.alink.operator.common.clustering.GmmModelData.ClusterSummary;
import com.alibaba.alink.operator.common.clustering.GmmModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.MultivariateGaussian;
import com.alibaba.alink.params.clustering.GmmTrainParams;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Gaussian Mixture is a kind of clustering algorithm.
 * <p>
 * Gaussian Mixture clustering performs expectation maximization for multivariate Gaussian
 * Mixture Models (GMMs).  A GMM represents a composite distribution of
 * independent Gaussian distributions with associated "mixing" weights
 * specifying each's contribution to the composite.
 * <p>
 * Given a set of sample points, this class will maximize the log-likelihood
 * for a mixture of k Gaussians, iterating until the log-likelihood changes by
 * less than convergenceTol, or until it has reached the max number of iterations.
 * While this process is generally guaranteed to converge, it is not guaranteed
 * to find a global optimum.
 */
public final class GmmTrainBatchOp extends BatchOperator<GmmTrainBatchOp>
    implements GmmTrainParams<GmmTrainBatchOp> {

    private static final Logger LOG = LoggerFactory.getLogger(GmmTrainBatchOp.class);

    public GmmTrainBatchOp() {
        this(new Params());
    }

    public GmmTrainBatchOp(Params params) {
        super(params);
    }

    /**
     * Initialize the clusters by sampling for each clusters several samples from which the mean and covariance
     * are computed.
     */
    private static DataSet<Tuple3<Integer, ClusterSummary, IterationStatus>>
    initRandom(DataSet<Vector> data, final int numClusters) {
        final int numSamplesPerCluster = 5;
        DataSet<Vector> samples = DataSetUtils.sampleWithSize(data, true,
            numClusters * numSamplesPerCluster);

        return DataSetUtils.zipWithIndex(samples)
            .groupBy(new KeySelector<Tuple2<Long, Vector>, Integer>() {
                @Override
                public Integer getKey(Tuple2<Long, Vector> value) throws Exception {
                    return (int) (value.f0 % numClusters);
                }
            })
            .reduceGroup(
                new GroupReduceFunction<Tuple2<Long, Vector>, Tuple3<Integer, ClusterSummary, IterationStatus>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, Vector>> values,
                                       Collector<Tuple3<Integer, ClusterSummary, IterationStatus>> out)
                        throws Exception {
                        List<Vector> data = new ArrayList<>(numSamplesPerCluster);
                        int clusterId = -1;
                        int featureSize = 0;

                        for (Tuple2<Long, Vector> sample : values) {
                            clusterId = (int) (sample.f0 % numClusters);
                            featureSize = sample.f1.size();
                            data.add(sample.f1);
                        }
                        assert numSamplesPerCluster == data.size();

                        // compute mean
                        DenseVector mean = new DenseVector(featureSize);
                        for (Vector sample : data) {
                            mean.plusEqual(sample);
                        }
                        mean.scaleEqual(1.0 / numSamplesPerCluster);

                        // compute covariance matrix （only keep the diagonal part as initial estimates)
                        DenseVector diagVec = new DenseVector(featureSize);
                        for (Vector sample : data) {
                            Vector shifted = sample.minus(mean);
                            for (int i = 0; i < featureSize; i++) {
                                diagVec.add(i, shifted.get(i) * shifted.get(i));
                            }
                        }
                        diagVec.scaleEqual(1.0 / numSamplesPerCluster);

                        DenseVector covVec = new DenseVector(featureSize * (featureSize + 1) / 2);
                        for (int i = 0; i < featureSize; i++) {
                            int pos = GmmModelData.getElementPositionInCompactMatrix(i, i, featureSize);
                            covVec.set(pos, diagVec.get(i));
                        }

                        ClusterSummary model = new ClusterSummary(clusterId, 1.0 / numClusters, mean, covVec);
                        out.collect(Tuple3.of(clusterId, model, new IterationStatus()));
                    }
                })
            .name("init_model");
    }

    /**
     * Train the Gaussian Mixture model with Expectation-Maximization algorithm.
     */
    @Override
    public GmmTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        final String vectorColName = getVectorCol();
        final int numClusters = getK();
        final int maxIter = getMaxIter();
        final double tol = getTol();

        // Extract the vectors from the input operator.
        Tuple2<DataSet<Vector>, DataSet<BaseVectorSummary>> vectorAndSummary =
            StatisticsHelper.summaryHelper(in, null, vectorColName);

        DataSet<Integer> featureSize = vectorAndSummary.f1
            .map(new MapFunction<BaseVectorSummary, Integer>() {
                @Override
                public Integer map(BaseVectorSummary summary) throws Exception {
                    return summary.vectorSize();
                }
            });

        DataSet<Vector> data = vectorAndSummary.f0
            .map(new RichMapFunction<Vector, Vector>() {
                transient int featureSize;

                @Override
                public void open(Configuration parameters) throws Exception {
                    List<Integer> bc = getRuntimeContext().getBroadcastVariable("featureSize");
                    this.featureSize = bc.get(0);
                }

                @Override
                public Vector map(Vector vec) throws Exception {
                    if (vec instanceof SparseVector) {
                        ((SparseVector) vec).setSize(featureSize);
                    }
                    return vec;
                }
            })
            .withBroadcastSet(featureSize, "featureSize");

        // Initialize the model.
        DataSet<Tuple3<Integer, ClusterSummary, IterationStatus>> initialModel = initRandom(data, numClusters);

        // Iteratively update the model with EM algorithm.
        IterativeDataSet<Tuple3<Integer, ClusterSummary, IterationStatus>> loop = initialModel.iterate(maxIter);

        DataSet<Tuple3<Integer, ClusterSummary, IterationStatus>> updatedModel = data
            .<LocalAggregator>mapPartition(new RichMapPartitionFunction<Vector, LocalAggregator>() {
                transient DenseVector oldWeights;
                transient DenseVector[] oldMeans;
                transient DenseVector[] oldCovs;

                @Override
                public void open(Configuration parameters) throws Exception {
                    oldWeights = new DenseVector(numClusters);
                    oldMeans = new DenseVector[numClusters];
                    oldCovs = new DenseVector[numClusters];
                }

                @Override
                public void mapPartition(Iterable<Vector> values, Collector<LocalAggregator> out)
                    throws Exception {
                    List<Integer> bcNumFeatures = getRuntimeContext().getBroadcastVariable("featureSize");
                    List<Tuple3<Integer, ClusterSummary, IterationStatus>> bcOldModel =
                        getRuntimeContext().getBroadcastVariable("oldModel");
                    double prevLogLikelihood = 0.;

                    for (Tuple3<Integer, ClusterSummary, IterationStatus> t : bcOldModel) {
                        int clusterId = t.f0;
                        ClusterSummary clusterInfo = t.f1;
                        prevLogLikelihood = t.f2.currLogLikelihood;
                        oldWeights.set(clusterId, clusterInfo.weight);
                        oldMeans[clusterId] = clusterInfo.mean;
                        oldCovs[clusterId] = clusterInfo.cov;
                    }

                    LocalAggregator aggregator = new LocalAggregator(numClusters, bcNumFeatures.get(0),
                        prevLogLikelihood, oldWeights, oldMeans, oldCovs);

                    values.forEach(aggregator::add);
                    out.collect(aggregator);
                }
            })
            .withBroadcastSet(featureSize, "featureSize")
            .withBroadcastSet(loop, "oldModel")
            .name("E-M_step")
            .reduce(new ReduceFunction<LocalAggregator>() {
                @Override
                public LocalAggregator reduce(LocalAggregator value1, LocalAggregator value2) throws Exception {
                    return value1.merge(value2);
                }
            })
            .flatMap(
                new RichFlatMapFunction<LocalAggregator, Tuple3<Integer, ClusterSummary, IterationStatus>>() {
                    @Override
                    public void flatMap(LocalAggregator aggregator,
                                        Collector<Tuple3<Integer, ClusterSummary, IterationStatus>> out)
                        throws Exception {
                        for (int i = 0; i < numClusters; i++) {
                            double w = aggregator.updatedWeightsSum.get(i);
                            aggregator.updatedMeansSum[i].scaleEqual(1.0 / w);
                            aggregator.updatedCovsSum[i].scaleEqual(1.0 / w);

                            ClusterSummary model = new ClusterSummary(i, w / aggregator.totalCount,
                                aggregator.updatedMeansSum[i], aggregator.updatedCovsSum[i]);

                            // note that we use Cov(X,Y) = E[XY] - E[X]E[Y] to compute Cov(X,Y)
                            int featureSize = model.mean.size();
                            for (int m = 0; m < featureSize; m++) { // loop over columns
                                for (int n = m; n < featureSize; n++) {
                                    int pos = GmmModelData.getElementPositionInCompactMatrix(m, n, featureSize);
                                    model.cov.add(pos, -1.0 * model.mean.get(m) * model.mean.get(n));
                                }
                            }

                            IterationStatus stat = new IterationStatus();
                            stat.prevLogLikelihood = aggregator.prevLogLikelihood;
                            stat.currLogLikelihood = aggregator.newLogLikelihood;
                            out.collect(Tuple3.of(i, model, stat));
                        }
                    }
                });

        // Check whether stop criterion is met.
        DataSet<Boolean> criterion = updatedModel.first(1)
            .flatMap(new RichFlatMapFunction<Tuple3<Integer, ClusterSummary, IterationStatus>, Boolean>() {
                @Override
                public void flatMap(Tuple3<Integer, ClusterSummary, IterationStatus> value,
                                    Collector<Boolean> out) throws Exception {
                    IterationStatus stat = value.f2;
                    int stepNo = getIterationRuntimeContext().getSuperstepNumber();
                    double diffLogLikelihood = Math.abs(stat.currLogLikelihood - stat.prevLogLikelihood);
                    LOG.info("step {}, prevLogLikelihood {}, currLogLikelihood {}, diffLogLikelihood {}",
                        stepNo, stat.prevLogLikelihood, stat.currLogLikelihood, diffLogLikelihood);
                    if (stepNo <= 1 || diffLogLikelihood > tol) {
                        out.collect(false);
                    }
                }
            });

        DataSet<Tuple3<Integer, ClusterSummary, IterationStatus>> finalModel = loop.closeWith(updatedModel,
            criterion);

        // Output the model.
        DataSet<Row> modelRows = finalModel
            .mapPartition(
                new RichMapPartitionFunction<Tuple3<Integer, ClusterSummary, IterationStatus>, Row>() {
                    transient int featureSize;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.featureSize = (int) (getRuntimeContext().getBroadcastVariable("featureSize").get(0));
                    }

                    @Override
                    public void mapPartition(Iterable<Tuple3<Integer, GmmModelData.ClusterSummary, IterationStatus>> values,
                                             Collector<Row> out) throws Exception {
                        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
                        if (numTasks > 1) {
                            throw new RuntimeException("parallelism is not 1 when saving model.");
                        }
                        GmmModelData model = new GmmModelData();
                        model.k = numClusters;
                        model.dim = featureSize;
                        model.vectorCol = vectorColName;
                        model.data = new ArrayList<>(numClusters);
                        for (Tuple3<Integer, ClusterSummary, IterationStatus> t : values) {
                            t.f1.clusterId = t.f0;
                            model.data.add(t.f1);
                        }
                        new GmmModelDataConverter().save(model, out);
                    }
                })
            .setParallelism(1)
            .withBroadcastSet(featureSize, "featureSize");

        this.setOutput(modelRows, new GmmModelDataConverter().getModelSchema());
        return this;
    }

    private static class IterationStatus implements Serializable {
        double prevLogLikelihood;
        double currLogLikelihood;

        @Override
        public String toString() {
            return String.format("prev:%f,curr:%f", prevLogLikelihood, currLogLikelihood);
        }
    }

    /**
     * The <code>LocalAggregator</code> computes statistics of each cluster with the local
     * partition of sample data.
     */
    private static class LocalAggregator implements Serializable {
        int k;
        int featureSize;
        long totalCount;
        double prevLogLikelihood;
        double newLogLikelihood;
        transient DenseVector oldWeights; // p(z)
        transient DenseVector[] oldMeans;
        transient DenseVector[] oldCovs;
        DenseVector updatedWeightsSum;
        DenseVector[] updatedMeansSum;
        DenseVector[] updatedCovsSum;
        transient MultivariateGaussian[] mnd;
        transient double[] prob; // p(z|x)

        LocalAggregator(int k, int featureSize, double prevLogLikelihood, DenseVector oldWeights,
                        DenseVector[] oldMeans, DenseVector[] oldCovs) {
            this.k = k;
            this.featureSize = featureSize;
            this.oldWeights = oldWeights;
            this.oldMeans = oldMeans;
            this.oldCovs = oldCovs;
            this.prevLogLikelihood = prevLogLikelihood;

            this.totalCount = 0L;
            this.newLogLikelihood = 0.;
            this.updatedWeightsSum = new DenseVector(k);
            this.updatedMeansSum = new DenseVector[k];
            this.updatedCovsSum = new DenseVector[k];

            for (int i = 0; i < k; i++) {
                this.updatedMeansSum[i] = new DenseVector(featureSize);
                this.updatedCovsSum[i] = new DenseVector((featureSize + 1) * featureSize / 2);
            }

            prob = new double[k];
            mnd = new MultivariateGaussian[k];
            for (int i = 0; i < k; i++) {
                DenseVector means = this.oldMeans[i];
                DenseMatrix cov = GmmModelData.expandCovarianceMatrix(this.oldCovs[i], featureSize);
                MultivariateGaussian d = new MultivariateGaussian(means, cov);
                mnd[i] = d;
            }
        }

        public void add(Vector sample) {
            // E-step: compute p(z|x) based on old p(z),mu,sigma, using Bayesian rule
            double probSum = 0.;
            for (int i = 0; i < k; i++) {
                double density = this.mnd[i].pdf(sample);
                double p = this.oldWeights.get(i) * density;
                prob[i] = p;
                probSum += p;
            }

            for (int i = 0; i < k; i++) {
                prob[i] /= probSum;
            }
            this.newLogLikelihood += Math.log(probSum);

            // M-step
            for (int i = 0; i < k; i++) {
                this.updatedWeightsSum.add(i, prob[i]);
                DenseVector localNewMeans = this.updatedMeansSum[i];
                localNewMeans.plusScaleEqual(sample, prob[i]);
                DenseVector localNewCovs = this.updatedCovsSum[i];

                int r = 0;
                for (int m = 0; m < featureSize; m++) {
                    for (int n = 0; n <= m; n++) {
                        localNewCovs.add(r, sample.get(m) * sample.get(n) * prob[i]);
                        r++;
                    }
                }
            }

            this.totalCount++;
        }

        public LocalAggregator merge(LocalAggregator other) {
            this.totalCount += other.totalCount;
            this.updatedWeightsSum.plusEqual(other.updatedWeightsSum);
            for (int i = 0; i < k; i++) {
                this.updatedMeansSum[i].plusEqual(other.updatedMeansSum[i]);
                this.updatedCovsSum[i].plusEqual(other.updatedCovsSum[i]);
            }
            return this;
        }
    }
}
