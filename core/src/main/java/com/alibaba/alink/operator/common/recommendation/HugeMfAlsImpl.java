package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.NormalEquation;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.sql.LeftOuterJoinBatchOp;
import com.alibaba.alink.params.recommendation.AlsImplicitTrainParams;
import com.alibaba.alink.params.recommendation.AlsTrainParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class HugeMfAlsImpl {

	private static final Logger LOG = LoggerFactory.getLogger(HugeMfAlsImpl.class);

	public static Tuple2 <BatchOperator, BatchOperator> factorize(BatchOperator data, Params params, boolean
		implicit) {
		final Long envId = data.getMLEnvironmentId();
		final String userColName = params.get(AlsTrainParams.USER_COL);
		final String itemColName = params.get(AlsTrainParams.ITEM_COL);
		final String rateColName = params.get(AlsTrainParams.RATE_COL);

		final double lambda = params.get(AlsTrainParams.LAMBDA);
		final int rank = params.get(AlsTrainParams.RANK);
		final int numIter = params.get(AlsTrainParams.NUM_ITER);
		final boolean nonNegative = params.get(AlsTrainParams.NON_NEGATIVE);
		final double alpha = params.get(AlsImplicitTrainParams.ALPHA);
		final int numMiniBatches = params.get(AlsTrainParams.NUM_BLOCKS);

		final int userColIdx = TableUtil.findColIndexWithAssert(data.getColNames(), userColName);
		final int itemColIdx = TableUtil.findColIndexWithAssert(data.getColNames(), itemColName);
		final int rateColIdx = TableUtil.findColIndexWithAssert(data.getColNames(), rateColName);

		TypeInformation userType = data.getColTypes()[userColIdx];
		TypeInformation itemType = data.getColTypes()[itemColIdx];
		boolean isLongTypeId = userType.equals(Types.LONG) && itemType.equals(Types.LONG);

		BatchOperator distinctUsers = null;
		BatchOperator distinctItems = null;

		if (!isLongTypeId) {
			distinctUsers = data.select("`" + userColName + "`").distinct();
			distinctItems = data.select("`" + itemColName + "`").distinct();
			MultiStringIndexerTrainBatchOp msi = new MultiStringIndexerTrainBatchOp()
				.setMLEnvironmentId(envId)
				.setSelectedCols(userColName, itemColName).linkFrom(data);
			MultiStringIndexerPredictBatchOp msiPredictor = new MultiStringIndexerPredictBatchOp()
				.setMLEnvironmentId(envId)
				.setSelectedCols(userColName, itemColName);
			data = msiPredictor.linkFrom(msi, data);
			distinctUsers = new MultiStringIndexerPredictBatchOp()
				.setMLEnvironmentId(envId)
				.setSelectedCols(userColName)
				.setOutputCols("__user_index").linkFrom(msi, distinctUsers);
			distinctItems = new MultiStringIndexerPredictBatchOp()
				.setMLEnvironmentId(envId)
				.setSelectedCols(itemColName)
				.setOutputCols("__item_index").linkFrom(msi, distinctItems);
		}

		// tuple3: userId, itemId, rating
		DataSet <Tuple3 <Long, Long, Float>> alsInput = data.getDataSet()
			.map(new MapFunction <Row, Tuple3 <Long, Long, Float>>() {
				private static final long serialVersionUID = 6671683813980584160L;

				@Override
				public Tuple3 <Long, Long, Float> map(Row value) {
					Object user = value.getField(userColIdx);
					Object item = value.getField(itemColIdx);
					Object rating = value.getField(rateColIdx);
					Preconditions.checkNotNull(user, "user is null");
					Preconditions.checkNotNull(item, "item is null");
					Preconditions.checkNotNull(rating, "rating is null");
					return new Tuple3 <>(((Number) user).longValue(),
						((Number) item).longValue(),
						((Number) rating).floatValue());
				}
			});

		AlsTrain als = new AlsTrain(rank, numIter, lambda, implicit, alpha, numMiniBatches, nonNegative);
		DataSet <Tuple3 <Byte, Long, float[]>> factors = als.fit(alsInput);

		BatchOperator userFactors = getFactors(envId, factors, userColName, (byte) 0);
		BatchOperator itemFactors = getFactors(envId, factors, itemColName, (byte) 1);

		if (!isLongTypeId) {
			BatchOperator joinUser = new LeftOuterJoinBatchOp()
				.setMLEnvironmentId(envId)
				.setJoinPredicate(String.format("a.`%s`=b.__user_index", userColName))
				.setSelectClause(String.format("b.`%s`, a.`%s`", userColName, "factors"));
			userFactors = joinUser.linkFrom(userFactors, distinctUsers);
			BatchOperator joinItem = new LeftOuterJoinBatchOp()
				.setMLEnvironmentId(envId)
				.setJoinPredicate(String.format("a.`%s`=b.__item_index", itemColName))
				.setSelectClause(String.format("b.`%s`, a.`%s`", itemColName, "factors"));
			itemFactors = joinItem.linkFrom(itemFactors, distinctItems);
		}

		return Tuple2.of(userFactors, itemFactors);
	}

	private static BatchOperator getFactors(Long envId, DataSet <Tuple3 <Byte, Long, float[]>> factors,
											String name, final byte userOrItem) {
		factors = factors
			.filter(new FilterFunction <Tuple3 <Byte, Long, float[]>>() {
				private static final long serialVersionUID = -2198675502442522328L;

				@Override
				public boolean filter(Tuple3 <Byte, Long, float[]> value) throws Exception {
					return value.f0 == userOrItem;
				}
			});
		DataSet <Row> rows = factors
			.map(new MapFunction <Tuple3 <Byte, Long, float[]>, Row>() {
				private static final long serialVersionUID = 3477932515673402769L;

				@Override
				public Row map(Tuple3 <Byte, Long, float[]> value) throws Exception {
					double[] d = new double[value.f2.length];
					for (int i = 0; i < d.length; i++) {
						d[i] = (double) value.f2[i];
					}
					return Row.of(value.f1, new DenseVector(d).toString());
				}
			});
		Table table = DataSetConversionUtil.toTable(envId, rows, new String[] {name, "factors"},
			new TypeInformation[] {Types.LONG, Types.STRING});
		return BatchOperator.fromTable(table).setMLEnvironmentId(envId);
	}

	/**
	 * The implementation of parallel ALS algorithm. reference: 1. explicit feedback: Large-scale Parallel
	 * Collaborative
	 * Filtering for the Netflix Prize, 2007 2. implicit feedback: Collaborative Filtering for Implicit Feedback
	 * Datasets, 2008
	 */
	public static class AlsTrain {

		final private int numFactors;
		final private int numIters;
		final private double lambda;
		final private boolean implicitPrefs;
		final private double alpha;
		final private int numMiniBatches;
		final private boolean nonnegative;

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
			private static final long serialVersionUID = -5283706915605582930L;
			public byte identity; // 0->user, 1->item
			public long nodeId; // userId or itemId
			public long[] neighbors;
			public float[] ratings;
		}

		/**
		 * Factors of a user or an item.
		 */
		private static class Factors implements Serializable {
			private static final long serialVersionUID = -616590158456104866L;
			public byte identity; // 0->user, 1->item
			public long nodeId;// userId or itemId
			public float[] factors;

			/**
			 * Since we use double precision to solve the least square problem, we need to convert the factors to
			 * double
			 * array.
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
		public DataSet <Tuple3 <Byte, Long, float[]>> fit(DataSet <Tuple3 <Long, Long, Float>> ratings) {
			DataSet <Ratings> graphData = initGraph(ratings);
			DataSet <Factors> factors = initFactors(graphData, numFactors);

			IterativeDataSet <Factors> loop = factors.iterate(Integer.MAX_VALUE);
			Tuple2 <DataSet <Factors>, DataSet <Integer>> factorsAndStopCriterion =
				updateFactors(loop, graphData, numMiniBatches, numFactors, nonnegative, numIters);
			factors = loop.closeWith(factorsAndStopCriterion.f0, factorsAndStopCriterion.f1);

			return factors.map(new MapFunction <Factors, Tuple3 <Byte, Long, float[]>>() {
				private static final long serialVersionUID = -8820639290497738048L;

				@Override
				public Tuple3 <Byte, Long, float[]> map(Factors value) throws Exception {
					return Tuple3.of(value.identity, value.nodeId, value.factors);
				}
			});
		}

		/**
		 * Group users and items ratings from user-item-rating tuples.
		 */
		private DataSet <Ratings>
		initGraph(DataSet <Tuple3 <Long, Long, Float>> ratings) {
			return ratings
				. <Tuple4 <Long, Long, Float, Byte>>flatMap(new FlatMapFunction <Tuple3 <Long, Long, Float>,
					Tuple4 <Long, Long, Float, Byte>>() {
					private static final long serialVersionUID = 3894371804771123007L;

					@Override
					public void flatMap(Tuple3 <Long, Long, Float> value,
										Collector <Tuple4 <Long, Long, Float, Byte>> out) throws Exception {
						out.collect(Tuple4.of(value.f0, value.f1, value.f2, (byte) 0));
						out.collect(Tuple4.of(value.f1, value.f0, value.f2, (byte) 1));
					}
				})
				.groupBy(3, 0)
				.sortGroup(1, Order.ASCENDING)
				.reduceGroup(new GroupReduceFunction <Tuple4 <Long, Long, Float, Byte>, Ratings>() {
					private static final long serialVersionUID = 3391161187867934671L;

					@Override
					public void reduce(Iterable <Tuple4 <Long, Long, Float, Byte>> values, Collector <Ratings> out)
						throws Exception {
						byte identity = -1;
						long srcNodeId = -1L;
						List <Long> neighbors = new ArrayList <>();
						List <Float> ratings = new ArrayList <>();

						for (Tuple4 <Long, Long, Float, Byte> v : values) {
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
		private DataSet <Factors> initFactors(DataSet <Ratings> graph, final int numFactors) {
			return graph
				.map(new RichMapFunction <Ratings, Factors>() {
					private static final long serialVersionUID = -6242580857177532093L;
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

		public static class DataProfile implements Serializable {
			private static final long serialVersionUID = 1976492491732644585L;
			public long parallelism;
			public long numSamples;
			public long numUsers;
			public long numItems;

			public int numUserBatches;
			public int numItemBatches;

			// to make it POJO
			public DataProfile() {
			}

			void decideNumMiniBatches(int numFactors, int parallelism, int minBlocks) {
				this.numUserBatches = decideUserMiniBatches(numSamples, numUsers, numItems, numFactors, parallelism,
					minBlocks);
				this.numItemBatches = decideUserMiniBatches(numSamples, numItems, numUsers, numFactors, parallelism,
					minBlocks);
			}

			static int decideUserMiniBatches(long numSamples, long numUsers, long numItems, int numFactors,
											 int parallelism, int minBlocks) {
				final long TASK_CAPACITY = 2L /* nodes in million */ * 1024 * 1024 * 100 /* rank */;
				long numBatches = 1L;
				if (numItems * numFactors > TASK_CAPACITY) {
					numBatches = numSamples * numFactors / (parallelism * TASK_CAPACITY) + 1;
				}
				numBatches = Math.max(numBatches, minBlocks);
				return (int) numBatches;
			}
		}

		private static DataSet <DataProfile> generateDataProfile(DataSet <Ratings> graphData, final int numFactors,
																 final int minBlocks) {
			return graphData
				. <Tuple3 <Long, Long, Long>>mapPartition(
					new MapPartitionFunction <Ratings, Tuple3 <Long, Long, Long>>() {
						private static final long serialVersionUID = -3529850335007040435L;

						@Override
						public void mapPartition(Iterable <Ratings> values, Collector <Tuple3 <Long, Long, Long>> out)
							throws Exception {
							long numUsers = 0L;
							long numItems = 0L;
							long numRatings = 0L;
							for (Ratings ratings : values) {
								if (ratings.identity == 0) {
									numUsers++;
									numRatings += ratings.ratings.length;
								} else {
									numItems++;
								}
							}
							out.collect(Tuple3.of(numUsers, numItems, numRatings));
						}
					})
				.reduce(new ReduceFunction <Tuple3 <Long, Long, Long>>() {
					private static final long serialVersionUID = 3849683380245684843L;

					@Override
					public Tuple3 <Long, Long, Long> reduce(Tuple3 <Long, Long, Long> value1,
															Tuple3 <Long, Long, Long> value2) throws Exception {
						value1.f0 += value2.f0;
						value1.f1 += value2.f1;
						value1.f2 += value2.f2;
						return value1;
					}
				})
				.map(new RichMapFunction <Tuple3 <Long, Long, Long>, DataProfile>() {
					private static final long serialVersionUID = -2224348217053561771L;

					@Override
					public DataProfile map(Tuple3 <Long, Long, Long> value) throws Exception {
						int parallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
						DataProfile profile = new DataProfile();
						profile.parallelism = parallelism;
						profile.numUsers = value.f0;
						profile.numItems = value.f1;
						profile.numSamples = value.f2;

						profile.decideNumMiniBatches(numFactors, parallelism, minBlocks);
						return profile;
					}
				})
				.name("data_profiling");
		}

		/**
		 * Update user factors or item factors in an iteration step. Only a mini-batch of users' or items' factors are
		 * updated at one step.
		 *
		 * @param userAndItemFactors Users' and items' factors at the beginning of the step.
		 * @param graphData          Users' and items' ratings.
		 * @param minBlocks          Minimum number of mini-batches.
		 * @param numFactors         Number of factors.
		 * @param nonnegative        Whether to enforce non-negativity constraint.
		 * @return Tuple2 of all factors and stop criterion.
		 */
		private Tuple2 <DataSet <Factors>, DataSet <Integer>> updateFactors(
			DataSet <Factors> userAndItemFactors,
			DataSet <Ratings> graphData,
			final int minBlocks,
			final int numFactors,
			final boolean nonnegative,
			final int numIters) {

			// Just to inherit the iteration env
			DataSet <Factors> empty = userAndItemFactors.flatMap(new FlatMapFunction <Factors, Factors>() {
				private static final long serialVersionUID = -4512655206561622474L;

				@Override
				public void flatMap(Factors factors, Collector <Factors> collector) throws Exception {
				}
			});

			// Generate data profile
			DataSet <DataProfile> profile = generateDataProfile(graphData, numFactors, minBlocks);

			// Get the mini-batch
			DataSet <Tuple2 <Integer, Ratings>> miniBatch = graphData
				.filter(new RichFilterFunction <Ratings>() {
					private static final long serialVersionUID = -6221088866110309923L;
					private transient DataProfile profile;
					private transient int alsStepNo;
					private transient int userOrItem;
					private transient int subStepNo;
					private transient int numSubsteps;

					@Override
					public void open(Configuration parameters) throws Exception {
						int stepNo = getIterationRuntimeContext().getSuperstepNumber();
						if (stepNo == 1) {
							profile = (DataProfile) getRuntimeContext().getBroadcastVariable("profile").get(0);
							LOG.info("Data profile {}", JsonConverter.toJson(profile));

							subStepNo = -1;
							userOrItem = 0;
							alsStepNo = 0;
							numSubsteps = profile.numUserBatches;
						}

						subStepNo++;
						if (userOrItem == 0) { // user step
							if (subStepNo >= numSubsteps) {
								userOrItem = 1;
								numSubsteps = profile.numItemBatches;
								subStepNo = 0;
							}
						} else if (userOrItem == 1) { // item step
							if (subStepNo >= numSubsteps) {
								userOrItem = 0;
								numSubsteps = profile.numUserBatches;
								subStepNo = 0;
								alsStepNo++;
							}
						}

						LOG.info("ALS step no {}, user or item {}, sub step no {}", alsStepNo, userOrItem, subStepNo);
					}

					@Override
					public boolean filter(Ratings value) throws Exception {
						return alsStepNo < numIters && value.identity == userOrItem && Math.abs(value.nodeId)
							% numSubsteps == subStepNo;
					}
				})
				.name("createMiniBatch")
				.withBroadcastSet(empty, "empty")
				.withBroadcastSet(profile, "profile")
				.map(new RichMapFunction <Ratings, Tuple2 <Integer, Ratings>>() {
					private static final long serialVersionUID = 2482586233207883428L;
					transient int partitionId;

					@Override
					public void open(Configuration parameters) throws Exception {
						this.partitionId = getRuntimeContext().getIndexOfThisSubtask();
					}

					@Override
					public Tuple2 <Integer, Ratings> map(Ratings value) throws Exception {
						return Tuple2.of(partitionId, value);
					}
				});

			// Generate the request.
			// Tuple: srcPartitionId, targetIdentity, targetNodeId
			DataSet <Tuple3 <Integer, Byte, Long>> request = miniBatch // Tuple: partitionId, ratings
				.flatMap(new FlatMapFunction <Tuple2 <Integer, Ratings>, Tuple3 <Integer, Byte, Long>>() {
					private static final long serialVersionUID = -3507408658975187358L;

					@Override
					public void flatMap(Tuple2 <Integer, Ratings> value,
										Collector <Tuple3 <Integer, Byte, Long>> out) throws Exception {
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
			DataSet <Tuple2 <Integer, Factors>> response
				= request // Tuple: srcPartitionId, targetIdentity, targetNodeId
				.coGroup(userAndItemFactors) // Factors
				.where(new KeySelector <Tuple3 <Integer, Byte, Long>, Tuple2 <Byte, Long>>() {
					private static final long serialVersionUID = 5984785442398189198L;

					@Override
					public Tuple2 <Byte, Long> getKey(Tuple3 <Integer, Byte, Long> value) throws Exception {
						return Tuple2.of(value.f1, value.f2);
					}
				})
				.equalTo(new KeySelector <Factors, Tuple2 <Byte, Long>>() {
					private static final long serialVersionUID = -7009936622357038623L;

					@Override
					public Tuple2 <Byte, Long> getKey(Factors value) throws Exception {
						return Tuple2.of(value.identity, value.nodeId);
					}
				})
				.with(new RichCoGroupFunction <Tuple3 <Integer, Byte, Long>, Factors, Tuple2 <Integer, Factors>>() {
					private static final long serialVersionUID = 7541748515432588189L;
					private transient int[] flag = null;
					private transient int[] partitionsIds = null;

					@Override
					public void open(Configuration parameters) throws Exception {
						int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
						flag = new int[numTasks];
						partitionsIds = new int[numTasks];
					}

					@Override
					public void close() throws Exception {
						flag = null;
						partitionsIds = null;
					}

					@Override
					public void coGroup(Iterable <Tuple3 <Integer, Byte, Long>> request,
										Iterable <Factors> factorsStore,
										Collector <Tuple2 <Integer, Factors>> out) throws Exception {
						if (request == null) {
							return;
						}

						int numRequests = 0;
						byte targetIdentity = -1;
						long targetNodeId = Long.MIN_VALUE;
						int numPartitionsIds = 0;
						Arrays.fill(flag, 0);

						// loop over request: srcBlockId, targetIdentity, targetNodeId
						for (Tuple3 <Integer, Byte, Long> v : request) {
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

			DataSet <Factors> updatedBatchFactors;

			// Calculate factors
			if (implicitPrefs) {
				DataSet <double[]> YtY = computeYtY(userAndItemFactors, numFactors, minBlocks);

				// Tuple: Identity, nodeId, factors
				updatedBatchFactors = miniBatch // Tuple: partitioId, Ratings
					.coGroup(response) // Tuple: partitionId, Factors
					.where(0)
					.equalTo(0)
					.withPartitioner(new Partitioner <Integer>() {
						private static final long serialVersionUID = 2820044599727883648L;

						@Override
						public int partition(Integer key, int numPartitions) {
							return key % numPartitions;
						}
					})
					.with(new UpdateFactorsFunc(false, numFactors, lambda, alpha, nonnegative))
					.withBroadcastSet(YtY, "YtY")
					.name("CalculateNewFactorsImplicit");
			} else {
				// Tuple: Identity, nodeId, factors
				updatedBatchFactors = miniBatch // Tuple: partitioId, Ratings
					.coGroup(response) // Tuple: partitionId, Factors
					.where(0)
					.equalTo(0)
					.withPartitioner(new Partitioner <Integer>() {
						private static final long serialVersionUID = 1421529212117086604L;

						@Override
						public int partition(Integer key, int numPartitions) {
							return key % numPartitions;
						}
					})
					.with(new UpdateFactorsFunc(true, numFactors, lambda, nonnegative))
					.name("CalculateNewFactorsExplicit");
			}

			DataSet <Factors> factors = userAndItemFactors
				.coGroup(updatedBatchFactors)
				.where(new KeySelector <Factors, Tuple2 <Byte, Long>>() {
					private static final long serialVersionUID = -2656531711247296641L;

					@Override
					public Tuple2 <Byte, Long> getKey(Factors value) throws Exception {
						return Tuple2.of(value.identity, value.nodeId);
					}
				})
				.equalTo(new KeySelector <Factors, Tuple2 <Byte, Long>>() {
					private static final long serialVersionUID = -3261052949977562238L;

					@Override
					public Tuple2 <Byte, Long> getKey(Factors value) throws Exception {
						return Tuple2.of(value.identity, value.nodeId);
					}
				})
				.with(new RichCoGroupFunction <Factors, Factors, Factors>() {
					private static final long serialVersionUID = -1806297671515688974L;

					@Override
					public void coGroup(Iterable <Factors> old, Iterable <Factors> updated, Collector <Factors> out)
						throws Exception {

						assert (old != null);
						Iterator <Factors> iterator;

						if (updated == null || !(iterator = updated.iterator()).hasNext()) {
							for (Factors oldFactors : old) {
								out.collect(oldFactors);
							}
						} else {
							Factors newFactors = iterator.next();
							for (Factors oldFactors : old) {
								assert (oldFactors.identity == newFactors.identity
									&& oldFactors.nodeId == newFactors.nodeId);
								out.collect(newFactors);
							}
						}
					}
				})
				.name("UpdateFactors");

			DataSet <Integer> stopCriterion = profile
				.flatMap(new RichFlatMapFunction <DataProfile, Integer>() {
					private static final long serialVersionUID = 378571173957011355L;

					@Override
					public void flatMap(DataProfile pf, Collector <Integer> out) throws Exception {
						int stepNo = getIterationRuntimeContext().getSuperstepNumber();
						if (stepNo < (pf.numUserBatches + pf.numItemBatches) * numIters) {
							out.collect(0);
						}
					}
				})
				.withBroadcastSet(empty, "empty")
				.name("StopCriterion");

			return Tuple2.of(factors, stopCriterion);
		}

		/**
		 * Update users' or items' factors in the local partition, after all depending remote factors have been
		 * collected to the local partition.
		 */
		private static class UpdateFactorsFunc
			extends RichCoGroupFunction <Tuple2 <Integer, Ratings>, Tuple2 <Integer, Factors>, Factors> {
			private static final long serialVersionUID = 4950896077295065254L;
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
			public void coGroup(Iterable <Tuple2 <Integer, Ratings>> rows,
								Iterable <Tuple2 <Integer, Factors>> factors,
								Collector <Factors> out) throws Exception {
				assert (rows != null && factors != null);
				List <Tuple2 <Integer, Factors>> cachedFactors = new ArrayList <>();
				Map <Long, Integer> index2pos = new HashMap <>();

				// loop over received factors
				for (Tuple2 <Integer, Factors> factor : factors) {
					cachedFactors.add(factor);
					index2pos.put(factor.f1.nodeId, (int) numNeighbors);
					numNeighbors++;
				}

				NormalEquation ls = new NormalEquation(numFactors);
				DenseVector x = new DenseVector(numFactors); // the solution buffer
				DenseVector buffer = new DenseVector(numFactors); // buffers for factors

				// loop over local nodes
				for (Tuple2 <Integer, Ratings> row : rows) {
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

		private DataSet <double[]> computeYtY(DataSet <Factors> factors, final int numFactors, final int
			numMiniBatch) {
			return factors
				.mapPartition(new RichMapPartitionFunction <Factors, double[]>() {
					private static final long serialVersionUID = 4337923793883497898L;

					@Override
					public void mapPartition(Iterable <Factors> values, Collector <double[]> out) throws Exception {
						int stepNo = getIterationRuntimeContext().getSuperstepNumber() - 1;
						int identity = (stepNo / numMiniBatch) % 2; // updating 'Identity'
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
				.reduce(new ReduceFunction <double[]>() {
					private static final long serialVersionUID = 3534712378694892154L;

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
}