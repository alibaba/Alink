package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.common.Sample;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.params.clustering.GroupKMeansParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT),
})
@ParamSelectColumnSpec(name = "featureCols", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@ParamSelectColumnSpec(name = "groupCols", portIndices = 0)
@ParamSelectColumnSpec(name = "idCol", portIndices = 0)
@NameCn("分组Kmeans")
@NameEn("Group Kmeans")
public final class GroupKMeansBatchOp extends BatchOperator <GroupKMeansBatchOp>
	implements GroupKMeansParams <GroupKMeansBatchOp> {

	public GroupKMeansBatchOp() {
		super(null);
	}

	public GroupKMeansBatchOp(Params params) {
		super(params);
	}

	@Override
	public GroupKMeansBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String[] featureColNames = this.getFeatureCols();
		final int k = this.getK();
		final double epsilon = this.getEpsilon();
		final int maxIter = this.getMaxIter();
		final DistanceType distanceType = getDistanceType();
		final String[] groupColNames = this.getGroupCols();
		final String idCol = this.getIdCol();
		final ContinuousDistance distance = distanceType.getFastDistance();

		if ((featureColNames == null || featureColNames.length == 0)) {
			throw new RuntimeException("featureColNames should be set !");
		}
		for (String groupColName : groupColNames) {
			if (TableUtil.findColIndex(featureColNames, groupColName) >= 0) {
				throw new RuntimeException("groupColNames should NOT be included in featureColNames!");
			}
		}
		if (null == idCol || "".equals(idCol)) {
			throw new RuntimeException("idCol column should be set!");
		} else if (TableUtil.findColIndex(featureColNames, idCol) >= 0) {
			throw new RuntimeException("idCol column should NOT be included in featureColNames !");
		} else if (TableUtil.findColIndex(groupColNames, idCol) >= 0) {
			throw new RuntimeException("idCol column should NOT be included in groupColNames !");
		}

		final String[] outputCols = ArrayUtils.addAll(groupColNames, idCol, getPredictionCol());
		final TypeInformation <?>[] outputTypes = ArrayUtils.addAll(
			TableUtil.findColTypesWithAssertAndHint(in.getSchema(), groupColNames),
			TableUtil.findColTypeWithAssertAndHint(in.getSchema(), idCol), Types.LONG);

		String[] inputColNames = in.getColNames();
		final int[] groupNameIndices = TableUtil.findColIndices(inputColNames, groupColNames);
		final int idColIndex = TableUtil.findColIndex(inputColNames, idCol);
		final int[] featureColIndex = TableUtil.findColIndices(inputColNames, featureColNames);
		DataSet<Sample> inputSamples = in.getDataSet()
			.map(new MapRowToSample(groupNameIndices, idColIndex, featureColIndex));

		DataSet <Tuple2 <String, Long>> groupsAndSizes = inputSamples.groupBy(new GroupNameKeySelector()).reduceGroup(
			new ComputingGroupSizes());

		final String broadcastGroupSizeKey = "groupAndSizeKey";
		final long partitionInfoHandle = IterTaskObjKeeper.getNewHandle();
		final long cacheDataHandle = IterTaskObjKeeper.getNewHandle();
		final long cacheModelHandle = IterTaskObjKeeper.getNewHandle();
		final long lossHandle = IterTaskObjKeeper.getNewHandle();

		IterativeDataSet <Object> loopStart = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
			.fromParallelCollection(new NumberSequenceIterator(1L, 2L),
				BasicTypeInfo.LONG_TYPE_INFO)
			.map(new MapLongToObject())
			.iterate(maxIter);

		DataSet <Tuple2 <Integer, Sample>> rebalancedSamples = inputSamples.mapPartition(
			new RebalanceDataAndCachePartitionInfo(broadcastGroupSizeKey, partitionInfoHandle))
			.withBroadcastSet(loopStart, "loopStart")
			.withBroadcastSet(groupsAndSizes, broadcastGroupSizeKey)
			.name("rebalanceData");

		DataSet <Tuple3 <Integer, String, double[][]>> initModelWithTargetWorkerIds = rebalancedSamples
			.partitionCustom(
				new HashPartitioner(), 0)
			.mapPartition(new CacheSamplesAndGenInitModel(cacheDataHandle, partitionInfoHandle, k))
			.name("cacheTrainingDataAndGetInitModel");

		DataSet <Object> cachedInitModel = initModelWithTargetWorkerIds.partitionCustom(
			new HashPartitioner(), 0)
			.mapPartition(new CacheInitModel(cacheModelHandle, partitionInfoHandle, lossHandle))
			.name("cacheInitModel");

		DataSet <Tuple3 <Integer, String, double[][]>> modelUpdates = loopStart.mapPartition(
			new ComputeUpdates(cacheDataHandle, cacheModelHandle, partitionInfoHandle, distance))
			.withBroadcastSet(cachedInitModel, "cacheInitModel")
			.name("computeUpdates");

		DataSet <Object> loopOutput = modelUpdates.partitionCustom(
			new HashPartitioner(), 0)
			.mapPartition(new UpdateModel(cacheModelHandle, lossHandle, epsilon))
			.name("updateModel");

		DataSet <Object> loopEnd = loopStart.closeWith(loopOutput, loopOutput);
		DataSet <Sample> transformedDataset = loopEnd.mapPartition(
			new OutputDataSamples(cacheDataHandle, partitionInfoHandle, cacheModelHandle, lossHandle))
			.withBroadcastSet(loopEnd, "iterationEnd")
			.name("outputDataSamples");
		DataSet <Row> rowDataSet = transformedDataset
			.map(new MapSampleToRow(groupColNames.length, outputTypes));
		this.setOutput(rowDataSet, new TableSchema(outputCols, outputTypes));
		return this;
	}


	private static class MapRowToSample implements MapFunction <Row, Sample> {
		private final int[] groupNameIndices;
		private final int idColIndex;
		private final int[] featureColIndices;

		public MapRowToSample(int[] groupNameIndices, int idColIndex, int[] featureColIndices) {
			this.groupNameIndices = groupNameIndices;
			this.idColIndex = idColIndex;
			this.featureColIndices = featureColIndices;
		}

		@Override
		public Sample map(Row row) throws Exception {
			String[] groupColNames = new String[groupNameIndices.length];
			for (int i = 0; i < groupColNames.length; i ++) {
				Object o = row.getField(groupNameIndices[i]);
				Preconditions.checkNotNull(o, "There is NULL value in group col!");
				groupColNames[i] = o.toString();
			}
			double[] values = new double[featureColIndices.length];
			for (int i = 0; i < values.length; i ++) {
				Object o = row.getField(featureColIndices[i]);
				Preconditions.checkNotNull(o, "There is NULL value in feature col!");
				values[i] = ((Number) o).doubleValue();
			}
			Object o = row.getField(idColIndex);
			Preconditions.checkNotNull(o, "There is NULL value in id col!");
			String idColValue = o.toString();
			return new Sample(idColValue, new DenseVector(values), -1, groupColNames);
		}
	}

	private static class GroupNameKeySelector implements KeySelector <Sample, String> {

		@Override
		public String getKey(Sample value) throws Exception {
			return value.getGroupColNamesString();
		}
	}

	/**
	 * Computes number of elements in each group.
	 */
	private static class ComputingGroupSizes implements GroupReduceFunction <Sample, Tuple2 <String, Long>> {

		@Override
		public void reduce(Iterable <Sample> values, Collector <Tuple2 <String, Long>> out) throws Exception {
			String groupName = null;
			long groupSize = 0;

			Iterator <Sample> iterator = values.iterator();
			if (iterator.hasNext()) {
				Sample sample = iterator.next();
				groupName = sample.getGroupColNamesString();
				groupSize++;
			}
			while (iterator.hasNext()) {
				groupSize++;
				iterator.next();
			}
			out.collect(Tuple2.of(groupName, groupSize));
		}
	}

	private static class MapLongToObject implements MapFunction <Long, Object> {
		@Override
		public Object map(Long value) throws Exception {
			return new Object();
		}
	}

	/**
	 * Caches the partition information on each TM at superStep-1 and re-balances data at superStep-2.
	 */
	private static class RebalanceDataAndCachePartitionInfo
		extends RichMapPartitionFunction <Sample, Tuple2 <Integer, Sample>> {

		private final String broadcastGroupSizeKey;

		private final long partitionInfoHandle;

		public RebalanceDataAndCachePartitionInfo(String broadcastGroupSizeKey, long partitionInfoHandle) {
			this.broadcastGroupSizeKey = broadcastGroupSizeKey;
			this.partitionInfoHandle = partitionInfoHandle;
		}

		@Override
		public void mapPartition(Iterable <Sample> values, Collector <Tuple2 <Integer, Sample>> out) throws Exception {
			int superStepNum = getIterationRuntimeContext().getSuperstepNumber();
			if (superStepNum == 1) {
				List <Tuple2 <String, Long>> groupsAndSizes = getRuntimeContext().getBroadcastVariable(
					broadcastGroupSizeKey);
				Map <String, Long> sizeByGroup = new HashMap <>(groupsAndSizes.size());
				for (Tuple2 <String, Long> groupAndSize : groupsAndSizes) {
					sizeByGroup.put(groupAndSize.f0, groupAndSize.f1);
				}
				Map <String, int[]> partitionInfos = getPartitionInfo(sizeByGroup,
					getRuntimeContext().getNumberOfParallelSubtasks());
				IterTaskObjKeeper.put(partitionInfoHandle, getRuntimeContext().getIndexOfThisSubtask(),
					partitionInfos);
			} else if (superStepNum == 2) {
				Map <String, int[]> partitionInfos = IterTaskObjKeeper.get(partitionInfoHandle,
					getRuntimeContext().getIndexOfThisSubtask());
				HashMap <String, Integer> offsetByName = new HashMap <>(partitionInfos.size());
				for (String groupName : partitionInfos.keySet()) {
					offsetByName.put(groupName, -1);
				}
				for (Sample value : values) {
					String groupName = value.getGroupColNamesString();
					int[] possibleWorkerIds = partitionInfos.get(groupName);
					int offset = offsetByName.compute(groupName, (k, v) -> (v + 1) % possibleWorkerIds.length);
					int workerId = possibleWorkerIds[offset];
					out.collect(Tuple2.of(workerId, value));
				}
			}
		}
	}

	/**
	 * Caches samples in static memory and send initModel to corresponding workers at superStep-2.
	 */
	private static class CacheSamplesAndGenInitModel
		extends RichMapPartitionFunction <Tuple2 <Integer, Sample>, Tuple3 <Integer, String, double[][]>> {

		private final long cacheDataHandle;

		private final long partitionInfoHandle;

		private final int numClusters;

		public CacheSamplesAndGenInitModel(long cacheDataHandle, long partitionInfoHandle, int numClusters) {
			this.cacheDataHandle = cacheDataHandle;
			this.partitionInfoHandle = partitionInfoHandle;
			this.numClusters = numClusters;
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Sample>> values,
								 Collector <Tuple3 <Integer, String, double[][]>> out)
			throws Exception {
			int superStep = getIterationRuntimeContext().getSuperstepNumber();
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			if (superStep == 2) {
				// cache training data
				Map <String, int[]> partitionInfos = IterTaskObjKeeper.get(partitionInfoHandle, taskId);
				Preconditions.checkNotNull(partitionInfos);
				List <String> groupNamesToHandle = getGroupNames(partitionInfos, taskId);
				Map <String, ArrayList <Sample>> cachedData = new HashMap <>();
				for (String groupName : groupNamesToHandle) {
					cachedData.put(groupName, new ArrayList <>());
				}

				for (Tuple2 <Integer, Sample> value : values) {
					cachedData.get(value.f1.getGroupColNamesString()).add(value.f1);
				}

				Map <String, Sample[]> cachedDataArray = new HashMap <>();
				for (Map.Entry <String, ArrayList <Sample>> entry : cachedData.entrySet()) {
					cachedDataArray.put(entry.getKey(), entry.getValue().toArray(new Sample[0]));
				}
				IterTaskObjKeeper.put(cacheDataHandle, taskId, cachedDataArray);

				// generate init model and send to corresponding workers.
				// Note: We assume that when one group is partitioned to multiple workers, then the number on one
				// worker is greater than or equal to k.
				for (String groupName : groupNamesToHandle) {
					if (partitionInfos.get(groupName)[0] == taskId) {
						// this worker do the initialization
						Sample[] samples = cachedDataArray.get(groupName);
						int k = Math.min(numClusters, samples.length);

						int dataDim = samples[0].getVector().getData().length;
						double[][] initCenter = new double[k][dataDim];
						for (int i = 0; i < k; i++) {
							System.arraycopy(samples[i].getVector().getData(), 0, initCenter[i], 0, dataDim);
						}
						for (int targetWorkerId : partitionInfos.get(groupName)) {
							out.collect(Tuple3.of(targetWorkerId, groupName, initCenter));
						}
					}
				}
			}
		}
	}

	/**
	 * Caches the init model at superStep-2.
	 */
	private static class CacheInitModel
		extends RichMapPartitionFunction <Tuple3 <Integer, String, double[][]>, Object> {

		private final long cacheModelHandler;

		private final long lossHandler;

		private final long partitionInfoHandle;

		public CacheInitModel(long cacheModelHandler, long partitionInfoHandle, long lossHandler) {
			this.cacheModelHandler = cacheModelHandler;
			this.partitionInfoHandle = partitionInfoHandle;
			this.lossHandler = lossHandler;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, String, double[][]>> values, Collector <Object> out)
			throws Exception {
			int superStep = getIterationRuntimeContext().getSuperstepNumber();
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			if (superStep == 2) {
				Map <String, int[]> groupAndOwners = IterTaskObjKeeper.get(partitionInfoHandle, taskId);
				Preconditions.checkNotNull(groupAndOwners);
				List <String> groupsToHandle = getGroupNames(groupAndOwners, taskId);

				Map <String, double[][]> initModel = new HashMap <>();
				Map <String, Double> loss = new HashMap <>();
				for (Tuple3 <Integer, String, double[][]> val : values) {
					initModel.put(val.f1, val.f2);
					loss.put(val.f1, 0.);
				}

				if (initModel.size() != groupsToHandle.size()) {
					throw new RuntimeException("Illegal model size.");
				}
				IterTaskObjKeeper.put(cacheModelHandler, taskId, initModel);
				IterTaskObjKeeper.put(lossHandler, taskId, loss);
			}
		}
	}

	/**
	 * Computes model updates using the cache data and cache model from superStep-2.
	 */
	private static class ComputeUpdates
		extends RichMapPartitionFunction <Object, Tuple3 <Integer, String, double[][]>> {

		private final long cacheDataHandle;

		private final long cacheModelHandle;

		private final ContinuousDistance distance;

		private final long partitionInfoHandle;

		public ComputeUpdates(long cacheDataHandle, long cacheModelHandle, long partitionInfoHandle,
							  ContinuousDistance distance) {
			this.cacheDataHandle = cacheDataHandle;
			this.cacheModelHandle = cacheModelHandle;
			this.partitionInfoHandle = partitionInfoHandle;
			this.distance = distance;
		}

		@Override
		public void mapPartition(Iterable <Object> values, Collector <Tuple3 <Integer, String, double[][]>> out)
			throws Exception {
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int superStepNum = getIterationRuntimeContext().getSuperstepNumber();
			if (superStepNum == 1) {
				// Does nothing at step 1.
				return;
			}
			Map <String, double[][]> cacheModel = IterTaskObjKeeper.get(cacheModelHandle, taskId);
			Map <String, Sample[]> cacheData = IterTaskObjKeeper.get(cacheDataHandle, taskId);
			Map <String, int[]> partitionInfos = IterTaskObjKeeper.get(partitionInfoHandle, taskId);
			Preconditions.checkNotNull(cacheData);
			Preconditions.checkNotNull(cacheModel);
			Preconditions.checkNotNull(partitionInfos);

			String[] groupsHandled = cacheData.keySet().toArray(new String[0]);

			for (String groupName : groupsHandled) {
				double[][] model = cacheModel.get(groupName);
				Sample[] trainData = cacheData.get(groupName);
				// last two elements are: number of data points, sum distance
				int featureDim = model[0].length;
				double[][] updates = new double[model.length][featureDim + 2];
				for (Sample sample : trainData) {
					Tuple2 <Integer, Double> closestIdAndDistance = findClosestCluster(sample.getVector().getData(),
						model, distance);
					sample.setClusterId(closestIdAndDistance.f0);
					double[] sampleData = sample.getVector().getData();
					for (int i = 0; i < sampleData.length; i++) {
						updates[closestIdAndDistance.f0][i] += sampleData[i];
					}
					// weight
					updates[closestIdAndDistance.f0][featureDim] += 1;
					// distance
					updates[closestIdAndDistance.f0][featureDim + 1] += closestIdAndDistance.f1;
				}
				int[] targetWorkerIds = partitionInfos.get(groupName);
				for (int wId : targetWorkerIds) {
					out.collect(Tuple3.of(wId, groupName, updates));
				}
			}

		}
	}

	/**
	 * Update Kmeans models on each partition. Note that one worker may maintain multiple groups from superStep-2.
	 */
	private static class UpdateModel
		extends RichMapPartitionFunction <Tuple3 <Integer, String, double[][]>, Object> {

		private final long cacheModelHandle;

		private final long lossHandle;

		private final double tol;

		public UpdateModel(long cacheModelHandle, long lossHandle, double tol) {
			this.cacheModelHandle = cacheModelHandle;
			this.lossHandle = lossHandle;
			this.tol = tol;
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Integer, String, double[][]>> values, Collector <Object> out)
			throws Exception {
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			int superStepNum = getIterationRuntimeContext().getSuperstepNumber();
			if (superStepNum == 1) {
				// Does nothing at superStep - 1.
				// Not converged by default.
				out.collect(new Object());
				return;
			}
			Map <String, double[][]> updates = new HashMap <>();
			for (Tuple3 <Integer, String, double[][]> value : values) {
				String groupName = value.f1;
				double[][] updateFromOneWorker = value.f2;
				if (updates.containsKey(groupName)) {
					double[][] accumulatedUpdates = updates.get(groupName);
					for (int i = 0; i < accumulatedUpdates.length; i++) {
						for (int j = 0; j < accumulatedUpdates[0].length; j++) {
							accumulatedUpdates[i][j] += updateFromOneWorker[i][j];
						}
					}
				} else {
					updates.put(groupName, updateFromOneWorker);
				}
			}

			boolean hasConverged = true;
			Map <String, double[][]> cacheModel = IterTaskObjKeeper.get(cacheModelHandle, taskId);
			Map <String, Double> cachedLoss = IterTaskObjKeeper.get(lossHandle, taskId);
			Preconditions.checkNotNull(cachedLoss);
			Preconditions.checkNotNull(cacheModel);

			for (Map.Entry <String, double[][]> entry : updates.entrySet()) {
				String groupName = entry.getKey();
				double[][] accumulatedUpdates = entry.getValue();
				double[][] cachedGroupModel = cacheModel.get(groupName);
				long numElements = 0;
				double distance = 0;
				for (int cId = 0; cId < accumulatedUpdates.length; cId++) {
					double[] currentCluster = accumulatedUpdates[cId];
					numElements += currentCluster[currentCluster.length - 2];
					distance += currentCluster[currentCluster.length - 1];
					for (int i = 0; i < currentCluster.length - 2; i++) {
						currentCluster[i] /= currentCluster[currentCluster.length - 2];
					}
					System.arraycopy(currentCluster, 0, cachedGroupModel[cId], 0, cachedGroupModel[cId].length);
				}

				double lossLastIteration = cachedLoss.get(groupName);
				double currentLoss = distance / numElements;
				cachedLoss.put(groupName, currentLoss);
				if (Math.abs(currentLoss - lossLastIteration) > tol) {
					hasConverged = false;
				}
			}
			if (!hasConverged) {
				out.collect(new Object());
			}
		}
	}

	/**
	 * Output data samples cached in memory and clear the all objects cached in static memory.
	 */
	private static class OutputDataSamples extends RichMapPartitionFunction <Object, Sample> {

		private final long cacheDataHandle;

		private final long partitionInfoHandle;

		private final long cacheModelHandle;

		private final long lossHandle;

		public OutputDataSamples(long cacheDataHandle, long partitionInfoHandle, long cacheModelHandle,
								 long lossHandle) {
			this.cacheDataHandle = cacheDataHandle;
			this.partitionInfoHandle = partitionInfoHandle;
			this.cacheModelHandle = cacheModelHandle;
			this.lossHandle = lossHandle;
		}

		@Override
		public void mapPartition(Iterable <Object> values, Collector <Sample> out) throws Exception {
			int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
			Map <String, Sample[]> cachedData = null;
			for (int i = 0; i < numTasks; i++) {
				cachedData = IterTaskObjKeeper.containsAndRemoves(cacheDataHandle, i);
				if (cachedData != null) {
					break;
				}
			}
			Preconditions.checkNotNull(cachedData);
			for (Sample[] oneGroupData : cachedData.values()) {
				for (Sample sample : oneGroupData) {
					out.collect(sample);
				}
			}
			for (int i = 0; i < numTasks; i++) {
				IterTaskObjKeeper.remove(cacheModelHandle, i);
				IterTaskObjKeeper.remove(partitionInfoHandle, i);
				IterTaskObjKeeper.remove(lossHandle, i);
			}
		}
	}

	/**
	 * Finds the closest cluster.
	 *
	 * @param dataPoint The input data point.
	 * @param centroids The centroids.
	 * @param distance  The distance measure.
	 * @return The closest cluster Id and the corresponding distance.
	 */
	private static Tuple2 <Integer, Double> findClosestCluster(double[] dataPoint, double[][] centroids,
															   ContinuousDistance distance) {
		double minDistance = Double.MAX_VALUE;
		int closestClusterId = Integer.MAX_VALUE;
		for (int i = 0; i < centroids.length; i++) {
			double tmpDistance = distance.calc(dataPoint, centroids[i]);
			if (tmpDistance < minDistance) {
				minDistance = tmpDistance;
				closestClusterId = i;
			}
		}
		return Tuple2.of(closestClusterId, minDistance);
	}

	/**
	 * Gets the groups that this worker needs to handle.
	 *
	 * @param partitionInfos GroupId and the workerIds that needs to handle group `groupId`.
	 * @param workerId       The worker id.
	 * @return The groups that this worker needs to handle.
	 */
	private static List <String> getGroupNames(Map <String, int[]> partitionInfos, int workerId) {
		List <String> res = new ArrayList <>();
		for (Map.Entry <String, int[]> entry : partitionInfos.entrySet()) {
			for (int idx : entry.getValue()) {
				if (idx == workerId) {
					res.add(entry.getKey());
					break;
				}
			}
		}
		return res;
	}

	/**
	 * Computes the owner of each group of training data, i.e., should it be handled by one worker or multiple workers.
	 * Note that this result of this function should be deterministic and unique given the same input. Because each
	 * worker executes this function individually.
	 *
	 * @param sizeByGroupName Number of data points in each group.
	 * @param numWorkers      Number of workers.
	 * @return The owner of each group.
	 */
	@VisibleForTesting
	static Map <String, int[]> getPartitionInfo(Map <String, Long> sizeByGroupName, int numWorkers) {
		// workerId, number of elements assigned to this worker.
		PriorityQueue <Tuple2 <Integer, Long>> workerAndAssignedNumElements =
			new PriorityQueue <>(
				(o1, o2) -> Long.compare(o1.f1, o2.f1) == 0 ? Integer.compare(o1.f0, o2.f0)
					: Long.compare(o1.f1, o2.f1));
		for (int i = 0; i < numWorkers; i++) {
			workerAndAssignedNumElements.add(Tuple2.of(i, 0L));
		}

		// converts to tuple array
		Tuple2 <String, Long>[] sizeAndGroupNameArray = new Tuple2[sizeByGroupName.size()];
		int idx = 0;
		for (Map.Entry <String, Long> entry : sizeByGroupName.entrySet()) {
			sizeAndGroupNameArray[idx] = Tuple2.of(entry.getKey(), entry.getValue());
			idx++;
		}
		Arrays.sort(sizeAndGroupNameArray,
			(o1, o2) -> -Long.compare(o1.f1, o2.f1) == 0 ? o1.f0.compareTo(o2.f0) : -Long.compare(o1.f1, o2.f1));

		// stores the maintained result
		Map <String, List <Integer>> groupAndWorkerIds = new HashMap <>(sizeAndGroupNameArray.length);
		long averageNumElementsPerWorker = 0;
		for (Tuple2 <String, Long> groupAndSize : sizeAndGroupNameArray) {
			groupAndWorkerIds.put(groupAndSize.f0, new ArrayList <>());
			averageNumElementsPerWorker += groupAndSize.f1;
		}
		averageNumElementsPerWorker = averageNumElementsPerWorker / numWorkers + 1;

		for (Tuple2 <String, Long> stringLongTuple2 : sizeAndGroupNameArray) {
			String groupName = stringLongTuple2.f0;
			long numElementsInThisGroup = stringLongTuple2.f1;
			// splits large groups.
			long numWorkersNeeded = numElementsInThisGroup / averageNumElementsPerWorker;
			// does not split small groups.
			if (numWorkersNeeded == 0) {
				numWorkersNeeded = 1;
			}
			long numElementsInEachWorker = numElementsInThisGroup / numWorkersNeeded;
			for (int splitId = 0; splitId < numWorkersNeeded; splitId++) {
				Tuple2 <Integer, Long> smallestWorker = workerAndAssignedNumElements.remove();
				groupAndWorkerIds.get(groupName).add(smallestWorker.f0);
				workerAndAssignedNumElements.add(
					Tuple2.of(smallestWorker.f0, numElementsInEachWorker + smallestWorker.f1));
			}
		}

		Map <String, int[]> result = new HashMap <>();
		for (Map.Entry <String, List <Integer>> entry : groupAndWorkerIds.entrySet()) {
			int[] targetWorkerIds = entry.getValue().stream().mapToInt(Integer::intValue).toArray();
			Arrays.sort(targetWorkerIds);
			result.put(entry.getKey(), targetWorkerIds);
		}
		return result;
	}

	private static class MapSampleToRow implements MapFunction <Sample, Row> {
		private final int groupColNamesSize;
		private final TypeInformation <?>[] outputTypes;

		public MapSampleToRow(int groupColNamesSize, TypeInformation <?>[] outputTypes) {
			this.groupColNamesSize = groupColNamesSize;
			this.outputTypes = outputTypes;
		}

		@Override
		public Row map(Sample value) throws Exception {
			Row row = new Row(groupColNamesSize + 2);
			for (int i = 0; i < groupColNamesSize; i++) {
				row.setField(i, EvaluationUtil.castTo(value.getGroupColNames()[i], outputTypes[i]));
			}
			row.setField(groupColNamesSize, EvaluationUtil.castTo(value.getSampleId(),
				outputTypes[groupColNamesSize]));
			row.setField(groupColNamesSize + 1, value.getClusterId());
			return row;
		}
	}

	private static class HashPartitioner implements Partitioner <Integer> {

		@Override
		public int partition(Integer key, int numPartitions) {
			return key % numPartitions;
		}
	}

}
