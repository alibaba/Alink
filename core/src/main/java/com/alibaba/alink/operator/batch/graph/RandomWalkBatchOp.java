package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.storage.GraphEdge;
import com.alibaba.alink.operator.batch.graph.storage.HomoGraphEngine;
import com.alibaba.alink.operator.batch.graph.utils.ConstructHomoEdge;
import com.alibaba.alink.operator.batch.graph.utils.EndWritingRandomWalks;
import com.alibaba.alink.operator.batch.graph.utils.IDMappingUtils;
import com.alibaba.alink.operator.batch.graph.utils.LongArrayToRow;
import com.alibaba.alink.operator.batch.graph.utils.ParseGraphData;
import com.alibaba.alink.operator.batch.graph.utils.ComputeGraphStatistics;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionFunction;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionHashFunction;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitioner;
import com.alibaba.alink.operator.batch.graph.utils.GraphStatistics;
import com.alibaba.alink.operator.batch.graph.utils.RandomWalkMemoryBuffer;
import com.alibaba.alink.operator.batch.graph.utils.ReadFromBufferAndRemoveStaticObject;
import com.alibaba.alink.operator.batch.graph.utils.RecvRequestKeySelector;
import com.alibaba.alink.operator.batch.graph.utils.SendRequestKeySelector;
import com.alibaba.alink.operator.batch.graph.utils.HandleReceivedMessage;
import com.alibaba.alink.operator.batch.graph.walkpath.RandomWalkPathEngine;
import com.alibaba.alink.params.nlp.walk.RandomWalkParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This algorithm realizes the Random Walk. The graph is saved in the form of DataSet of edges, and the output random
 * walk consists of vertices in order. The iteration is in the form of DataSet so that it almost consumes no
 * communication memory.
 * <p>
 * If a random walk terminals before reach the walk length, it won't continue and we only need to return this short
 * walk.
 */

public final class RandomWalkBatchOp extends BatchOperator <RandomWalkBatchOp>
	implements RandomWalkParams <RandomWalkBatchOp> {

	private static final long serialVersionUID = 3726910334434343013L;
	public static final String PATH_COL_NAME = "path";
	public static final String GRAPH_STATISTICS = "graphStatistics";

	public RandomWalkBatchOp(Params params) {
		super(params);
	}

	public RandomWalkBatchOp() {
		this(new Params());
	}

	@Override
	public RandomWalkBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		int walkNum = getWalkNum();
		final int walkLength = getWalkLength();
		String delimiter = getDelimiter();
		String valueColName = getWeightCol();
		Boolean isToUnDigraph = getIsToUndigraph();
		int node0Idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), getSourceCol());
		int node1Idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), getTargetCol());
		int valueIdx = (valueColName == null) ? -1 : TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
			valueColName);
		boolean isWeightedSampling = valueColName != null;

		// we should parse string to long first.
		TypeInformation <?> node0Type = in.getColTypes()[node0Idx];
		TypeInformation <?> node1Type = in.getColTypes()[node1Idx];
		DataSet <Tuple2 <String, Long>> nodeMapping = null;
		DataSet <Row> mappedDataSet = null;
		boolean needRemapping = !((node0Type == BasicTypeInfo.LONG_TYPE_INFO
			&& node1Type == BasicTypeInfo.LONG_TYPE_INFO) || (node0Type == BasicTypeInfo.INT_TYPE_INFO
			&& node1Type == BasicTypeInfo.INT_TYPE_INFO));

		if (needRemapping) {
			nodeMapping = IDMappingUtils.computeIdMapping(in.getDataSet(), new int[] {node0Idx, node1Idx});
			mappedDataSet = IDMappingUtils.mapDataSetWithIdMapping(in.getDataSet(), nodeMapping,
				new int[] {node0Idx, node1Idx});
		} else {
			mappedDataSet = in.getDataSet();
		}

		GraphPartitionFunction graphPartitionFunction = new GraphPartitionHashFunction();

		// we always assume that the vertexId is long and the weight is double.
		DataSet <GraphEdge> parsedAndPartitionedAndSortedGraph = mappedDataSet
			.flatMap(new ParseGraphData(node0Idx, node1Idx, valueIdx, isToUnDigraph))
			.partitionCustom(new GraphPartitioner(graphPartitionFunction), 0)
			.sortPartition(0, Order.ASCENDING)
			.sortPartition(1, Order.ASCENDING)
			.map(new ConstructHomoEdge())
			.name("parsedAndPartitionedAndSortedGraph");

		DataSet <GraphStatistics> graphStatistics = parsedAndPartitionedAndSortedGraph.mapPartition(
			new ComputeGraphStatistics())
			.name("graphStatistics");

		long graphStorageHandler = IterTaskObjKeeper.getNewHandle();
		long randomWalkStorageHandler = IterTaskObjKeeper.getNewHandle();
		long walkWriteBufferHandler = IterTaskObjKeeper.getNewHandle();
		long walkReadBufferHandler = IterTaskObjKeeper.getNewHandle();
		// walkWriteBufferHandler and WalkReadBufferHandler points to the same object

		/**
		 * Since in DataSet API, Flink by default do slot sharing but not co-location, thus task ids may not be the
		 * same on on TM. However, the data preprocessing is expensive and we don't want it to be part of the loop.
		 * Thus we need to maintain the {partitionIdOutSideLoop --- partitionIdInsideLoop} map.
		 */
		final ExecutionEnvironment currentEnv = MLEnvironmentFactory.get(getMLEnvironmentId())
			.getExecutionEnvironment();
		DataSet <RandomWalkCommunicationUnit> initData = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(1L, currentEnv.getParallelism()),
				BasicTypeInfo.LONG_TYPE_INFO)
			.map(new MapFunction <Long, RandomWalkCommunicationUnit>() {
				@Override
				public RandomWalkCommunicationUnit map(Long value) throws Exception {
					return new RandomWalkCommunicationUnit(1, 1, null, null);
				}
			})
			.name("initData");
		IterativeDataSet <RandomWalkCommunicationUnit> loop = initData.iterate(Integer.MAX_VALUE)
			.name("loop");

		DataSet <RandomWalkCommunicationUnit> cacheGraphAndRandomWalk = parsedAndPartitionedAndSortedGraph
			.mapPartition(
				new CacheGraphAndRandomWalk(graphStorageHandler, randomWalkStorageHandler, walkWriteBufferHandler,
					walkReadBufferHandler, walkNum, walkLength, isWeightedSampling, getSamplingMethod(),
					graphPartitionFunction))
			.withBroadcastSet(graphStatistics, GRAPH_STATISTICS)
			.withBroadcastSet(loop, "loop")
			.name("cacheGraphAndRandomWalk");

		// get neighbors for remote sampling
		DataSet <RandomWalkCommunicationUnit> sendCommunicationUnit = cacheGraphAndRandomWalk.mapPartition(
			new GetMessageToSend(graphStorageHandler, randomWalkStorageHandler, walkWriteBufferHandler,
				graphPartitionFunction)
		).name("sendCommunicationUnit");

		DataSet <RandomWalkCommunicationUnit> recvCommunicationUnit = sendCommunicationUnit
			.partitionCustom(new GraphPartitioner(graphPartitionFunction),
				new SendRequestKeySelector <RandomWalkCommunicationUnit>())
			.mapPartition(new DoRemoteProcessing(graphStorageHandler))
			.partitionCustom(new GraphPartitioner(graphPartitionFunction),
				new RecvRequestKeySelector <RandomWalkCommunicationUnit>())
			.name("recvCommunicationUnit");

		DataSet <RandomWalkCommunicationUnit> finishedOneStep = recvCommunicationUnit.mapPartition(
			new HandleReceivedMessage <RandomWalkCommunicationUnit>(randomWalkStorageHandler)
		).name("finishedOneStep");

		DataSet <Object> termination = sendCommunicationUnit.map(
			new MapFunction <RandomWalkCommunicationUnit, Object>() {
				@Override
				public Object map(RandomWalkCommunicationUnit value) throws Exception {
					return new Object();
				}
			})
			.name("termination");
		DataSet <RandomWalkCommunicationUnit> output = loop.closeWith(finishedOneStep, termination);
		DataSet <long[]> emptyOut = output.mapPartition(
			new EndWritingRandomWalks <RandomWalkCommunicationUnit>(walkWriteBufferHandler))
			.name("emptyOut")
			.withBroadcastSet(output, "output");

		DataSet <long[]> memoryOut = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(0L, currentEnv.getParallelism()), BasicTypeInfo.LONG_TYPE_INFO)
			.mapPartition(new ReadFromBufferAndRemoveStaticObject <>(graphStorageHandler,
				randomWalkStorageHandler, walkReadBufferHandler, delimiter))
			.name("memoryOut");
		DataSet <long[]> mergedOutput = memoryOut.union(emptyOut)
			.name("mergedOutput");

		DataSet <Row> finalOutput = null;
		if (needRemapping) {
			finalOutput = IDMappingUtils.mapWalkToStringWithIdMapping(mergedOutput, nodeMapping, walkLength,
				delimiter);
		} else {
			finalOutput = mergedOutput.map(new LongArrayToRow(delimiter))
				.name("finalOutput");
		}

		setOutput(finalOutput, new TableSchema(new String[] {PATH_COL_NAME},
			new TypeInformation[] {Types.STRING}));

		return this;
	}

	static class CacheGraphAndRandomWalk extends RichMapPartitionFunction <GraphEdge, RandomWalkCommunicationUnit> {
		long graphStorageHandler;
		long randomWalkStorageHandler;
		long walkWriteBufferHandler;
		long walkReadBufferHandler;
		int numWalkPerVertex;
		int walkLen;
		boolean isWeighted;
		String samplingMethod;
		GraphPartitionFunction graphPartitionFunction;

		public CacheGraphAndRandomWalk(long graphStorageHandler, long randomWalkStorageHandler,
									   long walkWriteBufferHandler, long walkReadBufferHandler,
									   int numWalkPerVertex, int walkLen, boolean isWeighted, String samplingMethod,
									   GraphPartitionFunction graphPartitionFunction) {
			this.graphStorageHandler = graphStorageHandler;
			this.randomWalkStorageHandler = randomWalkStorageHandler;
			this.walkWriteBufferHandler = walkWriteBufferHandler;
			this.walkReadBufferHandler = walkReadBufferHandler;
			this.numWalkPerVertex = numWalkPerVertex;
			this.walkLen = walkLen;
			this.isWeighted = isWeighted;
			this.samplingMethod = samplingMethod;
			this.graphPartitionFunction = graphPartitionFunction;
		}

		@Override
		public void mapPartition(Iterable <GraphEdge> values, Collector <RandomWalkCommunicationUnit> out)
			throws Exception {
			List <GraphStatistics> graphStatistics = getRuntimeContext().getBroadcastVariable(GRAPH_STATISTICS);
			int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			int superStep = getIterationRuntimeContext().getSuperstepNumber();
			int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();

			if (superStep == 1) {
				// in step 1, we cache the graph and collect graph partition information.
				GraphStatistics myStatistics = null;
				for (GraphStatistics statistics : graphStatistics) {
					if (statistics.getPartitionId() == partitionId) {
						myStatistics = statistics;
						break;
					}
				}
				boolean useAlias = false;
				final String ALIAS_NAME = "ALIAS";
				if (samplingMethod.equalsIgnoreCase(ALIAS_NAME)) {
					useAlias = true;
				}
				HomoGraphEngine homoGraphEngine = new HomoGraphEngine(values, myStatistics.getVertexNum(),
					myStatistics.getEdgeNum(),
					isWeighted, useAlias);
				IterTaskObjKeeper.put(graphStorageHandler, partitionId, homoGraphEngine);

				Iterator <Long> allSrcVertices = homoGraphEngine.getAllSrcVertices();
				int randomWalkStorageSizePerVertex = this.numWalkPerVertex * this.walkLen;
				// we store at most 128MB in walks[] by default.
				int localBatchSize = (128 * 1024 * 1024) / 8
					/ randomWalkStorageSizePerVertex;
				localBatchSize = Math.min(localBatchSize, myStatistics.getVertexNum());

				RandomWalkPathEngine randomWalkPathEngine = new RandomWalkPathEngine(localBatchSize, numWalkPerVertex,
					walkLen, allSrcVertices);
				IterTaskObjKeeper.put(randomWalkStorageHandler, partitionId, randomWalkPathEngine);

				// 64MB as the buffer size
				int bufferSize = 64 * 1024 * 1024 / (walkLen * 8 + 16);
				RandomWalkMemoryBuffer randomWalkMemoryBuffer = new RandomWalkMemoryBuffer(bufferSize);
				IterTaskObjKeeper.put(walkWriteBufferHandler, partitionId, randomWalkMemoryBuffer);
				IterTaskObjKeeper.put(walkReadBufferHandler, partitionId, randomWalkMemoryBuffer);

				Iterator <Long> tmpAllSrcVertices = homoGraphEngine.getAllSrcVertices();
				if (tmpAllSrcVertices.hasNext()) {
					long oneVertexId = homoGraphEngine.getAllSrcVertices().next();
					int hashKey = graphPartitionFunction.apply(oneVertexId, numPartitions);
					out.collect(new RandomWalkCommunicationUnit(hashKey, partitionId, null, null));
				}
			} else if (superStep == 2) {
				// we store the hashKeyToPartitionMap in the second step
				List <RandomWalkCommunicationUnit> workerIdMappingList = getRuntimeContext().getBroadcastVariable(
					"loop");
				Map <Integer, Integer> workerIdMapping = new HashMap <>(workerIdMappingList.size());
				for (RandomWalkCommunicationUnit logical2physical : workerIdMappingList) {
					// We did not add a new class for now, but to make it more clean, here getSrcPartitionId refers to
					// hashKey, dstPartitionId refers to the worker id
					workerIdMapping.put(logical2physical.getSrcPartitionId(),
						logical2physical.getDstPartitionId());
				}
				HomoGraphEngine homoGraphEngine = IterTaskObjKeeper.get(graphStorageHandler, partitionId);
				assert null != homoGraphEngine;
				homoGraphEngine.setLogicalWorkerIdToPhysicalWorkerId(workerIdMapping);
			} else {
				// do nothing here.
			}
		}
	}

	/**
	 * GetNeighborsForRemoteSampling
	 */
	static class GetMessageToSend
		extends RichMapPartitionFunction <RandomWalkCommunicationUnit, RandomWalkCommunicationUnit> {
		long graphStorageHandler;
		long randomWalkStorageHandler;
		long walkBufferHandler;
		GraphPartitionFunction graphPartitionFunction;

		public GetMessageToSend(long graphStorageHandler, long randomWalkStorageHandler,
								long walkBufferHandler, GraphPartitionFunction graphPartitionFunction) {
			this.graphStorageHandler = graphStorageHandler;
			this.randomWalkStorageHandler = randomWalkStorageHandler;
			this.walkBufferHandler = walkBufferHandler;
			this.graphPartitionFunction = graphPartitionFunction;
		}

		@Override
		public void mapPartition(Iterable <RandomWalkCommunicationUnit> values,
								 Collector <RandomWalkCommunicationUnit> out)
			throws Exception {
			final int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			final int superStep = getIterationRuntimeContext().getSuperstepNumber();
			if (superStep == 1) {
				// pass the {hashkey --> partitionId} map to next step.
				for (RandomWalkCommunicationUnit randomWalkCommunicationUnit : values) {
					out.collect(randomWalkCommunicationUnit);
				}
			} else {
				for (RandomWalkCommunicationUnit randomWalkCommunicationUnit : values) {
					// blocking the Flink JOB EDGE and force the upside finish its job.
				}

				HomoGraphEngine homoGraphEngine = IterTaskObjKeeper.get(graphStorageHandler, partitionId);
				RandomWalkPathEngine randomWalkPathEngine = IterTaskObjKeeper.get(randomWalkStorageHandler,
					partitionId);
				RandomWalkMemoryBuffer randomWalkMemoryBuffer = IterTaskObjKeeper.get(walkBufferHandler, partitionId);
				assert null != homoGraphEngine;
				assert null != randomWalkPathEngine;
				assert null != randomWalkMemoryBuffer;

				long[] nextBatchOfVerticesToSampleFrom = randomWalkPathEngine.getNextBatchOfVerticesToSampleFrom();

				for (int walkId = 0; walkId < nextBatchOfVerticesToSampleFrom.length; walkId++) {

					// output the finished random walks by remote sampling.
					if (randomWalkPathEngine.canOutput(walkId)) {
						long[] finishedRandomWalk = randomWalkPathEngine.getOneWalkAndAddNewWalk(walkId);
						long newVertexToSampleFrom = randomWalkPathEngine.getNextVertexToSampleFrom(walkId);
						nextBatchOfVerticesToSampleFrom[walkId] = newVertexToSampleFrom;
						randomWalkMemoryBuffer.writeOneWalk(finishedRandomWalk);
					}

					long vertexToSample = nextBatchOfVerticesToSampleFrom[walkId];
					// do local sampling when possible for efficiency.
					while (homoGraphEngine.containsVertex(vertexToSample)) {
						vertexToSample = homoGraphEngine.sampleOneNeighbor(vertexToSample);
						randomWalkPathEngine.updatePath(walkId, vertexToSample);
						boolean canOutput = randomWalkPathEngine.canOutput(walkId);
						if (canOutput) {
							long[] finishedRandomWalk = randomWalkPathEngine.getOneWalkAndAddNewWalk(walkId);
							// output the random walk here
							randomWalkMemoryBuffer.writeOneWalk(finishedRandomWalk);
							vertexToSample = randomWalkPathEngine.getNextVertexToSampleFrom(walkId);
						}
					}
					nextBatchOfVerticesToSampleFrom[walkId] = vertexToSample;
				}

				// partition these request to other workers
				int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
				List <Long>[] vertexSplitPieces = new ArrayList[numPartitions];
				List <Integer>[] walkIdSplitPieces = new ArrayList[numPartitions];
				for (int p = 0; p < numPartitions; p++) {
					vertexSplitPieces[p] = new ArrayList <>();
					walkIdSplitPieces[p] = new ArrayList <>();
				}

				for (int walkId = 0; walkId < nextBatchOfVerticesToSampleFrom.length; walkId++) {
					if (nextBatchOfVerticesToSampleFrom[walkId] == -1) {
						// we don't have much node to sample by now --- the buffer is not full.
						continue;
					}
					int logicalDstId = graphPartitionFunction.apply(
						nextBatchOfVerticesToSampleFrom[walkId], numPartitions);
					int physicalDstId = homoGraphEngine.getPhysicalWorkerIdByLogicalWorkerId(logicalDstId);
					vertexSplitPieces[physicalDstId].add(nextBatchOfVerticesToSampleFrom[walkId]);
					walkIdSplitPieces[physicalDstId].add(walkId);
				}

				for (int dstPid = 0; dstPid < numPartitions; dstPid++) {
					if (walkIdSplitPieces[dstPid].size() != 0) {
						out.collect(
							new RandomWalkCommunicationUnit(partitionId, dstPid,
								walkIdSplitPieces[dstPid].toArray(new Integer[0]),
								vertexSplitPieces[dstPid].toArray(new Long[0])));
					}
				}

			}
		}
	}

	static class DoRemoteProcessing
		extends RichMapPartitionFunction <RandomWalkCommunicationUnit, RandomWalkCommunicationUnit> {

		long graphStorageHandler;

		public DoRemoteProcessing(long graphStorageHandler) {
			this.graphStorageHandler = graphStorageHandler;
		}

		@Override
		public void mapPartition(Iterable <RandomWalkCommunicationUnit> values,
								 Collector <RandomWalkCommunicationUnit> out)
			throws Exception {
			// we could further do sampling here if the sampled vertex is here. But it will introduce complex
			// data structure here. we leave it as future work.
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				// pass the {hashkey --> partitionId} map to next step.
				for (RandomWalkCommunicationUnit communicationUnit : values) {
					out.collect(communicationUnit);
				}
			} else {
				int partitionId = getRuntimeContext().getIndexOfThisSubtask();
				HomoGraphEngine homoGraphEngine = IterTaskObjKeeper.get(graphStorageHandler, partitionId);
				assert null != homoGraphEngine;
				for (RandomWalkCommunicationUnit randomWalkCommunicationUnit : values) {
					assert randomWalkCommunicationUnit.getDstPartitionId() == partitionId;
					Long[] verticesToSample = randomWalkCommunicationUnit.getRequestedVertexIds();
					for (int vertexCnt = 0; vertexCnt < verticesToSample.length; vertexCnt++) {
						if (homoGraphEngine.containsVertex(verticesToSample[vertexCnt])) {
							verticesToSample[vertexCnt] = homoGraphEngine.sampleOneNeighbor(
								verticesToSample[vertexCnt]);
						} else {
							// if this vertex should be partitioned to this worker via GraphPartition function, but it
							// has
							// no output neighbors.
							verticesToSample[vertexCnt] = -1L;
						}
					}
					out.collect(randomWalkCommunicationUnit);
				}
			}
		}
	}

	/**
	 * CommunicationUnit used in Flink shuffle. We send message from ${srcPartitionId} to ${dstPartitionId}, then get
	 * message back from ${dstPartitionId} to {srcPartitionId}.
	 */
	public static class RandomWalkCommunicationUnit implements Serializable {
		int srcPartitionId;
		int dstPartitionId;
		/**
		 * the vertex to sample
		 */
		Long[] requestedVertexIds;

		/**
		 * walkIds can be optimized if we use careful engineering. We leave it as future work.
		 */
		Integer[] walkIds;

		public RandomWalkCommunicationUnit(int srcPartitionId, int dstPartitionId, Integer[] walkIds,
										   Long[] requestedVertexIds) {
			this(srcPartitionId, dstPartitionId, walkIds, requestedVertexIds, null);
		}

		public RandomWalkCommunicationUnit(int srcPartitionId, int dstPartitionId, Integer[] walkIds,
										   Long[] requestedVertexIds,
										   Character[] vertexTypes) {
			this.srcPartitionId = srcPartitionId;
			this.dstPartitionId = dstPartitionId;
			this.walkIds = walkIds;
			this.requestedVertexIds = requestedVertexIds;
		}

		public int getDstPartitionId() {
			return dstPartitionId;
		}

		public int getSrcPartitionId() {
			return srcPartitionId;
		}

		public Integer[] getWalkIds() {
			return walkIds;
		}

		public Long[] getRequestedVertexIds() {
			return requestedVertexIds;
		}
	}
}

