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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp.RandomWalkCommunicationUnit;
import com.alibaba.alink.operator.batch.graph.storage.GraphEdge;
import com.alibaba.alink.operator.batch.graph.storage.HomoGraphEngine;
import com.alibaba.alink.operator.batch.graph.utils.ComputeGraphStatistics;
import com.alibaba.alink.operator.batch.graph.utils.ConstructHomoEdge;
import com.alibaba.alink.operator.batch.graph.utils.EndWritingRandomWalks;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionFunction;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionHashFunction;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitioner;
import com.alibaba.alink.operator.batch.graph.utils.GraphStatistics;
import com.alibaba.alink.operator.batch.graph.utils.IDMappingUtils;
import com.alibaba.alink.operator.batch.graph.utils.LongArrayToRow;
import com.alibaba.alink.operator.batch.graph.utils.ParseGraphData;
import com.alibaba.alink.operator.batch.graph.utils.RandomWalkMemoryBuffer;
import com.alibaba.alink.operator.batch.graph.utils.ReadFromBufferAndRemoveStaticObject;
import com.alibaba.alink.operator.batch.graph.utils.RecvRequestKeySelector;
import com.alibaba.alink.operator.batch.graph.utils.SendRequestKeySelector;
import com.alibaba.alink.operator.batch.graph.walkpath.Node2VecWalkPathEngine;
import com.alibaba.alink.params.nlp.Node2VecParams;
import com.alibaba.alink.params.nlp.walk.Node2VecWalkParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This algorithm realizes the node2vec Walk.
 * The graph is saved in the form of DataSet of edges,
 * and the output random walk consists of vertices in order.
 * In the DataSet iteration loop, the neighbors of vertex t needs to
 * be sent to v to judge the weight of edges.
 * In this algorithm, we record vertices in the form of edges.
 * The source represents the id of vertex, and the target represents who it denotes to
 * and the value represents the weight.
 * <p>
 * If a random walk terminals before reach the walk length, it won't continue and
 * we only need to return this short walk.
 */
@InputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.GRAPH)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "sourceCol", portIndices = 0, allowedTypeCollections = {TypeCollections.INT_LONG_TYPES, TypeCollections.STRING_TYPES})
@ParamSelectColumnSpec(name = "targetCol", portIndices = 0, allowedTypeCollections = {TypeCollections.INT_LONG_TYPES, TypeCollections.STRING_TYPES})
@ParamSelectColumnSpec(name = "weightCol", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@NameCn("Node2Vec游走")
public final class Node2VecWalkBatchOp extends BatchOperator <Node2VecWalkBatchOp>
	implements Node2VecWalkParams <Node2VecWalkBatchOp> {

	public static final String PATH_COL_NAME = "path";
	public static final String GRAPH_STATISTICS = "graphStatistics";
	private static final long serialVersionUID = 5772364018494433734L;
	final static long PREV_IN_CUR_NEIGHBOR = -2021L;
	final static long PREV_NOT_IN_CUR_NEIGHBOR = -2022L;

	public Node2VecWalkBatchOp() {
		super(new Params());
	}

	public Node2VecWalkBatchOp(Params params) {
		super(params);
	}

	@Override
	public Node2VecWalkBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		double pReciVal = 1 / getParams().get(Node2VecParams.P);
		double qReciVal = 1 / getParams().get(Node2VecParams.Q);
		Integer walkNum = getWalkNum();
		Integer walkLength = getWalkLength();
		String delimiter = getDelimiter();
		String node0ColName = getSourceCol();
		String node1ColName = getTargetCol();
		String valueColName = getWeightCol();
		Boolean isToUnDigraph = getIsToUndigraph();

		int node0Idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), node0ColName);
		int node1Idx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), node1ColName);
		int valueIdx = (valueColName == null) ? -1 : TableUtil.findColIndexWithAssertAndHint(in.getColNames(),
			valueColName);

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

		boolean isWeightedSampling = valueColName != null;
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

		final ExecutionEnvironment currentEnv = MLEnvironmentFactory.get(getMLEnvironmentId())
			.getExecutionEnvironment();
		DataSet <Node2VecCommunicationUnit> initData = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(1L, currentEnv.getParallelism()),
				BasicTypeInfo.LONG_TYPE_INFO)
			.map(new MapFunction <Long, Node2VecCommunicationUnit>() {
				@Override
				public Node2VecCommunicationUnit map(Long value) {
					return new Node2VecCommunicationUnit(1, 1, null, null, null, null);
				}
			}).name("initData");

		IterativeDataSet <Node2VecCommunicationUnit> loop = initData.iterate(Integer.MAX_VALUE)
			.name("loop");

		DataSet <Node2VecCommunicationUnit> cachedGraphAndRandomWalk = parsedAndPartitionedAndSortedGraph
			.mapPartition(new CacheGraphAndRandomWalk(graphStorageHandler, randomWalkStorageHandler,
				walkWriteBufferHandler, walkReadBufferHandler, walkNum, walkLength,
				isWeightedSampling, getSamplingMethod(), graphPartitionFunction))
			.withBroadcastSet(graphStatistics, GRAPH_STATISTICS)
			.withBroadcastSet(loop, "loop")
			.name("cachedGraphAndRandomWalk");

		// get neighbors for remote sampling
		DataSet <Node2VecCommunicationUnit> sendCommunicationUnit = cachedGraphAndRandomWalk.mapPartition(
			new GetMessageToSend(graphStorageHandler, randomWalkStorageHandler, walkWriteBufferHandler,
				graphPartitionFunction)
		)
			.name("sendCommunicationUnit");

		DataSet <Node2VecCommunicationUnit> recvCommunicationUnit = sendCommunicationUnit
			.partitionCustom(new GraphPartitioner(graphPartitionFunction),
				new SendRequestKeySelector <Node2VecCommunicationUnit>())
			.mapPartition(new DoRemoteProcessing(graphStorageHandler))
			.partitionCustom(new GraphPartitioner(graphPartitionFunction),
				new RecvRequestKeySelector <Node2VecCommunicationUnit>())
			.name("recvCommunicationUnit");

		DataSet <Node2VecCommunicationUnit> finishedOneStep = recvCommunicationUnit.mapPartition(
			new HandleReceivedMessage(randomWalkStorageHandler, pReciVal, qReciVal)
		).name("finishedOneStep");

		DataSet <Object> termination = sendCommunicationUnit.map(
			new MapFunction <Node2VecCommunicationUnit, Object>() {
				@Override
				public Object map(Node2VecCommunicationUnit value) {
					return new Object();
				}
			}).name("termination");

		DataSet <Node2VecCommunicationUnit> output = loop.closeWith(finishedOneStep, termination);
		DataSet <long[]> emptyOut = output.mapPartition(
			new EndWritingRandomWalks <Node2VecCommunicationUnit>(walkWriteBufferHandler))
			.name("emptyOut")
			.withBroadcastSet(output, "output");

		DataSet <long[]> memoryOut = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(0L, currentEnv.getParallelism()), BasicTypeInfo.LONG_TYPE_INFO)
			.mapPartition(new ReadFromBufferAndRemoveStaticObject <>(graphStorageHandler,
				randomWalkStorageHandler, walkReadBufferHandler, delimiter))
			.name("memoryOut");

		DataSet <long[]> mergedOutput = emptyOut.union(memoryOut)
			.name("mergedOutput");
		DataSet <Row> finalOutput = null;
		if (needRemapping) {
			finalOutput = IDMappingUtils.mapWalkToStringWithIdMapping(mergedOutput, nodeMapping, walkLength,
				delimiter);
		} else {
			finalOutput = mergedOutput.map(new LongArrayToRow(delimiter));
		}

		setOutput(finalOutput, new TableSchema(new String[] {PATH_COL_NAME},
			new TypeInformation[] {Types.STRING}));
		return this;
	}

	static class CacheGraphAndRandomWalk extends RichMapPartitionFunction <GraphEdge, Node2VecCommunicationUnit> {
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
									   long walkWriteBufferHandler,
									   long walkReadBufferHandler,
									   int numWalkPerVertex, int walkLen,
									   boolean isWeighted, String samplingMethod,
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
		public void mapPartition(Iterable <GraphEdge> values, Collector <Node2VecCommunicationUnit> out)
			throws Exception {
			final int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			final int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
			final int superStep = getIterationRuntimeContext().getSuperstepNumber();

			if (superStep == 1) {
				List <GraphStatistics> graphStatistics = getRuntimeContext().getBroadcastVariable(GRAPH_STATISTICS);
				GraphStatistics myStatistics = null;
				for (GraphStatistics statistics : graphStatistics) {
					if (statistics.getPartitionId() == partitionId) {
						myStatistics = statistics;
						break;
					}
				}
				boolean useAlias = false;
				AkPreconditions.checkNotNull(myStatistics, "The statistics is null.");
				final String ALIAS_NAME = "ALIAS";
				if (samplingMethod.equalsIgnoreCase(ALIAS_NAME)) {
					useAlias = true;
				}
				HomoGraphEngine homoGraphEngine = new HomoGraphEngine(values,
					myStatistics.getVertexNum(), myStatistics.getEdgeNum(),
					isWeighted, useAlias);
				IterTaskObjKeeper.put(graphStorageHandler, partitionId, homoGraphEngine);

				Iterator <Long> allSrcVertices = homoGraphEngine.getAllSrcVertices();
				int randomWalkStorageSizePerVertex = this.numWalkPerVertex * this.walkLen;
				// we store at most 128MB in walks[] by default.
				int localBatchSize = (128 * 1024 * 1024) / 8
					/ randomWalkStorageSizePerVertex;
				// since node2vec needs to maintain more states.
				localBatchSize = localBatchSize / 2;
				localBatchSize = Math.min(localBatchSize, myStatistics.getVertexNum());

				Node2VecWalkPathEngine node2VecWalkPathEngine = new Node2VecWalkPathEngine(localBatchSize,
					numWalkPerVertex,
					walkLen, allSrcVertices);
				IterTaskObjKeeper.put(randomWalkStorageHandler, partitionId, node2VecWalkPathEngine);

				// 64MB as the buffer size
				int bufferSize = 64 * 1024 * 1024 / (walkLen * 8 + 16);
				RandomWalkMemoryBuffer randomWalkMemoryBuffer = new RandomWalkMemoryBuffer(bufferSize);
				IterTaskObjKeeper.put(walkWriteBufferHandler, partitionId, randomWalkMemoryBuffer);
				IterTaskObjKeeper.put(walkReadBufferHandler, partitionId, randomWalkMemoryBuffer);

				Iterator <Long> tmpAllSrcVertices = homoGraphEngine.getAllSrcVertices();
				if (tmpAllSrcVertices.hasNext()) {
					long oneVertexId = homoGraphEngine.getAllSrcVertices().next();
					int hashKey = graphPartitionFunction.apply(oneVertexId, numPartitions);
					out.collect(new Node2VecCommunicationUnit(hashKey, partitionId, null, null, null, null));
				}
			} else if (superStep == 2) {
				List <Node2VecCommunicationUnit> workerIdMappingList = getRuntimeContext().getBroadcastVariable(
					"loop");
				Map <Integer, Integer> workerIdMapping = new HashMap <>(workerIdMappingList.size());
				for (Node2VecCommunicationUnit logical2physical : workerIdMappingList) {
					workerIdMapping.put(logical2physical.getSrcPartitionId(),
						logical2physical.getDstPartitionId());
				}
				HomoGraphEngine homoGraphEngine = IterTaskObjKeeper.get(graphStorageHandler, partitionId);
				AkPreconditions.checkNotNull(homoGraphEngine, "the graph engine is null");
				homoGraphEngine.setLogicalWorkerIdToPhysicalWorkerId(workerIdMapping);
			} else {
				// do nothing here.
			}
		}

	}

	/**
	 * SampleANbrOrGetNumOfNbrsOrCheckNextVertexConnectsPrevVertex
	 */
	static class GetMessageToSend
		extends RichMapPartitionFunction <Node2VecCommunicationUnit, Node2VecCommunicationUnit> {
		long graphStorageHandler;
		long randomWalkStorageHandler;
		long walkWriteBufferHandler;
		GraphPartitionFunction graphPartitionFunction;

		public GetMessageToSend(long graphStorageHandler,
								long randomWalkStorageHandler,
								long walkWriteBufferHandler,
								GraphPartitionFunction graphPartitionFunction) {
			this.graphStorageHandler = graphStorageHandler;
			this.randomWalkStorageHandler = randomWalkStorageHandler;
			this.walkWriteBufferHandler = walkWriteBufferHandler;
			this.graphPartitionFunction = graphPartitionFunction;
		}

		@Override
		public void mapPartition(Iterable <Node2VecCommunicationUnit> values,
								 Collector <Node2VecCommunicationUnit> out)
			throws Exception {
			final int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			final int superStep = getIterationRuntimeContext().getSuperstepNumber();
			final int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();

			if (superStep == 1) {
				for (Node2VecCommunicationUnit val : values) {
					out.collect(val);
				}
			} else {
				for (Node2VecCommunicationUnit val : values) {
					// blocking the Flink JOB EDGE and force the upside finish its job.
				}
				HomoGraphEngine homoGraphEngine = IterTaskObjKeeper.get(graphStorageHandler,
					partitionId);
				Node2VecWalkPathEngine node2VecWalkPathEngine = IterTaskObjKeeper.get(randomWalkStorageHandler,
					partitionId);
				RandomWalkMemoryBuffer randomWalkMemoryBuffer = IterTaskObjKeeper.get(walkWriteBufferHandler,
					partitionId);
				AkPreconditions.checkNotNull(homoGraphEngine, "homoGraphEngine is null.");
				AkPreconditions.checkNotNull(node2VecWalkPathEngine, "node2VecWalkPathEngine is null");
				AkPreconditions.checkNotNull(randomWalkMemoryBuffer, "randomWalkMemoryBuffer is null");

				long[] nextBatchOfVerticesToSampleFrom = node2VecWalkPathEngine.getNextBatchOfVerticesToSampleFrom();

				// output the finished random walks by remote sampling.
				for (int walkId = 0; walkId < nextBatchOfVerticesToSampleFrom.length; walkId++) {
					if (node2VecWalkPathEngine.canOutput(walkId)) {
						long[] finishedRandomWalk = node2VecWalkPathEngine.getOneWalkAndAddNewWalk(walkId);
						long newVertexToSampleFrom = node2VecWalkPathEngine.getNextVertexToSampleFrom(walkId);
						nextBatchOfVerticesToSampleFrom[walkId] = newVertexToSampleFrom;
						randomWalkMemoryBuffer.writeOneWalk(finishedRandomWalk);
					}
				}

				// update the second vertex of each random walk since we do vertex cut and all of the neighbors of the
				// start vertices are local.
				for (int walkId = 0; walkId < nextBatchOfVerticesToSampleFrom.length; walkId++) {
					long newVertexToSampleFrom = nextBatchOfVerticesToSampleFrom[walkId];
					if (newVertexToSampleFrom != -1 && node2VecWalkPathEngine.getPrevVertex(walkId) == -1) {
						// the path does not ends here & the path length is one by now.
						newVertexToSampleFrom = homoGraphEngine.sampleOneNeighbor(newVertexToSampleFrom);
						node2VecWalkPathEngine.updatePath(walkId, newVertexToSampleFrom);
						nextBatchOfVerticesToSampleFrom[walkId] = newVertexToSampleFrom;
						node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.INIT);
						node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.GET_NUM_OF_NEIGHBORS);
					}
				}

				// partition these request to other workers
				List <Long>[] requestIdsSplitPieces = new ArrayList[numPartitions];
				List <Integer>[] walkIdSplitPieces = new ArrayList[numPartitions];
				List <Node2VecState>[] node2VecStatePieces = new ArrayList[numPartitions];
				List <Long>[] prevVertices = new ArrayList[numPartitions];
				for (int p = 0; p < numPartitions; p++) {
					requestIdsSplitPieces[p] = new ArrayList <>();
					walkIdSplitPieces[p] = new ArrayList <>();
					node2VecStatePieces[p] = new ArrayList <>();
					prevVertices[p] = new ArrayList <>();
				}

				// prepare for communication
				for (int walkId = 0; walkId < nextBatchOfVerticesToSampleFrom.length; walkId++) {
					if (nextBatchOfVerticesToSampleFrom[walkId] == -1L) {
						// we don't have much node to sample by now --- the buffer is not full.
						continue;
					}
					int logicalDstId = graphPartitionFunction.apply(
						nextBatchOfVerticesToSampleFrom[walkId], numPartitions);
					int physicalDstId = homoGraphEngine.getPhysicalWorkerIdByLogicalWorkerId(logicalDstId);
					Node2VecState state = node2VecWalkPathEngine.getNode2VecState(walkId);

					switch (state) {
						case GET_NUM_OF_NEIGHBORS:
							// add request for get number of neighbors
							requestIdsSplitPieces[physicalDstId].add(nextBatchOfVerticesToSampleFrom[walkId]);
							walkIdSplitPieces[physicalDstId].add(walkId);
							node2VecStatePieces[physicalDstId].add(Node2VecState.GET_NUM_OF_NEIGHBORS);
							// could be null
							prevVertices[physicalDstId].add(
								node2VecWalkPathEngine.getPrevVertex(walkId));
							break;
						case SAMPLE_A_NEIGHBOR:
							requestIdsSplitPieces[physicalDstId].add(nextBatchOfVerticesToSampleFrom[walkId]);
							walkIdSplitPieces[physicalDstId].add(walkId);
							node2VecStatePieces[physicalDstId].add(Node2VecState.SAMPLE_A_NEIGHBOR);
							// could be null
							prevVertices[physicalDstId].add(
								node2VecWalkPathEngine.getPrevVertex(walkId));
							break;
						case CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX:
							// ATTENTION: the queryVertex is not the current vertex to sample, it is the neigbor of
							// {the current vertex to sample}, we need to query whether the edge (queryVertex, prev)
							// exists.
							long queryVertex = node2VecWalkPathEngine.getSampledNeighbor(walkId);

							logicalDstId = graphPartitionFunction.apply(queryVertex, numPartitions);
							physicalDstId = homoGraphEngine.getPhysicalWorkerIdByLogicalWorkerId(logicalDstId);
							requestIdsSplitPieces[physicalDstId].add(queryVertex);
							walkIdSplitPieces[physicalDstId].add(walkId);
							node2VecStatePieces[physicalDstId].add(
								Node2VecState.CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX);
							prevVertices[physicalDstId].add(node2VecWalkPathEngine.getPrevVertex(walkId));
							break;
						default:
							throw new RuntimeException(
								"Illegal state here: Remote state must be one of [GET_NUM_OF_NEIGHBORS, "
									+ "SAMPLE_A_NEIGHBOR and CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX]");
					}
				}

				for (int dstPid = 0; dstPid < numPartitions; dstPid++) {
					if (walkIdSplitPieces[dstPid].size() != 0) {
						out.collect(new Node2VecCommunicationUnit(partitionId, dstPid,
							walkIdSplitPieces[dstPid].toArray(new Integer[0]),
							requestIdsSplitPieces[dstPid].toArray(new Long[0]),
							node2VecStatePieces[dstPid].toArray(new Node2VecState[0]),
							prevVertices[dstPid].toArray(new Long[0])));
					}
				}

			}
		}
	}

	static class DoRemoteProcessing
		extends RichMapPartitionFunction <Node2VecCommunicationUnit, Node2VecCommunicationUnit> {

		long graphStorageHandler;

		public DoRemoteProcessing(long graphStorageHandler) {
			this.graphStorageHandler = graphStorageHandler;
		}

		@Override
		public void mapPartition(Iterable <Node2VecCommunicationUnit> values,
								 Collector <Node2VecCommunicationUnit> out)
			throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				// pass the {hashkey --> partitionId} map to next step.
				for (Node2VecCommunicationUnit communicationUnit : values) {
					out.collect(communicationUnit);
				}
			} else {
				// we could further do sampling here if the sampled vertex is here. But it will introduce complex
				// data structure here. we leave it as future work.
				int partitionId = getRuntimeContext().getIndexOfThisSubtask();
				HomoGraphEngine homoGraphEngine = IterTaskObjKeeper.get(graphStorageHandler,
					partitionId);
				AkPreconditions.checkNotNull(homoGraphEngine, "homoGraphEngine is null");

				for (Node2VecCommunicationUnit node2VecCommunicationUnit : values) {
					AkPreconditions.checkState(node2VecCommunicationUnit.getDstPartitionId() == partitionId,
						"The target task id is incorrect. It should be "
							+ node2VecCommunicationUnit.getDstPartitionId()
							+ ", but it is " + partitionId);
					Long[] requestedVertexIds = node2VecCommunicationUnit.getRequestedVertexIds();
					Long[] prevVertexIdsOrContainsPrevVertexIds
						= node2VecCommunicationUnit.getPrevVertexIdsOrContainsPrevVertexIds();
					for (int vertexCnt = 0; vertexCnt < requestedVertexIds.length; vertexCnt++) {
						// do the sampling here.
						if (homoGraphEngine.containsVertex(requestedVertexIds[vertexCnt])) {
							Node2VecState state = node2VecCommunicationUnit.getMessageTypes()[vertexCnt];
							switch (state) {
								case GET_NUM_OF_NEIGHBORS:
									int numNeighbors = homoGraphEngine.getNumNeighbors(
										requestedVertexIds[vertexCnt]);
									requestedVertexIds[vertexCnt] = (long) numNeighbors;
									break;
								case SAMPLE_A_NEIGHBOR:
									requestedVertexIds[vertexCnt] = homoGraphEngine.sampleOneNeighbor(
										requestedVertexIds[vertexCnt]);
									break;
								case CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX:
									long vertexToCheck = requestedVertexIds[vertexCnt];
									long prevNeighbor = prevVertexIdsOrContainsPrevVertexIds[vertexCnt];
									if (homoGraphEngine.containsEdge(vertexToCheck, prevNeighbor)) {
										prevVertexIdsOrContainsPrevVertexIds[vertexCnt] = PREV_IN_CUR_NEIGHBOR;
									} else {
										prevVertexIdsOrContainsPrevVertexIds[vertexCnt] = PREV_NOT_IN_CUR_NEIGHBOR;
									}
									break;
								default:
									throw new AkIllegalStateException(
										"Illegal state here: Remote state must be one of [GET_NUM_OF_NEIGHBORS, "
											+ "SAMPLE_A_NEIGHBOR and CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX]");
							}
						} else {
							// if this vertex should be partitioned to this worker via GraphPartition function, but it
							// has no output neighbors.
							requestedVertexIds[vertexCnt] = -1L;
						}
					}
					out.collect(node2VecCommunicationUnit);
				}
			}
		}
	}

	/**
	 * Update the walk path using the received CommunicationUnit from remote.
	 */
	static class HandleReceivedMessage
		extends RichMapPartitionFunction <Node2VecCommunicationUnit, Node2VecCommunicationUnit> {
		long randomWalkStorageHandler;
		double invP, invQ;
		Random random;

		public HandleReceivedMessage(long randomWalkStorageHandler, double invP, double invQ) {
			this.randomWalkStorageHandler = randomWalkStorageHandler;
			this.invP = invP;
			this.invQ = invQ;
			random = new Random(2021);
		}

		@Override
		public void mapPartition(Iterable <Node2VecCommunicationUnit> values,
								 Collector <Node2VecCommunicationUnit> out)
			throws Exception {
			int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			int superStep = getIterationRuntimeContext().getSuperstepNumber();
			if (superStep == 1) {
				// pass the {hashkey --> partitionId} map to next step.
				for (Node2VecCommunicationUnit val : values) {
					out.collect(val);
				}
			} else {
				Node2VecWalkPathEngine node2VecWalkPathEngine = IterTaskObjKeeper.get(randomWalkStorageHandler,
					partitionId);
				AkPreconditions.checkNotNull(node2VecWalkPathEngine, "node2VecWalkPathEngine is null");

				for (Node2VecCommunicationUnit node2VecCommunicationUnit : values) {
					int srcPartitionId = node2VecCommunicationUnit.getSrcPartitionId();
					AkPreconditions.checkState(srcPartitionId == partitionId,
						"The target task id is incorrect. It should be "
							+ srcPartitionId
							+ ", but it is " + partitionId);
					Long[] recvResults = node2VecCommunicationUnit.getRequestedVertexIds();
					Integer[] walkIds = node2VecCommunicationUnit.getWalkIds();
					Node2VecState[] messageTypes = node2VecCommunicationUnit.getMessageTypes();
					Long[] prevVertexIdsOrContainsPrevVertexIds
						= node2VecCommunicationUnit.getPrevVertexIdsOrContainsPrevVertexIds();

					for (int vertexCnt = 0; vertexCnt < recvResults.length; vertexCnt++) {
						int walkId = walkIds[vertexCnt];
						double prob;
						switch (messageTypes[vertexCnt]) {
							case GET_NUM_OF_NEIGHBORS:
								int numNeighbors = (int) ((long) (recvResults[vertexCnt]));
								if (numNeighbors == 0) {
									node2VecWalkPathEngine.updatePath(walkId, -1);
									node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.FINISHED);
									node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.INIT);
									node2VecWalkPathEngine.setNode2VecState(walkId,
										Node2VecState.GET_NUM_OF_NEIGHBORS);
								} else {
									Tuple3 <Double, Double, Double> rejectionSampled = rejectionSample(invP, invQ,
										numNeighbors);
									node2VecWalkPathEngine.setRejectionState(walkId, rejectionSampled);
									prob = random.nextDouble() * node2VecWalkPathEngine.getUpperBound(walkId);
									node2VecWalkPathEngine.setProb(walkId, prob);
									node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.SAMPLE_A_NEIGHBOR);
								}
								break;
							case SAMPLE_A_NEIGHBOR:
								long sampledNeighbor = recvResults[vertexCnt];
								prob = node2VecWalkPathEngine.getProb(walkId);
								double shatter = node2VecWalkPathEngine.getShatter(walkId);
								double upperBound = node2VecWalkPathEngine.getUpperBound(walkId);
								double lowerBound = node2VecWalkPathEngine.getLowerBound(walkId);
								long prevVertex = node2VecWalkPathEngine.getPrevVertex(walkId);
								boolean succeedSampling =
									(prob + shatter >= upperBound && sampledNeighbor == prevVertex) ||
										(prob < lowerBound) || (prob < invP && sampledNeighbor == prevVertex);
								if (succeedSampling) {
									// we succeed in sampling one vertex.
									node2VecWalkPathEngine.updatePath(walkId, sampledNeighbor);
									node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.FINISHED);
									node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.INIT);
									node2VecWalkPathEngine.setNode2VecState(walkId,
										Node2VecState.GET_NUM_OF_NEIGHBORS);
								} else {
									node2VecWalkPathEngine.setSampledNeighbor(walkId, sampledNeighbor);
									node2VecWalkPathEngine.setNode2VecState(walkId,
										Node2VecState.CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX);
								}
								break;
							case CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX:
								boolean nextNodeNeighborContainsPrevVertex =
									prevVertexIdsOrContainsPrevVertexIds[vertexCnt]
										== PREV_IN_CUR_NEIGHBOR;
								double prob_dash = nextNodeNeighborContainsPrevVertex ? 1.0 : invQ;
								if (prob_dash > node2VecWalkPathEngine.getProb(walkId)) {
									// we succeed sampling one vertex
									node2VecWalkPathEngine.updatePath(walkId, recvResults[vertexCnt]);
									node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.FINISHED);
									node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.INIT);
									node2VecWalkPathEngine.setNode2VecState(walkId,
										Node2VecState.GET_NUM_OF_NEIGHBORS);
								} else {
									prob = random.nextDouble() * node2VecWalkPathEngine.getUpperBound(walkId);
									node2VecWalkPathEngine.setProb(walkId, prob);
									node2VecWalkPathEngine.setNode2VecState(walkId, Node2VecState.SAMPLE_A_NEIGHBOR);
								}
								break;
							default:
								throw new RuntimeException(
									"Illegal state here: Remote state must be one of [GET_NUM_OF_NEIGHBORS, "
										+ "SAMPLE_A_NEIGHBOR and CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX]");
						}
					}
				}
			}
		}
	}

	/**
	 * in rejection-based sampling process,
	 */
	public enum Node2VecState {
		/**
		 * start of one sampling
		 */
		INIT,
		/**
		 * need to get number of neighbors
		 */
		GET_NUM_OF_NEIGHBORS,
		/**
		 * start of the inner loop of sampling one node
		 */
		LOOP_START,
		/**
		 * sample a neighbor of the current vertex
		 */
		SAMPLE_A_NEIGHBOR,
		/**
		 * check whether next node contains the previous vertex
		 */
		CHECK_NEXT_NODE_NEIGHBOR_CONTAINS_PREV_VERTEX,
		/**
		 * finished finding a neighbor for the current vertex
		 */
		FINISHED
	}

	static Tuple3 <Double, Double, Double> rejectionSample(double invP, double invQ, int numNeigbors) {
		double upperBound = Math.max(1, Math.max(invP, invQ));
		double lowerBound = Math.min(1, Math.min(invP, invQ));
		double shatter = 0;
		double secondUpperBound = Math.max(1.0, invQ);
		if (invP > secondUpperBound) {
			shatter = secondUpperBound / numNeigbors;
			upperBound = secondUpperBound + shatter;
		}
		return Tuple3.of(upperBound, lowerBound, shatter);

	}

	static class Node2VecCommunicationUnit extends RandomWalkCommunicationUnit implements Serializable {
		/**
		 * used in check whether next node contains previous vertex as a neighbor
		 */
		Long[] prevVertexIdsOrContainsPrevVertexIds;
		/**
		 * used by state transferring
		 */
		Node2VecState[] messageTypes;

		public Node2VecCommunicationUnit(int srcPartitionId, int dstPartitionId, Integer[] walkIds,
										 Long[] requestedVertexIds, Node2VecState[] messageTypes,
										 Long[] prevVertexIdsOrContainsPrevVertexIds) {
			super(srcPartitionId, dstPartitionId, walkIds, requestedVertexIds);
			this.messageTypes = messageTypes;
			this.prevVertexIdsOrContainsPrevVertexIds = prevVertexIdsOrContainsPrevVertexIds;
		}

		public Node2VecState[] getMessageTypes() {
			return messageTypes;
		}

		public Long[] getPrevVertexIdsOrContainsPrevVertexIds() {
			return prevVertexIdsOrContainsPrevVertexIds;
		}
	}
}
