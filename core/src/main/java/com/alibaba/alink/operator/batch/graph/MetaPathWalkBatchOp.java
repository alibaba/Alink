package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp.RandomWalkCommunicationUnit;
import com.alibaba.alink.operator.batch.graph.storage.GraphEdge;
import com.alibaba.alink.operator.batch.graph.storage.HeteGraphEngine;
import com.alibaba.alink.operator.batch.graph.utils.ComputeGraphStatistics;
import com.alibaba.alink.operator.batch.graph.utils.ConstructHeteEdge;
import com.alibaba.alink.operator.batch.graph.utils.EndWritingRandomWalks;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionFunction;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionHashFunction;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitioner;
import com.alibaba.alink.operator.batch.graph.utils.GraphStatistics;
import com.alibaba.alink.operator.batch.graph.utils.IDMappingUtils;
import com.alibaba.alink.operator.batch.graph.utils.LongArrayToRow;
import com.alibaba.alink.operator.batch.graph.utils.RandomWalkMemoryBuffer;
import com.alibaba.alink.operator.batch.graph.utils.ParseGraphData;
import com.alibaba.alink.operator.batch.graph.utils.ReadFromBufferAndRemoveStaticObject;
import com.alibaba.alink.operator.batch.graph.utils.RecvRequestKeySelector;
import com.alibaba.alink.operator.batch.graph.utils.SendRequestKeySelector;
import com.alibaba.alink.operator.batch.graph.utils.HandleReceivedMessage;
import com.alibaba.alink.operator.batch.graph.walkpath.MetaPathWalkPathEngine;
import com.alibaba.alink.params.nlp.walk.MetaPathWalkParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This algorithm realizes the metaPath Walk.
 * The graph is saved in the form of DataSet of edges,
 * and the output random walk consists of vertices in order.
 * <p>
 * If a walk terminals before reach the walk length, it won't continue and
 * we only need to return this short walk.
 */

public class MetaPathWalkBatchOp extends BatchOperator <MetaPathWalkBatchOp>
	implements MetaPathWalkParams <MetaPathWalkBatchOp> {

	public static final String PATH_COL_NAME = "path";
	public static final String GRAPH_STATISTICS = "graphStatistics";
	private static final long serialVersionUID = -4645013343119976771L;

	public MetaPathWalkBatchOp() {
		this(new Params());
	}

	public MetaPathWalkBatchOp(Params params) {
		super(params);
	}

	@Override
	public MetaPathWalkBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		BatchOperator <?> in1 = inputs[0];
		BatchOperator <?> in2 = inputs[1];

		Integer walkNum = getWalkNum();
		Integer walkLength = getWalkLength();
		String delimiter = getDelimiter();
		String node0ColName = getSourceCol();
		String node1ColName = getTargetCol();
		String valueColName = getWeightCol();
		Boolean isToUnDigraph = getIsToUndigraph();
		int node0Idx = TableUtil.findColIndexWithAssertAndHint(in1.getColNames(), node0ColName);
		int node1Idx = TableUtil.findColIndexWithAssertAndHint(in1.getColNames(), node1ColName);
		int valueIdx = (valueColName == null) ? -1 : TableUtil.findColIndexWithAssertAndHint(in1.getColNames(),
			valueColName);

		String vertexColName = getVertexCol();
		String typeColName = getTypeCol();
		int vertexIdx = TableUtil.findColIndexWithAssertAndHint(in2.getColNames(), vertexColName);
		int typeIdx = TableUtil.findColIndexWithAssertAndHint(in2.getColNames(), typeColName);
		String metaPath = getMetaPath();
		boolean isWeightedSampling = valueColName != null && !valueColName.isEmpty();
		String[] metaPaths = metaPath.split(",");
		for (int i = 0; i < metaPaths.length; ++i) {
			metaPaths[i] = metaPaths[i].trim();
		}

		List <char[]> metaPathList = new ArrayList <>(metaPaths.length);
		for (int i = 0; i < metaPaths.length; i++) {
			metaPathList.add(metaPaths[i].toCharArray());
		}

		// we should parse string to long first.
		TypeInformation <?> node0Type = in1.getColTypes()[node0Idx];
		TypeInformation <?> node1Type = in1.getColTypes()[node1Idx];
		DataSet <Tuple2 <String, Long>> nodeMapping = null;
		DataSet <Row> mappedDataSet = null;
		DataSet <Row> mappedNodeTypes = null;
		boolean needRemapping = !((node0Type == BasicTypeInfo.LONG_TYPE_INFO
			&& node1Type == BasicTypeInfo.LONG_TYPE_INFO) || (node0Type == BasicTypeInfo.INT_TYPE_INFO
			&& node1Type == BasicTypeInfo.INT_TYPE_INFO));

		if (needRemapping) {
			nodeMapping = IDMappingUtils.computeIdMapping(in1.getDataSet(), new int[] {node0Idx, node1Idx});
			mappedDataSet = IDMappingUtils.mapDataSetWithIdMapping(in1.getDataSet(), nodeMapping,
				new int[] {node0Idx, node1Idx});
			mappedNodeTypes = IDMappingUtils.mapDataSetWithIdMapping(in2.getDataSet(), nodeMapping,
				new int[] {vertexIdx});
		} else {
			mappedDataSet = in1.getDataSet();
			mappedNodeTypes = in2.getDataSet();
		}

		GraphPartitionFunction graphPartitionFunction = new GraphPartitionHashFunction();
		// do preprocessing
		DataSet <Tuple3 <Long, Long, Double>> parsedEdge = mappedDataSet.flatMap(
			new ParseGraphData(node0Idx, node1Idx, valueIdx, isToUnDigraph))
			.name("parsedEdge");
		DataSet <Tuple2 <Long, Character>> parsedVertexType = mappedNodeTypes.map(
			new ParseNodeType(vertexIdx, typeIdx))
			.name("parsedVertexType");

		DataSet <Tuple4 <Long, Long, Double, Character>> edgeWithDstType = parsedEdge.coGroup(parsedVertexType)
			.where(1).equalTo(0)
			.with(new ConcatDstNodeType())
			.name("edgeWithDstType");

		DataSet <GraphEdge> parsedAndPartitionedAndSortedGraph =
			edgeWithDstType.coGroup(parsedVertexType).where(0).equalTo(0)
				.with(new ConcatSrcNodeType())
				.partitionCustom(new GraphPartitioner(graphPartitionFunction), 0)
				.sortPartition(0, Order.ASCENDING)
				.sortPartition(4, Order.ASCENDING)
				.map(new ConstructHeteEdge())
			.name("parsedAndPartitionedAndSortedGraph");

		DataSet <GraphStatistics> graphStatistics = parsedAndPartitionedAndSortedGraph.mapPartition(
			new ComputeGraphStatistics())
			.name("graphStatistics");
		long graphStorageHandler = IterTaskObjKeeper.getNewHandle();
		long randomWalkStorageHandler = IterTaskObjKeeper.getNewHandle();
		long walkWriteBufferHandler = IterTaskObjKeeper.getNewHandle();
		long walkReadBufferHandler = IterTaskObjKeeper.getNewHandle();
		// walkWriteBufferHandler and walkReadBufferHandler points to the same object

		final ExecutionEnvironment currentEnv = MLEnvironmentFactory.get(getMLEnvironmentId())
			.getExecutionEnvironment();
		DataSet <MetaPathCommunicationUnit> initData = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(1L, currentEnv.getParallelism()),
				BasicTypeInfo.LONG_TYPE_INFO)
			.map(new MapFunction <Long, MetaPathCommunicationUnit>() {
				@Override
				public MetaPathCommunicationUnit map(Long value) throws Exception {
					return new MetaPathCommunicationUnit(1, 1, null, null, null);
				}
			})
			.name("initData");
		IterativeDataSet <MetaPathCommunicationUnit> loop = initData.iterate(Integer.MAX_VALUE).name("loop");

		DataSet <MetaPathCommunicationUnit> cachedGraphAndRandomWalk = parsedAndPartitionedAndSortedGraph.mapPartition(
			new CacheGraphAndRandomWalk(graphStorageHandler, randomWalkStorageHandler, walkWriteBufferHandler,
				walkReadBufferHandler, walkNum, walkLength, isWeightedSampling, getSamplingMethod(),
				metaPathList, graphPartitionFunction))
			.withBroadcastSet(graphStatistics, GRAPH_STATISTICS)
			.withBroadcastSet(loop, "loop")
			.name("cachedGraphAndRandomWalk");

		// get neighbors for remote sampling
		DataSet <MetaPathCommunicationUnit> sendCommunicationUnit = cachedGraphAndRandomWalk.mapPartition(
			new GetMessageToSend(graphStorageHandler, randomWalkStorageHandler, walkWriteBufferHandler,
				graphPartitionFunction)
		).name("sendCommunicationUnit");

		DataSet <MetaPathCommunicationUnit> recvCommunicationUnit = sendCommunicationUnit
			.partitionCustom(new GraphPartitioner(graphPartitionFunction),
				new SendRequestKeySelector <MetaPathCommunicationUnit>())
			.mapPartition(new DoRemoteProcessing(graphStorageHandler))
			.partitionCustom(new GraphPartitioner(graphPartitionFunction),
				new RecvRequestKeySelector <MetaPathCommunicationUnit>())
			.name("recvCommunicationUnit");

		DataSet <MetaPathCommunicationUnit> finishedOneStep = recvCommunicationUnit.mapPartition(
			new HandleReceivedMessage <MetaPathCommunicationUnit>(randomWalkStorageHandler)
		).name("finishedOneStep");

		DataSet <Object> termination = sendCommunicationUnit.map(
			new MapFunction <MetaPathCommunicationUnit, Object>() {
				@Override
				public Object map(MetaPathCommunicationUnit value)
					throws Exception {
					return new Object();
				}
			})
			.name("termination");

		DataSet <MetaPathCommunicationUnit> output = loop.closeWith(finishedOneStep, termination);
		DataSet <long[]> emptyOut = output.mapPartition(
			new EndWritingRandomWalks <MetaPathCommunicationUnit>(walkWriteBufferHandler))
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
			finalOutput = mergedOutput.map(new LongArrayToRow(delimiter)).name("finalOutput");
		}
		setOutput(finalOutput, new TableSchema(new String[] {PATH_COL_NAME},
			new TypeInformation[] {Types.STRING}));

		return this;
	}

	static class CacheGraphAndRandomWalk extends RichMapPartitionFunction <GraphEdge, MetaPathCommunicationUnit> {
		long graphStorageHandler;
		long randomWalkStorageHandler;
		long walkWriteBufferHandler;
		long walkReadBufferHandler;
		int numWalkPerVertex;
		int walkLen;
		boolean isWeighted;
		String samplingMethod;
		List <char[]> metaPaths;
		GraphPartitionFunction graphPartitionFunction;

		public CacheGraphAndRandomWalk(long graphStorageHandler, long randomWalkStorageHandler,
									   long walkWriteBufferHandler,
									   long walkReadBufferHandler,
									   int numWalkPerVertex, int walkLen, boolean isWeighted, String samplingMethod,
									   List <char[]> metaPaths, GraphPartitionFunction graphPartitionFunction) {
			this.graphStorageHandler = graphStorageHandler;
			this.randomWalkStorageHandler = randomWalkStorageHandler;
			this.walkWriteBufferHandler = walkWriteBufferHandler;
			this.walkReadBufferHandler = walkReadBufferHandler;
			this.numWalkPerVertex = numWalkPerVertex;
			this.walkLen = walkLen;
			this.isWeighted = isWeighted;
			this.samplingMethod = samplingMethod;
			this.metaPaths = metaPaths;
			this.graphPartitionFunction = graphPartitionFunction;
		}

		@Override
		public void mapPartition(Iterable <GraphEdge> values, Collector <MetaPathCommunicationUnit> out)
			throws Exception {
			int superStep = getIterationRuntimeContext().getSuperstepNumber();
			int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();

			if (superStep == 1) {
				// in step 1, we cache the graph and collect workers' {logicalID -- physicalId} map.
				List <GraphStatistics> graphStatistics = getRuntimeContext().getBroadcastVariable(GRAPH_STATISTICS);
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
				HeteGraphEngine heteGraphEngine = new HeteGraphEngine(values, myStatistics.getVertexNum(),
					myStatistics.getEdgeNum(), myStatistics.getNodeTypes(), isWeighted, useAlias);
				IterTaskObjKeeper.put(graphStorageHandler, partitionId, heteGraphEngine);
				Iterator <Tuple2 <Long, Character>> allSrcVertices = heteGraphEngine.getAllSrcVerticesWithTypes();
				int randomWalkStorageSizePerVertex = this.numWalkPerVertex * this.walkLen;
				// we store at most 128MB in walks[] by default.
				int localBatchSize = (128 * 1024 * 1024) / 8
					/ randomWalkStorageSizePerVertex;
				localBatchSize = Math.min(localBatchSize, myStatistics.getVertexNum());

				MetaPathWalkPathEngine heteWalkPath = new MetaPathWalkPathEngine(localBatchSize, numWalkPerVertex,
					walkLen, allSrcVertices, metaPaths);
				IterTaskObjKeeper.put(randomWalkStorageHandler, partitionId, heteWalkPath);

				// 64MB as the buffer size
				int bufferSize = 64 * 1024 * 1024 / (walkLen * 8 + 16);
				RandomWalkMemoryBuffer randomWalkMemoryBuffer = new RandomWalkMemoryBuffer(bufferSize);
				IterTaskObjKeeper.put(walkWriteBufferHandler, partitionId, randomWalkMemoryBuffer);
				IterTaskObjKeeper.put(walkReadBufferHandler, partitionId, randomWalkMemoryBuffer);

				Iterator <Long> tmpAllSrcVertices = heteGraphEngine.getAllSrcVertices();
				if (tmpAllSrcVertices.hasNext()) {
					long oneVertexId = tmpAllSrcVertices.next();
					int hashKey = graphPartitionFunction.apply(oneVertexId, numPartitions);
					out.collect(new MetaPathCommunicationUnit(hashKey, partitionId, null, null, null));
				}

			} else if (superStep == 2) {
				// we store the hashKeyToPartitionMap in the second step
				List <MetaPathCommunicationUnit> workerIdMappingList = getRuntimeContext().getBroadcastVariable(
					"loop");
				Map <Integer, Integer> workerIdMapping = new HashMap <>(workerIdMappingList.size());
				for (MetaPathCommunicationUnit logical2physical : workerIdMappingList) {
					workerIdMapping.put(logical2physical.getSrcPartitionId(),
						logical2physical.getDstPartitionId());
				}
				HeteGraphEngine heteGraphEngine = IterTaskObjKeeper.get(graphStorageHandler, partitionId);
				assert null != heteGraphEngine;
				heteGraphEngine.setLogicalWorkerIdToPhysicalWorkerId(workerIdMapping);
			} else {
				// do nothing here.
			}

		}
	}

	/**
	 * Get message to send over the network. 1. mapping from logical worker id to physical worker id 2. metapath
	 * communication unit.
	 */
	static class GetMessageToSend
		extends RichMapPartitionFunction <MetaPathCommunicationUnit, MetaPathCommunicationUnit> {
		long graphStorageHandler;
		long randomWalkStorageHandler;
		long walkWriteBufferHandler;
		GraphPartitionFunction graphPartitionFunction;

		public GetMessageToSend(long graphStorageHandler, long randomWalkStorageHandler,
								long walkWriteBufferHandler, GraphPartitionFunction graphPartitionFunction) {
			this.graphStorageHandler = graphStorageHandler;
			this.randomWalkStorageHandler = randomWalkStorageHandler;
			this.walkWriteBufferHandler = walkWriteBufferHandler;
			this.graphPartitionFunction = graphPartitionFunction;
		}

		@Override
		public void mapPartition(Iterable <MetaPathCommunicationUnit> values,
								 Collector <MetaPathCommunicationUnit> out)
			throws Exception {
			final int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			final int superStep = getIterationRuntimeContext().getSuperstepNumber();
			if (superStep == 1) {
				// pass the {hashkey --> partitionId} map to next step.
				for (MetaPathCommunicationUnit val : values) {
					out.collect(val);
				}
			} else {
				for (MetaPathCommunicationUnit val : values) {
					// blocking the Flink JOB EDGE and force the upside finish its job.
				}
				HeteGraphEngine heteGraphEngine = IterTaskObjKeeper.get(graphStorageHandler, partitionId);
				MetaPathWalkPathEngine heteWalkPath = IterTaskObjKeeper.get(randomWalkStorageHandler, partitionId);
				RandomWalkMemoryBuffer randomWalkMemoryBuffer = IterTaskObjKeeper.get(walkWriteBufferHandler,
					partitionId);
				assert null != heteGraphEngine;
				assert null != heteWalkPath;
				assert null != randomWalkMemoryBuffer;
				Tuple2 <long[], Character[]> nextBatchOfVerticesToSampleFrom =
					heteWalkPath.getNextBatchOfVerticesToSampleFrom();

				for (int walkId = 0; walkId < nextBatchOfVerticesToSampleFrom.f0.length; walkId++) {

					// output the finished random walks by remote sampling.
					if (heteWalkPath.canOutput(walkId)) {
						long[] finishedRandomWalk = heteWalkPath.getOneWalkAndAddNewWalk(walkId);
						Tuple2 <Long, Character> newVertexToSampleFrom =
							heteWalkPath.getNextVertexToSampleFrom(walkId);
						if (null == newVertexToSampleFrom) {
							nextBatchOfVerticesToSampleFrom.f0[walkId] = -1L;
							nextBatchOfVerticesToSampleFrom.f1[walkId] = null;
						} else {
							nextBatchOfVerticesToSampleFrom.f0[walkId] = newVertexToSampleFrom.f0;
							nextBatchOfVerticesToSampleFrom.f1[walkId] = newVertexToSampleFrom.f1;
						}
						randomWalkMemoryBuffer.writeOneWalk(finishedRandomWalk);
					}

					long vertexToSample = nextBatchOfVerticesToSampleFrom.f0[walkId];
					Character typeToSample = nextBatchOfVerticesToSampleFrom.f1[walkId];
					// do local sampling when possible for efficiency.
					while (heteGraphEngine.containsVertex(vertexToSample)) {
						vertexToSample = heteGraphEngine.sampleOneNeighbor(vertexToSample, typeToSample);
						heteWalkPath.updatePath(walkId, vertexToSample);
						boolean canOutput = heteWalkPath.canOutput(walkId);
						if (canOutput) {
							long[] finishedRandomWalk = heteWalkPath.getOneWalkAndAddNewWalk(walkId);
							randomWalkMemoryBuffer.writeOneWalk(finishedRandomWalk);
						}
						// update typeToSample and vertexToSample
						Tuple2 <Long, Character> nextToSample = heteWalkPath.getNextVertexToSampleFrom(walkId);
						if (null != nextToSample) {
							vertexToSample = nextToSample.f0;
							typeToSample = nextToSample.f1;
						} else {
							vertexToSample = -1;
							typeToSample = null;
						}
					}
					nextBatchOfVerticesToSampleFrom.f0[walkId] = vertexToSample;
					nextBatchOfVerticesToSampleFrom.f1[walkId] = typeToSample;
				}

				// partition these request to other workers
				int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
				List <Long>[] vertexSplitPieces = new ArrayList[numPartitions];
				List <Character>[] typesSplitPieces = new ArrayList[numPartitions];
				List <Integer>[] walkIdSplitPieces = new ArrayList[numPartitions];
				for (int p = 0; p < numPartitions; p++) {
					vertexSplitPieces[p] = new ArrayList <>();
					typesSplitPieces[p] = new ArrayList <>();
					walkIdSplitPieces[p] = new ArrayList <>();
				}

				for (int walkId = 0; walkId < nextBatchOfVerticesToSampleFrom.f0.length; walkId++) {
					if (nextBatchOfVerticesToSampleFrom.f0[walkId] == -1) {
						// we don't have much node to sample by now --- the buffer is not full.
						continue;
					}
					int logicalId = graphPartitionFunction.apply(
						nextBatchOfVerticesToSampleFrom.f0[walkId], numPartitions);
					int physicalId = heteGraphEngine.getPhysicalWorkerIdByLogicalWorkerId(logicalId);
					vertexSplitPieces[physicalId].add(nextBatchOfVerticesToSampleFrom.f0[walkId]);
					typesSplitPieces[physicalId].add(nextBatchOfVerticesToSampleFrom.f1[walkId]);
					walkIdSplitPieces[physicalId].add(walkId);
				}

				for (int dstPid = 0; dstPid < numPartitions; dstPid++) {
					if (walkIdSplitPieces[dstPid].size() != 0) {
						out.collect(new MetaPathCommunicationUnit(partitionId, dstPid,
							walkIdSplitPieces[dstPid].toArray(new Integer[0]),
							vertexSplitPieces[dstPid].toArray(new Long[0]),
							typesSplitPieces[dstPid].toArray(new Character[0])));
					}
				}

			}
		}
	}

	static class DoRemoteProcessing
		extends RichMapPartitionFunction <MetaPathCommunicationUnit, MetaPathCommunicationUnit> {

		long graphStorageHandler;

		public DoRemoteProcessing(long graphStorageHandler) {
			this.graphStorageHandler = graphStorageHandler;
		}

		@Override
		public void mapPartition(Iterable <MetaPathCommunicationUnit> values,
								 Collector <MetaPathCommunicationUnit> out)
			throws Exception {
			// we could further do sampling here if the sampled vertex is here. But it will introduce complex
			// data structure here. we leave it as future work.
			final int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
				// pass the {hashkey --> partitionId} map to next step.
				for (MetaPathCommunicationUnit communicationUnit : values) {
					out.collect(communicationUnit);
				}
			} else {
				HeteGraphEngine heteGraphEngine = IterTaskObjKeeper.get(graphStorageHandler, partitionId);
				assert null != heteGraphEngine;
				for (MetaPathCommunicationUnit metaPathCommunicationUnit : values) {
					assert metaPathCommunicationUnit.getDstPartitionId() == partitionId;
					Long[] verticesToSample = metaPathCommunicationUnit.getRequestedVertexIds();
					Character[] typesToSample = metaPathCommunicationUnit.getVertexTypes();
					for (int vertexCnt = 0; vertexCnt < verticesToSample.length; vertexCnt++) {
						if (heteGraphEngine.containsVertex(verticesToSample[vertexCnt])) {
							verticesToSample[vertexCnt] = heteGraphEngine.sampleOneNeighbor(
								verticesToSample[vertexCnt], typesToSample[vertexCnt]);

						} else {
							// if this vertex should be partitioned to this worker via GraphPartition function, but it
							// has no output neighbors.
							verticesToSample[vertexCnt] = -1L;
						}
					}
					// ${vertexTypes} is useless for the update process, so set is as null reduce network traffic.
					metaPathCommunicationUnit.deleteVertexTypes();
					out.collect(metaPathCommunicationUnit);
				}
			}
		}
	}

	static class MetaPathCommunicationUnit extends RandomWalkCommunicationUnit implements Serializable {
		/**
		 * used in hete
		 */
		Character[] vertexTypes;

		public MetaPathCommunicationUnit(int srcPartitionId, int dstPartitionId, Integer[] walkIds,
										 Long[] requestedVertexIds,
										 Character[] vertexTypes) {
			super(srcPartitionId, dstPartitionId, walkIds, requestedVertexIds);
			this.vertexTypes = vertexTypes;
		}

		public Character[] getVertexTypes() {
			return vertexTypes;
		}

		public void deleteVertexTypes() {
			this.vertexTypes = null;
		}
	}

	static class ParseNodeType implements MapFunction <Row, Tuple2 <Long, Character>> {
		int vertexIdx, typeIdx;

		public ParseNodeType(int vertexIdx, int typeIdx) {
			this.vertexIdx = vertexIdx;
			this.typeIdx = typeIdx;
		}

		@Override
		public Tuple2 <Long, Character> map(Row value) throws Exception {
			long vertexId = ((Number) value.getField(vertexIdx)).longValue();
			Character vertexType = ((String) value.getField(typeIdx)).charAt(0);
			return Tuple2.of(vertexId, vertexType);
		}
	}

	static class ConcatDstNodeType implements CoGroupFunction <Tuple3 <Long, Long, Double>, Tuple2 <Long, Character>,
		Tuple4 <Long, Long, Double, Character>> {
		@Override
		public void coGroup(Iterable <Tuple3 <Long, Long, Double>> first,
							Iterable <Tuple2 <Long, Character>> second,
							Collector <Tuple4 <Long, Long, Double, Character>> out)
			throws Exception {
			Tuple2 <Long, Character> vertexIdAndType = second.iterator().next();
			for (Tuple3 <Long, Long, Double> edge : first) {
				out.collect(Tuple4.of(edge.f0, edge.f1, edge.f2, vertexIdAndType.f1));
			}
		}
	}

	static class ConcatSrcNodeType
		implements CoGroupFunction <Tuple4 <Long, Long, Double, Character>, Tuple2 <Long, Character>,
		Tuple5 <Long, Long, Double, Character, Character>> {
		@Override
		public void coGroup(Iterable <Tuple4 <Long, Long, Double, Character>> first,
							Iterable <Tuple2 <Long, Character>> second,
							Collector <Tuple5 <Long, Long, Double, Character, Character>> out)
			throws Exception {
			Tuple2 <Long, Character> vertexIdAndType = second.iterator().next();
			for (Tuple4 <Long, Long, Double, Character> edge : first) {
				// we do not collect GraphEdge here because KeySelector cannot be chained.
				out.collect(
					Tuple5.of(edge.f0, edge.f1, edge.f2, vertexIdAndType.f1, edge.f3));
			}
		}
	}
}