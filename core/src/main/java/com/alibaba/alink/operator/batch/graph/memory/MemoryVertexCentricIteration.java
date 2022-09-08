package com.alibaba.alink.operator.batch.graph.memory;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionFunction;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitionHashFunction;
import com.alibaba.alink.operator.batch.graph.utils.GraphPartition.GraphPartitioner;
import com.alibaba.alink.operator.batch.graph.utils.IDMappingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * A memory computing framework for graph implemented based on Flink DataSet runtime. It:
 * <i>Adopts edge cut, which means that edge value can be updated, but cannot be shared among workers. Vertex values
 * can be updated and shared among all workers.
 * <i>Maintains all the vertex values and edge values in memory, such that no extra disk scan for these values are
 * needed during the iterations.
 * <i>Caches the input graph outside the main iteration, such that no extra disk scan for the input graph are needed
 * during the main loop.
 * <i>Supports vertex ID as Long, vertex VALUE as DOUBLE, edge VALUE as Double, message VALUE as DOUBLE.
 *
 * <p>Implementation details:
 * <i>Stores graph in an CSR format, with java primitive arrays.
 * <i>Buffers the communication unit as 32KB.
 *
 * <p>Users need to imlement a {@link MemoryComputeFunction} to use this graph framework.
 */
public class MemoryVertexCentricIteration {
	private final static String CACHE_LOOP_NAME = "cache_loop";
	private static final Logger LOG = LoggerFactory.getLogger(MemoryVertexCentricIteration.class);
	public final static int NUM_CACHE_STEPS = 2;
	public final static int NUM_SETUP_STEPS = 1;

	public static DataSet <Row> runAndGetEdges(DataSet <Row> edges,
											   TypeInformation <?> vertexType,
											   boolean hasWeight,
											   boolean isToUnDigraph,
											   long mlEnvironmentId,
											   int maxIter,
											   MemoryComputeFunction memoryComputeFunction) {
		return run(edges, vertexType, hasWeight, isToUnDigraph, mlEnvironmentId,
			maxIter, memoryComputeFunction, false);
	}

	public static DataSet <Row> runAndGetVertices(DataSet <Row> edges,
												  TypeInformation <?> vertexType,
												  boolean hasWeight,
												  boolean isToUnDigraph,
												  long mlMLEnvironmentId,
												  int maxIter,
												  MemoryComputeFunction memoryComputeFunction) {
		return run(edges, vertexType, hasWeight, isToUnDigraph, mlMLEnvironmentId,
			maxIter, memoryComputeFunction, true);
	}

	private static DataSet <Row> run(DataSet <Row> edges,
									 TypeInformation <?> vertexType,
									 boolean hasWeight,
									 boolean isToUnDigraph,
									 long mlMLEnvironmentId,
									 int maxIter,
									 MemoryComputeFunction memoryComputeFunction,
									 boolean needOutputVertices) {
		if (!vertexType.equals(BasicTypeInfo.INT_TYPE_INFO) && !vertexType.equals(BasicTypeInfo.LONG_TYPE_INFO)
			&& !vertexType.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
			throw new RuntimeException("Unsupported vertex type. Supported types are [int, long, string].");
		}
		DataSet <Tuple2 <String, Long>> nodeMapping = null;
		DataSet <Row> mappedDataSet;
		boolean needRemapping = !(vertexType == BasicTypeInfo.LONG_TYPE_INFO
			|| vertexType == BasicTypeInfo.INT_TYPE_INFO);
		if (needRemapping) {
			nodeMapping = IDMappingUtils.computeIdMapping(edges, new int[] {0, 1});
			mappedDataSet = IDMappingUtils.mapDataSetWithIdMapping(edges, nodeMapping,
				new int[] {0, 1});
		} else {
			mappedDataSet = edges;
		}

		GraphPartitionFunction graphPartitionFunction = new GraphPartitionHashFunction();
		final ExecutionEnvironment currentEnv = MLEnvironmentFactory.get(mlMLEnvironmentId)
			.getExecutionEnvironment();

		/* -------------------------Cache Loop Start-------------------------*/
		// The final output of this loop is Tuple2<numEdge, numVertex> in each partition.
		// handler for storing graphs in cache loop.
		long graphStorageHandler = IterTaskObjKeeper.getNewHandle();
		IterativeDataSet <Tuple2 <Integer, Integer>> cacheLoop = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(1L, currentEnv.getParallelism()),
				BasicTypeInfo.LONG_TYPE_INFO)
			.map(new MapFunction <Long, Tuple2 <Integer, Integer>>() {
				@Override
				public Tuple2 <Integer, Integer> map(Long value) {
					return Tuple2.of(0, 0);
				}
			}).iterate(NUM_CACHE_STEPS)
			.name("cacheLoop");
		DataSet <Tuple3 <Long, Long, Double>> tupleEdges = mappedDataSet
			.map(new ParseEdge(0, 1, hasWeight ? 2 : -1))
			.withBroadcastSet(cacheLoop, CACHE_LOOP_NAME);

		DataSet <Either <Long, Tuple3 <Long, Long, Double>>> partitionedIdOrEdges = tupleEdges.flatMap(
				new EmitSrcDstIdPlusOnesOrEdges(isToUnDigraph))
			.name("EmitSrcDstIdPlusOnesOrEdges")
			.partitionCustom(new GraphPartitioner(graphPartitionFunction), new EitherKeySelector())
			.name("PartitionIdsAndEdges");
		DataSet <Tuple2 <Integer, Integer>> vertexNumAndEdgeNum = partitionedIdOrEdges.mapPartition(
				new ComputeMetaOrCacheGraph(graphStorageHandler, isToUnDigraph))
			.name("computeGraphMetaOrCacheGraph");
		DataSet <Tuple2 <Integer, Integer>> cachedEnd = cacheLoop.closeWith(vertexNumAndEdgeNum);
		/* -------------------------Cache Loop End-------------------------*/

		/* -------------------Graph Computing Loop Start-------------------*/
		// handler for storing the states during graph computing.
		long graphStateHandler = IterTaskObjKeeper.getNewHandle();
		DataSet <GraphCommunicationUnit> computeLoopStart = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(1L, currentEnv.getParallelism()),
				BasicTypeInfo.LONG_TYPE_INFO)
			.map(new MapFunction <Long, GraphCommunicationUnit>() {
				@Override
				public GraphCommunicationUnit map(Long value) {
					return new GraphCommunicationUnit(0, null, null);
				}
			})
			.name("computeLoopStart");
		// avoids possible overflow for num iterations.
		IterativeDataSet <GraphCommunicationUnit> computeLoop = computeLoopStart.iterate(
				Math.max(maxIter, maxIter + NUM_SETUP_STEPS))
			.name("loop");
		DataSet <GraphCommunicationUnit> sendCommunicationUnit = computeLoop.mapPartition(
				new GetMessageToSend(graphStorageHandler, graphStateHandler, graphPartitionFunction, memoryComputeFunction)
			)
			.withBroadcastSet(cachedEnd, CACHE_LOOP_NAME)
			.name("sendCommunicationUnit");
		DataSet <Object> termination = sendCommunicationUnit.mapPartition(
			new MapPartitionFunction <GraphCommunicationUnit, Object>() {
				@Override
				public void mapPartition(Iterable <GraphCommunicationUnit> values, Collector <Object> out) {
					if (values.iterator().hasNext()) {
						out.collect(new Object());
					}
				}
			}).name("termination");
		DataSet <GraphCommunicationUnit> finishedOneStep = sendCommunicationUnit
			.partitionCustom(new GraphPartitioner(graphPartitionFunction),
				new KeySelector <GraphCommunicationUnit, Long>() {
					@Override
					public Long getKey(GraphCommunicationUnit value) {
						return (long) (value.targetWorkerId);
					}
				})
			.mapPartition(new HandleReceivedMessage(graphStateHandler, memoryComputeFunction)
			).name("handleReceivedMessage");
		DataSet <GraphCommunicationUnit> finalResult = computeLoop.closeWith(finishedOneStep, termination);
		/* -------------------Graph Computing Loop Start-------------------*/

		DataSet <Row> outputVertices = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(0L, currentEnv.getParallelism()),
				BasicTypeInfo.LONG_TYPE_INFO)
			.mapPartition(new OutputVertexValues(graphStateHandler))
			.withBroadcastSet(finalResult, "finalResult")
			.name("memoryOut");
		DataSet <Row> outputEdges = currentEnv
			.fromParallelCollection(new NumberSequenceIterator(0L, currentEnv.getParallelism()),
				BasicTypeInfo.LONG_TYPE_INFO)
			.mapPartition(new OutputEdgeValues(graphStateHandler))
			.withBroadcastSet(finalResult, "finalResult")
			.name("memoryOut");
		if (needRemapping) {
			outputVertices = IDMappingUtils.recoverDataSetWithIdMapping(outputVertices, nodeMapping, new int[] {0});
			outputEdges = IDMappingUtils.recoverDataSetWithIdMapping(outputEdges, nodeMapping, new int[] {0, 1});
		}
		if (vertexType.equals(BasicTypeInfo.INT_TYPE_INFO)) {
			outputVertices = outputVertices.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					int src = ((Number) value.getField(0)).intValue();
					return Row.of(src, value.getField(1));
				}
			});
			outputEdges = outputEdges.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					int src = ((Number) value.getField(0)).intValue();
					int dst = ((Number) value.getField(1)).intValue();
					return Row.of(src, dst, value.getField(2));
				}
			});
		}
		if (needOutputVertices) {
			return outputVertices;
		} else {
			return outputEdges;
		}
	}

	/**
	 * sourceIdx, dstIdx, weightIdx
	 */
	private static class ParseEdge
		implements MapFunction <Row, Tuple3 <Long, Long, Double>> {
		private final int sourceIdx;
		private final int targetIdx;
		private final int weightIdx;

		ParseEdge(int sourceIdx, int targetIdx, int weightIdx) {
			this.sourceIdx = sourceIdx;
			this.targetIdx = targetIdx;
			this.weightIdx = weightIdx;
		}

		@Override
		public Tuple3 <Long, Long, Double> map(Row value) throws Exception {
			long source = ((Number) value.getField(sourceIdx)).longValue();
			long target = ((Number) value.getField(targetIdx)).longValue();
			double weight = -1;
			if (weightIdx != -1) {
				weight = ((Number) value.getField(weightIdx)).doubleValue();
			}
			return Tuple3.of(source, target, weight);
		}
	}

	private static class OutputVertexValues extends RichMapPartitionFunction <Long, Row> {
		private final long graphStorageHandler;

		public OutputVertexValues(long graphStorageHandler) {
			this.graphStorageHandler = graphStorageHandler;
		}

		@Override
		public void mapPartition(Iterable <Long> values, Collector <Row> out) throws Exception {
			int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
			MemoryEdgeListGraph graph = null;
			for (int pid = 0; pid < numTasks; pid++) {
				graph = IterTaskObjKeeper.containsAndRemoves(graphStorageHandler, pid);
				if (graph != null) {
					break;
				}
			}
			Preconditions.checkNotNull(graph);
			long start = System.currentTimeMillis();
			LOG.info("[GraphLog] Starts output vertices: " + start);
			long[] vertexSet = graph.orderedVertices;
			double[] vertexVal = graph.curVertexValues;
			for (int i = 0; i < vertexSet.length; i++) {
				out.collect(Row.of(vertexSet[i], vertexVal[i]));
			}
			LOG.info("[GraphLog] Finishes output vertices: " + (System.currentTimeMillis() - start));
		}
	}

	private static class OutputEdgeValues extends RichMapPartitionFunction <Long, Row> {
		private final long graphStorageHandler;

		public OutputEdgeValues(long graphStorageHandler) {
			this.graphStorageHandler = graphStorageHandler;
		}

		@Override
		public void mapPartition(Iterable <Long> values, Collector <Row> out) throws Exception {
			int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
			MemoryEdgeListGraph graph = null;
			for (int pid = 0; pid < numTasks; pid++) {
				graph = IterTaskObjKeeper.containsAndRemoves(graphStorageHandler, pid);
				if (graph != null) {
					break;
				}
			}
			Preconditions.checkNotNull(graph);
			long start = System.currentTimeMillis();
			LOG.info("[GraphLog] Starts output edges: " + start);
			long[] vertexSet = graph.orderedVertices;
			for (int localIdx = 0; localIdx < vertexSet.length; localIdx++) {
				int localStart = localIdx == 0 ? 0 : graph.srcEnds[localIdx - 1];
				int localEnd = graph.srcEnds[localIdx];
				for (int i = localStart; i < localEnd; i++) {
					out.collect(Row.of(vertexSet[localIdx], graph.dsts[i], graph.edgeValues[i]));
				}
			}
			LOG.info("[GraphLog] Finishes output edges: " + (System.currentTimeMillis() - start));
		}
	}

	private static class HandleReceivedMessage
		extends RichMapPartitionFunction <GraphCommunicationUnit, GraphCommunicationUnit> {
		private final long graphStateHandler;
		private MemoryComputeFunction udf;

		public HandleReceivedMessage(long graphStateHandler, MemoryComputeFunction udf) {
			this.graphStateHandler = graphStateHandler;
			this.udf = udf;
		}

		@Override
		public void mapPartition(Iterable <GraphCommunicationUnit> values, Collector <GraphCommunicationUnit> out)
			throws Exception {
			final int superStep = getIterationRuntimeContext().getSuperstepNumber();
			if (superStep == 1) {
				for (GraphCommunicationUnit communicationUnit : values) {
					out.collect(communicationUnit);
				}
			} else {
				int partitionId = getRuntimeContext().getIndexOfThisSubtask();
				int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
				MemoryEdgeListGraph memoryEdgeListGraph = IterTaskObjKeeper.get(graphStateHandler, partitionId);
				Preconditions.checkNotNull(memoryEdgeListGraph);
				udf.setup(memoryEdgeListGraph,
					out,
					numPartitions,
					null,
					memoryEdgeListGraph.graphContext,
					getIterationRuntimeContext().getSuperstepNumber());

				for (GraphCommunicationUnit graphCommunicationUnit : values) {
					long[] receivedVertexIds = graphCommunicationUnit.vertexIds;
					double[] receivedVertexValues = graphCommunicationUnit.vertexValues;
					if (receivedVertexIds == null) {
						// deal with broadcast message
						long[] vertexSet = memoryEdgeListGraph.orderedVertices;
						for (int i = 0; i < vertexSet.length; i++) {
							for (double recVal : receivedVertexValues) {
								udf.gatherMessage(vertexSet[i], recVal);
							}
						}
					} else {
						// deal with non-broadcast message
						for (int i = 0; i < receivedVertexIds.length; i++) {
							udf.gatherMessage(receivedVertexIds[i], receivedVertexValues[i]);
						}
					}
				}
			}
		}
	}

	private static class GetMessageToSend
		extends RichMapPartitionFunction <GraphCommunicationUnit, GraphCommunicationUnit> {
		/**
		 * The handler that stores cached graphs. Will be removed in the first iteration.
		 */
		private final long graphStorageHandler;
		/**
		 * The handler that stores graph states during graph computations. Will be added in the first iteration.
		 */
		private final long graphStateHandler;
		private final GraphPartitionFunction graphPartitionFunction;
		private MemoryComputeFunction udf;

		public GetMessageToSend(long graphStorageHandler,
								long graphStateHandler,
								GraphPartitionFunction graphPartitionFunction,
								MemoryComputeFunction udf) {
			this.graphStorageHandler = graphStorageHandler;
			this.graphStateHandler = graphStateHandler;
			this.graphPartitionFunction = graphPartitionFunction;
			this.udf = udf;
		}

		@Override
		public void mapPartition(Iterable <GraphCommunicationUnit> values,
								 Collector <GraphCommunicationUnit> out)
			throws Exception {
			final int partitionId = getRuntimeContext().getIndexOfThisSubtask();
			final int superStep = getIterationRuntimeContext().getSuperstepNumber();
			final int numPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
			if (superStep == 1) {
				MemoryEdgeListGraph memoryEdgeListGraph = null;
				for (int pid = 0; pid < numPartitions; pid++) {
					memoryEdgeListGraph = IterTaskObjKeeper.containsAndRemoves(graphStorageHandler, pid);
					if (memoryEdgeListGraph != null) {
						break;
					}
				}
				Preconditions.checkNotNull(memoryEdgeListGraph);
				IterTaskObjKeeper.put(graphStateHandler, partitionId, memoryEdgeListGraph);
				List <Tuple2 <Integer, Integer>> metaInfo = getRuntimeContext().getBroadcastVariable(
					CACHE_LOOP_NAME);
				long numVertex = 0;
				long numEdge = 0;
				for (Tuple2 <Integer, Integer> edgeNumAndVertexNum : metaInfo) {
					numVertex += edgeNumAndVertexNum.f1;
					numEdge += edgeNumAndVertexNum.f0;
				}
				memoryEdgeListGraph.setGraphContext(new DistributedGraphContext(numVertex, numEdge));
				udf.setup(memoryEdgeListGraph,
					null,
					getRuntimeContext().getNumberOfParallelSubtasks(),
					graphPartitionFunction,
					memoryEdgeListGraph.graphContext,
					superStep);
				/* ---------------------user function start---------------------*/
				udf.initVerticesValues();
				memoryEdgeListGraph.overrideLastStepVertexValues();
				udf.initEdgesValues();
				/* ---------------------user function start---------------------*/

				long[] vertex = memoryEdgeListGraph.orderedVertices;
				long[] logicalIdAndPhysicalId = new long[] {graphPartitionFunction.apply(vertex[0],
					numPartitions), partitionId};
				for (int i = 0; i < numPartitions; i++) {
					out.collect(new GraphCommunicationUnit(i, logicalIdAndPhysicalId, null));
				}
			} else {
				long start = System.currentTimeMillis();
				MemoryEdgeListGraph memoryEdgeListGraph = IterTaskObjKeeper.get(graphStateHandler,
					partitionId);
				Preconditions.checkNotNull(memoryEdgeListGraph);
				if (superStep == 2) {
					HashMap <Integer, Integer> logical2physical = new HashMap <>(numPartitions);
					for (GraphCommunicationUnit communicationUnit : values) {
						long[] logiAndPhy = communicationUnit.vertexIds;
						logical2physical.put((int) logiAndPhy[0], (int) logiAndPhy[1]);
					}
					memoryEdgeListGraph.setLogicalWorkerId2PhysicalWorkerId(logical2physical);
				}
				udf.setup(memoryEdgeListGraph,
					out,
					numPartitions,
					graphPartitionFunction,
					memoryEdgeListGraph.graphContext,
					superStep);
				/* ---------------------user function start---------------------*/
				long[] vertexSet = memoryEdgeListGraph.orderedVertices;
				for (int i = 0; i < vertexSet.length; i++) {
					long src = vertexSet[i];
					udf.sendMessage(memoryEdgeListGraph.getNeighborsWithValue(src), src);
				}
				udf.flushMessages();
				memoryEdgeListGraph.overrideLastStepVertexValues();
				/* ---------------------user function end---------------------*/
				LOG.info(
					"[GraphLog] TaskId: " + getRuntimeContext().getIndexOfThisSubtask() + ", CurrentSuperStep is: "
						+ (superStep - NUM_SETUP_STEPS)
						+ ", time is: " + System.currentTimeMillis() + ", elapsed: " + (System.currentTimeMillis()
						- start));
			}
		}
	}

	private static class EmitSrcDstIdPlusOnesOrEdges
		extends RichFlatMapFunction <Tuple3 <Long, Long, Double>, Either <Long, Tuple3 <Long, Long, Double>>> {
		private final boolean isToUnDigraph;

		public EmitSrcDstIdPlusOnesOrEdges(boolean isToUnDigraph) {
			this.isToUnDigraph = isToUnDigraph;
		}

		@Override
		public void flatMap(Tuple3 <Long, Long, Double> edge,
							Collector <Either <Long, Tuple3 <Long, Long, Double>>> out) throws Exception {
			int superStep = getIterationRuntimeContext().getSuperstepNumber();
			if (superStep == 1) {
				// assume all vertex ids are greater than or equal to zero.
				// plus vertexId by one to identify ID zero.
				// source vertex.
				out.collect(org.apache.flink.types.Either.Left(edge.f0 + 1));
				// target vertex.
				out.collect(org.apache.flink.types.Either.Left(-(edge.f1 + 1)));
			} else {
				out.collect(org.apache.flink.types.Either.Right(edge));
				if (isToUnDigraph) {
					out.collect(org.apache.flink.types.Either.Right(Tuple3.of(edge.f1, edge.f0, edge.f2)));
				}
			}
		}
	}

	private static class ComputeMetaOrCacheGraph
		extends RichMapPartitionFunction <Either <Long, Tuple3 <Long, Long, Double>>, Tuple2 <Integer, Integer>> {
		private int superStep;
		private int taskId;
		private final long graphStorageHandler;
		private final boolean isToUnDigraph;

		public ComputeMetaOrCacheGraph(long graphStorageHandler, boolean isToUnDigraph) {
			this.graphStorageHandler = graphStorageHandler;
			this.isToUnDigraph = isToUnDigraph;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			superStep = getIterationRuntimeContext().getSuperstepNumber();
			taskId = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void mapPartition(Iterable <Either <Long, Tuple3 <Long, Long, Double>>> values,
								 Collector <Tuple2 <Integer, Integer>> out) throws Exception {

			if (superStep == 1) {
				long startTime = System.currentTimeMillis();
				LOG.info("[GraphLog] Starts computing vertexNum and edgeNum, " + startTime);
				long edgeNum = 0;
				// assume we have enough memory for vertices.
				HashSet <Long> vertexSet = new HashSet <>();
				for (Either <Long, Tuple3 <Long, Long, Double>> value : values) {
					long vertexId = value.left();
					if (vertexId > 0) {
						vertexSet.add(vertexId - 1);
						edgeNum++;
					} else {
						vertexSet.add(-vertexId - 1);
						if (isToUnDigraph) {
							edgeNum++;
						}
					}
				}
				Preconditions.checkState(edgeNum <= Integer.MAX_VALUE,
					"Number of edges on a single worker exceeds Integer.MAX_VALUE, Please use more workers.");
				long[] vertices = vertexSet.stream().mapToLong(Long::longValue).toArray();
				Arrays.sort(vertices);
				IterTaskObjKeeper.put(graphStorageHandler, taskId, Tuple2.of((int) edgeNum, vertices));
				LOG.info(
					"[GraphLog] Finishes computing vertexNum and edgeNum, " + (System.currentTimeMillis()
						- startTime));
			} else if (superStep == 2) {
				// Caches the graph on each worker, and sends out number of vertex and edges on each worker.
				long startTime = System.currentTimeMillis();
				LOG.info("[GraphLog] Starts loading graphs, " + startTime);
				Tuple2 <Integer, long[]> edgeNumAndOrderedVertices = IterTaskObjKeeper.get(graphStorageHandler,
					taskId);
				Preconditions.checkNotNull(edgeNumAndOrderedVertices);
				MemoryEdgeListGraph memoryEdgeListGraph = new MemoryEdgeListGraph(edgeNumAndOrderedVertices.f1,
					edgeNumAndOrderedVertices.f0);
				memoryEdgeListGraph.loadGraph(values);
				IterTaskObjKeeper.put(graphStorageHandler, taskId, memoryEdgeListGraph);
				out.collect(Tuple2
					.of(edgeNumAndOrderedVertices.f0, edgeNumAndOrderedVertices.f1.length));
			}
		}

	}

	private static class EitherKeySelector implements KeySelector <Either <Long, Tuple3 <Long, Long, Double>>, Long> {
		@Override
		public Long getKey(Either <Long, Tuple3 <Long, Long, Double>> value) throws Exception {
			if (value.isLeft()) {
				long id = value.left();
				if (id > 0) {
					return id - 1;
				} else {
					return -id - 1;
				}
			} else {
				return value.right().f0;
			}
		}
	}
}
