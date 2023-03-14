package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.VizDataWriterInterface;
import com.alibaba.alink.common.viz.VizOpChartData;
import com.alibaba.alink.common.viz.VizOpDataInfo;
import com.alibaba.alink.common.viz.VizOpMeta;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.dataproc.FirstReducer;
import com.alibaba.alink.params.graph.VertexNeighborSearchParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * This algorithm implements vertex neighbor search.
 * It returns an induced sub-graph whose vertices are within most k-hops of sources vertices.
 *
 * @author Fan Hong
 */
@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES),
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRAPH_VERTICES, isOptional = true)
})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)
})
@ParamSelectColumnSpec(name = "vertexIdCol", portIndices = 1)
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)

@NameCn("点邻居搜索")
@NameEn("Vertex Neighbor Search")
public final class VertexNeighborSearchBatchOp extends BatchOperator <VertexNeighborSearchBatchOp>
	implements VertexNeighborSearchParams <VertexNeighborSearchBatchOp>, AlinkViz <VertexNeighborSearchBatchOp> {

	private static final long serialVersionUID = 4341880061845091326L;
	static int MAX_NUM_EDGES_TO_ES = 1200;

	public VertexNeighborSearchBatchOp() {
		super(new Params());
	}

	public VertexNeighborSearchBatchOp(Params params) {
		super(params);
	}

	@Override
	public VertexNeighborSearchBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		VizDataWriterInterface writer = this.getVizDataWriter();

		// Parse parameters and inputs
		BatchOperator <?> edgesOperator = inputs[0];

		Boolean isUndirected = getAsUndirectedGraph();
		String vertexIdColName = getVertexIdCol();
		String edgeSourceColName = getEdgeSourceCol();
		String edgeTargetColName = getEdgeTargetCol();

		int edgeSourceColId = TableUtil.findColIndexWithAssertAndHint(edgesOperator.getColNames(), edgeSourceColName);
		int edgeTargetColId = TableUtil.findColIndexWithAssertAndHint(edgesOperator.getColNames(), edgeTargetColName);

		int depth = getDepth();
		HashSet <String> sources = new HashSet <>(Arrays.asList(getSources()));

		DataSet <Tuple3 <String, String, Long>> edges = edgesOperator.getDataSet()
			.map(new Row2EdgeTuple(edgeSourceColId, edgeTargetColId));

		// Construct graph
		Graph <String, Long, Long> graph;
		BatchOperator <?> verticesOperator = null;
		int vertexIdColId = 0;
		DataSet <Tuple2 <String, Long>> vertices = null;
		if (inputs.length > 1) {
			verticesOperator = inputs[1];
			vertexIdColId = TableUtil.findColIndexWithAssertAndHint(verticesOperator.getColNames(), vertexIdColName);
			vertices = verticesOperator.getDataSet().map(new Row2VertexTuple(vertexIdColId));
			graph = Graph.fromTupleDataSet(vertices, edges,
				MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment());
		} else {
			graph = Graph.fromTupleDataSet(edges, new VertexValueInitializer(),
				MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment());
		}

		if (isUndirected) {
			graph = graph.getUndirected();
		}

		// Vertex neighbor search algorithm
		Graph subgraph = graph;
		try {
			subgraph = new VertexNeighborSearch(sources, depth).run(graph);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Filter and output vertices and edges from original data to keep additional attributes
		DataSet <Row> inducedEdges = subgraph.getEdgesAsTuple3()
			.joinWithHuge(edgesOperator.getDataSet())
			.where(0, 1)
			.equalTo(0, 1)
			.with(new OnlySecondJoinFunction());
		this.setOutput(inducedEdges, edgesOperator.getSchema());
		Table[] sideOutputs = new Table[1];
		this.setSideOutputTables(new Table[1]);
		DataSet <Row> inducedVertices = null;
		String[] outVerticesColNames = null;
		if (verticesOperator != null) {
			inducedVertices = subgraph.getVerticesAsTuple2()
				.join(verticesOperator.getDataSet())
				.where(0)
				.equalTo(0)
				.with(new OnlySecondJoinFunction());
			outVerticesColNames = verticesOperator.getColNames();
			sideOutputs[0] = DataSetConversionUtil.toTable(getMLEnvironmentId(), inducedVertices,
				verticesOperator.getSchema()

			);
		} else {
			// Even there is no input tables for vertices, we still create an output one
			inducedVertices = subgraph.getVerticesAsTuple2()
				.project(0)
				.map(new MapFunction <Tuple1 <String>, Row>() {
					private static final long serialVersionUID = 599089156563158818L;

					@Override
					public Row map(Tuple1 <String> o) {
						return Row.of(o.f0);
					}
				});
			outVerticesColNames = new String[] {"name"};
			vertexIdColName = "name";
			sideOutputs[0] = DataSetConversionUtil.toTable(getMLEnvironmentId(), inducedVertices,
				new String[] {"String"},
				new TypeInformation <?>[] {AlinkTypes.STRING});
		}

		this.setSideOutputTables(sideOutputs);
		// Construct and write results
		if (writer != null) {
			/**
			 * Write all information about edges and vertices in to VizWriter.
			 * However, if #edges is larger than MAX_NUM_EDGES_TO_ES,
			 * only first `MAX_NUM_EDGES_TO_ES` edges and no vertices are written.
			 */
			final DataSet <String> visWriterResult = inducedEdges
				.reduceGroup(new FirstReducer <>(MAX_NUM_EDGES_TO_ES))
				.combineGroup(new AllInOneGroupCombineFunction <Row>())
				.cross(inducedVertices.combineGroup(new AllInOneGroupCombineFunction <Row>()))
				.with(new Graph2JsonCrossFunction(sources,
					edgesOperator.getColNames(), edgeSourceColName, edgeTargetColName,
					outVerticesColNames, vertexIdColName,
					isUndirected, MAX_NUM_EDGES_TO_ES))
				.map(new VizWriterMapFunction(0, writer));
			DataSetUtil.linkDummySink(visWriterResult);

			// Write meta to VizDataWriter
			VizOpMeta meta = new VizOpMeta();
			meta.dataInfos = new VizOpDataInfo[1];
			meta.dataInfos[0] = new VizOpDataInfo(0);

			meta.cascades = new HashMap <>();
			meta.cascades.put(
				gson.toJson(new String[] {"图可视化"}),
				new VizOpChartData(0));

			meta.setSchema(edgesOperator.getSchema());
			meta.params = getParams();
			meta.isOutput = false;
			meta.opName = "VertexNeighborSearchBatchOp";

			writer.writeBatchMeta(meta);
		}

		return this;
	}

	public static class VertexValueInitializer implements MapFunction <String, Long> {
		private static final long serialVersionUID = -8771018283053295267L;

		@Override
		public Long map(String s) throws Exception {
			return 0L;
		}
	}

	public static class Row2EdgeTuple implements MapFunction <Row, Tuple3 <String, String, Long>> {
		private static final long serialVersionUID = -7430905996667254712L;
		private int sourceColId;
		private int targetColId;

		Row2EdgeTuple(int sourceColId, int targetColId) {
			this.sourceColId = sourceColId;
			this.targetColId = targetColId;
		}

		@Override
		public Tuple3 <String, String, Long> map(Row value) throws Exception {
			return Tuple3.of((String) value.getField(sourceColId), (String) value.getField(targetColId), 0L);
		}
	}

	public static class Row2VertexTuple implements MapFunction <Row, Tuple2 <String, Long>> {
		private static final long serialVersionUID = -1149958337899075070L;
		private int vertexColId;

		Row2VertexTuple(int vertexColId) {
			this.vertexColId = vertexColId;
		}

		@Override
		public Tuple2 <String, Long> map(Row value) throws Exception {
			return Tuple2.of((String) value.getField(vertexColId), 0L);
		}
	}

	public static class OnlySecondJoinFunction<IN1, IN2> implements JoinFunction <IN1, IN2, IN2> {
		private static final long serialVersionUID = 5961146867726046621L;

		@Override
		public IN2 join(IN1 in1, IN2 in2) {
			return in2;
		}
	}

	public static class AllInOneGroupCombineFunction<T> implements GroupCombineFunction <T, List <T>> {
		private static final long serialVersionUID = -7055580437134926663L;

		@Override
		public void combine(Iterable <T> iterable, Collector <List <T>> collector) {
			List <T> list = new ArrayList <>();
			for (T t : iterable) {
				list.add(t);
			}
			collector.collect(list);
		}
	}

	public static class Graph2JsonCrossFunction
		implements CrossFunction <List <Row>, List <Row>, String>, AlinkSerializable {
		private static final long serialVersionUID = -6978604589815110412L;
		private Set <String> selectedVertexIds;
		private String[] edgesColNames;
		private String edgeSourceColName;
		private String edgeTargetColName;
		private String[] verticesColNames;
		private String vertexIdColName;
		private boolean isUndirected;
		private int maxNumEdgesToEs;

		private Object[][] edges;
		private Object[][] vertices;

		Graph2JsonCrossFunction(Set <String> selectedVertexIds,
								String[] edgesColNames, String edgeSourceColName, String edgeTargetColName,
								String[] verticesColNames, String vertexIdColName,
								boolean isUndirected, int maxNumEdgesToEs) {
			this.selectedVertexIds = selectedVertexIds;
			this.edgesColNames = edgesColNames;
			this.edgeSourceColName = edgeSourceColName;
			this.edgeTargetColName = edgeTargetColName;
			this.verticesColNames = verticesColNames;
			this.vertexIdColName = vertexIdColName;
			this.isUndirected = isUndirected;
			this.maxNumEdgesToEs = maxNumEdgesToEs;
		}

		@Override
		public String cross(List <Row> edges, List <Row> vertices) {

			List <Object[]> edgesList = new ArrayList <>();

			int counter = 0;
			Set <String> relatedVerticesSet = new HashSet <>(edges.size() * 2);
			for (Row row : edges) {
				Object[] obj = new Object[edgesColNames.length];
				for (int i = 0; i < edgesColNames.length; i += 1) {
					obj[i] = row.getField(i);
				}
				edgesList.add(obj);
				relatedVerticesSet.add((String) obj[0]);
				relatedVerticesSet.add((String) obj[1]);
				counter += 1;
				if (counter >= maxNumEdgesToEs) {    // do not write too many edges to es
					break;
				}
			}
			this.edges = new Object[edgesList.size()][];
			edgesList.toArray(this.edges);

			List <Object[]> verticesList = new ArrayList <>();
			for (Row row : vertices) {
				Object[] obj = new Object[verticesColNames.length];
				for (int i = 0; i < verticesColNames.length; i += 1) {
					obj[i] = row.getField(i);
				}
				if (relatedVerticesSet.contains(obj[0])) {
					verticesList.add(obj);
				}
			}
			this.vertices = new Object[verticesList.size()][];
			verticesList.toArray(this.vertices);
			return gson.toJson(this);
		}
	}

	public static class VizWriterMapFunction implements MapFunction <String, String> {
		private static final long serialVersionUID = -3562810264038340464L;
		int dataId;
		VizDataWriterInterface writer;

		VizWriterMapFunction(int dataId, VizDataWriterInterface writer) {
			this.dataId = dataId;
			this.writer = writer;
		}

		@Override
		public String map(String s) throws Exception {
			writer.writeBatchData(dataId, s, System.currentTimeMillis());
			return s;
		}
	}

	/**
	 * This class implements a graph algorithm that returns a sub-graph induced by
	 * a node list `sources` and their neighbors with at most `depth` hops.
	 */

	public static class VertexNeighborSearch implements
		GraphAlgorithm <String, Long, Long, Graph <String, Long, Long>> {

		private int depth;
		private HashSet <String> sources;

		public VertexNeighborSearch(HashSet <String> sources, int depth) {
			this.sources = sources;
			this.depth = depth;
		}

		@Override
		public Graph <String, Long, Long> run(Graph <String, Long, Long> graph) throws Exception {
			Graph <String, Long, Long> subgraph = graph
				.translateVertexValues(new SetLongMaxValue())
				.runVertexCentricIteration(new VertexNeighborComputeFunction(sources), new MinimumDistanceCombiner(),
					depth + 1)
				.filterOnVertices(new FilterByValue(depth));

			DataSet <Vertex <String, Long>> vertices = subgraph.getVertices();
			return subgraph;
		}

		public static final class SetLongMaxValue implements TranslateFunction <Long, Long> {
			private static final long serialVersionUID = 6439208445273249327L;

			@Override
			public Long translate(Long aLong, Long o) {
				return Long.MAX_VALUE / 2L;
			}
		}

		public static final class FilterByValue implements FilterFunction <Vertex <String, Long>> {
			private static final long serialVersionUID = -3443337881858305297L;
			private int thresh;

			FilterByValue(int thresh) {
				this.thresh = thresh;
			}

			@Override
			public boolean filter(Vertex <String, Long> vertex) {
				return vertex.getValue() <= thresh;
			}
		}

		public static final class VertexNeighborComputeFunction extends ComputeFunction <String, Long, Long, Long> {
			private static final long serialVersionUID = -4927352871373053814L;
			private HashSet <String> sources;

			VertexNeighborComputeFunction(HashSet <String> sources) {
				this.sources = sources;
			}

			@Override
			public void compute(Vertex <String, Long> vertex, MessageIterator <Long> messages) {
				long minDistance = sources.contains(vertex.getId()) ? 0L : Long.MAX_VALUE / 2;

				for (Long msg : messages) {
					minDistance = Math.min(minDistance, msg);
				}

				if (minDistance < vertex.getValue()) {
					setNewVertexValue(minDistance);
					for (Edge <String, Long> e : getEdges()) {
						sendMessageTo(e.getTarget(), minDistance + 1);
					}
				}
			}
		}

		public static final class MinimumDistanceCombiner extends MessageCombiner <String, Long> {

			private static final long serialVersionUID = 1916706983491173310L;

			public void combineMessages(MessageIterator <Long> messages) {

				long minMessage = Long.MAX_VALUE / 2;
				for (Long msg : messages) {
					minMessage = Math.min(minMessage, msg);
				}
				sendCombinedMessage(minMessage);
			}
		}
	}
}
