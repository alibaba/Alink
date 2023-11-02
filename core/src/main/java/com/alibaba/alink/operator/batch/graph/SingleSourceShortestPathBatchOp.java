package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Row;

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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.graph.SingleSourceShortestPathParams;
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES))
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 0)
@NameCn("单源最短路径")
@NameEn("Single Source Shortest Path")
public class SingleSourceShortestPathBatchOp extends BatchOperator <SingleSourceShortestPathBatchOp>
	implements SingleSourceShortestPathParams <SingleSourceShortestPathBatchOp> {
	private static final long serialVersionUID = -1637471953684406867L;

	public SingleSourceShortestPathBatchOp(Params params) {
		super(params);
	}

	public SingleSourceShortestPathBatchOp() {
		super(new Params());
	}

	@Override
	public SingleSourceShortestPathBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();

		String sourcePoint = getSourcePoint();
		String[] outputCols = new String[]{"vertex", "distance"};
		Integer maxIter = getMaxIter();
		Boolean directed = getAsUndirectedGraph();

		String weightCol = getEdgeWeightCol();
		boolean hasWeightCol = !(weightCol == null);
		String[] inputEdgeCols = hasWeightCol ? new String[] {sourceCol, targetCol, getEdgeWeightCol()} :
			 new String[] {sourceCol, targetCol};

		int vertexColTypeIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), sourceCol);
		TypeInformation <?> vertexType = in.getColTypes()[vertexColTypeIndex];
		DataSet<Row> inputData = GraphUtilsWithString.input2json(in, inputEdgeCols, 2, true);
		GraphUtilsWithString map = new GraphUtilsWithString(inputData, vertexType);
		DataSet <Edge <Long, Double>> edges = map
			.inputType2longEdge(inputData, hasWeightCol);

		Graph <Long, Double, Double> graphInitial = Graph
			.fromDataSet(edges, MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
			.mapVertices(new MapVertices());
		long MLEnvId = in.getMLEnvironmentId();
		DataSet <Tuple1 <Long>> mappedSource = map.string2longSource(sourcePoint, MLEnvId, vertexType);
		DataSet <Vertex <Long, Double>> vertices = graphInitial.getVertices()
			.leftOuterJoin(mappedSource).where(0).equalTo(0)
			.with(new JoinFunction <Vertex <Long, Double>, Tuple1 <Long>, Vertex <Long, Double>>() {
				private static final long serialVersionUID = 1964647721649366980L;

				@Override
				public Vertex <Long, Double> join(Vertex <Long, Double> first, Tuple1 <Long> second) throws Exception {
					if (second != null) {
						first.f1 = 0.;
					}
					return first;
				}
			});
		//        DataSet<Vertex<Long, Double>> vertices = graphInitial.getVertices().map(new InitialMap())
		//                .withBroadcastSet(mappedSource, "sourcePoint");

		Graph <Long, Double, Double> graph;
		if (directed) {
			graph = Graph.fromDataSet(vertices, edges,
				MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
				.getUndirected();
		} else {
			graph = Graph.fromDataSet(vertices, edges,
				MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment());
		}

		DataSet <Vertex <Long, Double>> resData = graph
			.runGatherSumApplyIteration(new CalculateDistances(), new ChooseMinDistance(),
				new UpdateDistance(), maxIter)
			.getVertices();
		DataSet <Row> res = map.double2outputTypeVertex(resData, Types.DOUBLE);
		this.setOutput(res, outputCols, new TypeInformation <?>[] {vertexType, Types.DOUBLE});
		return this;
	}

	public static class MapVertices implements MapFunction <Vertex <Long, NullValue>, Double> {
		private static final long serialVersionUID = -6624679629933017172L;

		@Override
		public Double map(Vertex <Long, NullValue> value) throws Exception {
			//return Double.MAX_VALUE;
			return Double.POSITIVE_INFINITY;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path UDFs
	// --------------------------------------------------------------------------------------------

	private static final class CalculateDistances extends GatherFunction <Double, Double, Double> {

		private static final long serialVersionUID = 8666543088654403877L;

		public Double gather(Neighbor <Double, Double> neighbor) {
			return neighbor.getNeighborValue() + neighbor.getEdgeValue();
		}
	}

	private static final class ChooseMinDistance extends SumFunction <Double, Double, Double> {

		private static final long serialVersionUID = 7176693441223938280L;

		public Double sum(Double newValue, Double currentValue) {
			return Math.min(newValue, currentValue);
		}
	}

	private static final class UpdateDistance extends ApplyFunction <Long, Double, Double> {

		private static final long serialVersionUID = 7753801433080130491L;

		public void apply(Double newDistance, Double oldDistance) {
			if (newDistance < oldDistance) {
				setResult(newDistance);
			}
		}
	}

}
