package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.graph.GraphUtilsWithString;
import com.alibaba.alink.params.graph.EdgeClusterCoefficientParams;
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES))
@OutputPorts(values = @PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT))
@ParamSelectColumnSpec(name = "edgeSourceCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@ParamSelectColumnSpec(name = "edgeTargetCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@NameCn("边聚类系数")
public class EdgeClusterCoefficientBatchOp extends BatchOperator <EdgeClusterCoefficientBatchOp>
	implements EdgeClusterCoefficientParams <EdgeClusterCoefficientBatchOp> {

	private static final long serialVersionUID = 7552939759062806711L;

	public EdgeClusterCoefficientBatchOp(Params params) {
		super(params);
	}

	public EdgeClusterCoefficientBatchOp() {
		super(new Params());
	}

	@Override
	public EdgeClusterCoefficientBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String sourceCol = getEdgeSourceCol();
		String targetCol = getEdgeTargetCol();
		String[] outputCols = new String[] {"node1", "node2", "neighbor1", "neighbor2", "commonNeighbor", "edgeClusterCoefficient"};
		Boolean directed = getAsUndirectedGraph();
		String[] inputEdgeCols = new String[] {sourceCol, targetCol};
		TypeInformation <?>[] inputTypes = in.getColTypes();
		int vertexColTypeIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), sourceCol);
		TypeInformation vertexType = inputTypes[vertexColTypeIndex];
		DataSet<Row> inputData = GraphUtilsWithString.input2json(in, inputEdgeCols, 2, false);
		GraphUtilsWithString map = new GraphUtilsWithString(inputData, vertexType);

		DataSet <Edge <Long, Double>> edges = map
			.inputType2longEdge(inputData, false);

		Graph <Long, Double, Double> graph;
		if (directed) {
			graph = Graph.fromDataSet(edges, MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
				.mapVertices(new MapVertices()).getUndirected();
		} else {
			graph = Graph.fromDataSet(edges, MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment())
				.mapVertices(new MapVertices());
		}
		//distinct the output undirected edges.
		DataSet <Tuple6 <Long, Long, Long, Long, Long, Double>> resTuple = new EdgeClusterCoefficient().run(graph)
			.groupBy(0)
			.reduceGroup(
				new GroupReduceFunction <Tuple6 <Long, Long, Long, Long, Long, Double>, Tuple6 <Long, Long, Long,
					Long, Long, Double>>() {
					private static final long serialVersionUID = 2757693152062829979L;

					@Override
					public void reduce(Iterable <Tuple6 <Long, Long, Long, Long, Long, Double>> values,
									   Collector <Tuple6 <Long, Long, Long, Long, Long, Double>> out) throws
						Exception {
						for (Tuple6 <Long, Long, Long, Long, Long, Double> i : values) {
							if (i.f0 > i.f1) {
								out.collect(i);
							}
						}
					}
				});
		DataSet <Row> res = map.long2outputTypeECC(resTuple);
		this.setOutput(res, outputCols,
			new TypeInformation <?>[] {vertexType, vertexType, Types.LONG,
				Types.LONG, Types.LONG, Types.DOUBLE});
		return this;
	}

	public static class MapVertices implements MapFunction <Vertex <Long, NullValue>, Double> {
		private static final long serialVersionUID = -2462169842349628527L;

		@Override
		public Double map(Vertex <Long, NullValue> value) throws Exception {
			return 1.;
		}
	}
}
