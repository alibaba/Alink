package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

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
import com.alibaba.alink.operator.batch.graph.memory.MemoryComputeFunction;
import com.alibaba.alink.operator.batch.graph.memory.MemoryVertexCentricIteration;
import com.alibaba.alink.params.graph.KCoreParams;

import java.util.Iterator;
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES))
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@ParamSelectColumnSpec(name = "edgeSourceCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@ParamSelectColumnSpec(name = "edgeTargetCol", allowedTypeCollections = TypeCollections.INT_LONG_STRING_TYPES)
@NameCn("KCore算法")
public class KCoreBatchOp extends BatchOperator <KCoreBatchOp>
	implements KCoreParams <KCoreBatchOp> {
	private static final long serialVersionUID = -7537644695230031028L;

	public KCoreBatchOp(Params params) {
		super(params);
	}

	public KCoreBatchOp() {
		super(new Params());
	}

	@Override
	public KCoreBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] outCols = new String[] {"node1", "node2"};
		String[] inputColNames = in.getColNames();
		int sourceIdx = TableUtil.findColIndexWithAssertAndHint(inputColNames, getEdgeTargetCol());
		int targetIdx = TableUtil.findColIndexWithAssertAndHint(inputColNames, getEdgeTargetCol());
		TypeInformation <?> sourceType = in.getColTypes()[sourceIdx];
		Preconditions.checkState(sourceType == in.getColTypes()[targetIdx],
			"The source and target should be the same type.");

		DataSet <Row> result = MemoryVertexCentricIteration.runAndGetEdges(
			in.select(new String[] {getEdgeSourceCol(), getEdgeTargetCol()}).getDataSet(),
			sourceType,
			false,
			true,
			getMLEnvironmentId(),
			Integer.MAX_VALUE,
			new KcoreComputeFunction(getK()));

		result = result.flatMap(new FlatMapFunction <Row, Row>() {
			@Override
			public void flatMap(Row value, Collector <Row> out) throws Exception {
				Number number = (Number) value.getField(2);
				if (number.doubleValue() > 0) {
					out.collect(Row.of(value.getField(0), value.getField(1)));
				}
			}
		});

		this.setOutput(result, outCols, new TypeInformation <?>[] {sourceType, sourceType});
		return this;
	}

	/**
	 * For vertex values, if it is positive, it is the out degree of this vertex. If it is negative, it means that this
	 * vertex has been removed.
	 * <p>
	 * For edge values, it is invalid if its value is smaller than zero. It is valid if its value is greater than zero.
	 * <p>
	 * For messages, it is vertexId for activating/deactivating the edge.
	 */
	private static class KcoreComputeFunction extends MemoryComputeFunction {
		private final int k;
		private static final double VALID_EDGE = 1;
		private static final double INVALID_EDGE = -1;
		private static final double INVALID_VERTEX = -1;
		private static final double doubleEpsilon = 1e-7;

		public KcoreComputeFunction(int k) {
			this.k = k;
		}

		@Override
		public void gatherMessage(long vertexId, double message) {
			incCurVertexValue(vertexId, -1);
			// Since the input graph of Kcore is always undirected, we can use the sourceVertexId as the targetId.
			setEdgeValue(vertexId, (long) message, INVALID_EDGE);
		}

		@Override
		public void sendMessage(Iterator <Tuple2 <Long, Double>> neighborAndValues, long vertexId) {
			double vertexValue = getCurVertexValue(vertexId);
			if (isValidVertex(vertexValue) && vertexValue <= (k + doubleEpsilon)) {
				// deactivate this vertex and all its outgoing edges.
				long lastTargetID = -1;
				while (neighborAndValues.hasNext()) {
					Tuple2 <Long, Double> neighAndValue = neighborAndValues.next();
					if (neighAndValue.f1 > 0) {
						sendMessageTo(neighAndValue.f0, vertexId);
						if (lastTargetID != -1 && lastTargetID != neighAndValue.f0) {
							setEdgeValue(vertexId, lastTargetID, INVALID_EDGE);
						}
						lastTargetID = neighAndValue.f0;
					}
				}
				if (lastTargetID != -1) {
					setEdgeValue(vertexId, lastTargetID, INVALID_EDGE);
				}
				setCurVertexValue(vertexId, INVALID_VERTEX);
			}
		}

		private boolean isValidVertex(double vertexValue) {
			return vertexValue > INVALID_VERTEX;
		}

		@Override
		public void initVerticesValues() {
			setAllVertexValueByOutDegree();
		}

		@Override
		public void initEdgesValues() {
			setAllEdgeValues(VALID_EDGE);
		}
	}
}
