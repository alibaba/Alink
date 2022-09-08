package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.memory.MemoryVertexCentricIteration;
import com.alibaba.alink.operator.batch.graph.memory.MemoryComputeFunction;
import com.alibaba.alink.params.graph.PageRankParams;

import java.util.Iterator;

/**
 * This Op computes the pagerank value given a directed graph.
 */
@InputPorts(values = {
	@PortSpec(value = PortType.DATA, opType = OpType.BATCH, desc = PortDesc.GRPAH_EDGES)
})
@OutputPorts(values = @PortSpec(value = PortType.DATA))
@ParamSelectColumnSpec(name = "edgeSourceCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeTargetCol", portIndices = 0)
@ParamSelectColumnSpec(name = "edgeWeightCol", portIndices = 0)
@NameCn("PageRank算法")
public class PageRankBatchOp extends BatchOperator <PageRankBatchOp>
	implements PageRankParams <PageRankBatchOp> {

	public PageRankBatchOp(Params params) {
		super(params);
	}

	public PageRankBatchOp() {
		this(new Params());
	}

	@Override
	public PageRankBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] outCols = new String[] {"vertex", "label"};

		String[] inputColNames = in.getColNames();
		int sourceIdx = TableUtil.findColIndexWithAssertAndHint(inputColNames, getEdgeSourceCol());
		int targetIdx = TableUtil.findColIndexWithAssertAndHint(inputColNames, getEdgeTargetCol());
		String edgeWeightCol = getEdgeWeightCol();
		String[] selectedCols;
		if (edgeWeightCol != null) {
			selectedCols = new String[] {getEdgeSourceCol(), getEdgeTargetCol(), edgeWeightCol};
		} else {
			selectedCols = new String[] {getEdgeSourceCol(), getEdgeTargetCol()};
		}
		TypeInformation <?> vertexType = in.getColTypes()[sourceIdx];
		Preconditions.checkState(vertexType == in.getColTypes()[targetIdx],
			"The source and target should be the same type.");

		DataSet <Row> result = MemoryVertexCentricIteration.runAndGetVertices(
			in.select(selectedCols).getDataSet(),
			vertexType,
			edgeWeightCol != null,
			false,
			getMLEnvironmentId(),
			getMaxIter(),
			new PageRankComputeFunction(getDampingFactor(), getEpsilon()));

		DataSet <Double> sumPageRank = result.map(new MapFunction <Row, Double>() {
			@Override
			public Double map(Row value) throws Exception {
				return (Double) value.getField(1);
			}
		}).reduce(new ReduceFunction <Double>() {
			@Override
			public Double reduce(Double value1, Double value2) throws Exception {
				return value1 + value2;
			}
		});

		result = result.map(new RichMapFunction <Row, Row>() {

			double sum;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				sum = (Double) getRuntimeContext().getBroadcastVariable("sumPageRank").get(0);
			}

			@Override
			public Row map(Row value) throws Exception {
				return Row.of(value.getField(0), (Double) value.getField(1) / sum);
			}
		}).withBroadcastSet(sumPageRank, "sumPageRank");

		this.setOutput(result, outCols,
			new TypeInformation <?>[] {vertexType, BasicTypeInfo.DOUBLE_TYPE_INFO});
		return this;
	}

	private static class PageRankComputeFunction extends MemoryComputeFunction {
		private final double dampingFactor;
		private final double epsilon;

		public PageRankComputeFunction(double dampingFactor, double epsilon) {
			this.dampingFactor = dampingFactor;
			this.epsilon = epsilon;
		}

		@Override
		public void gatherMessage(long vertexId, double message) {
			incCurVertexValue(vertexId, message * dampingFactor);
		}

		@Override
		public void sendMessage(Iterator <Tuple2 <Long, Double>> neighborAndValues, long vertexId) {
			int step = getSuperStep();
			if (step == 1) {
				if (neighborAndValues.hasNext()) {
					double oldVal = getLastStepVertexValue(vertexId);
					while (neighborAndValues.hasNext()) {
						Tuple2 <Long, Double> nv = neighborAndValues.next();
						sendMessageTo(nv.f0, oldVal * nv.f1);
					}
				} else {
					sendMessageTo(-1, 1. / getGraphContext().numVertex);
				}

			} else {
				double newVal = getCurVertexValue(vertexId);
				double oldVal = getLastStepVertexValue(vertexId);
				double diff = newVal - oldVal;
				if (Math.abs(diff) / newVal > epsilon) {
					if (neighborAndValues.hasNext()) {
						while (neighborAndValues.hasNext()) {
							Tuple2 <Long, Double> nv = neighborAndValues.next();
							sendMessageTo(nv.f0, diff * nv.f1);
						}
					} else {
						// dangling vertices.
						sendMessageTo(-1, diff / getGraphContext().numVertex);
					}
				}
			}

		}

		@Override
		public void initVerticesValues() {
			setAllVertexValues(1.);
		}

		@Override
		public void initEdgesValues() {
			normalizeEdgeValuesByVertex();
		}
	}
}