package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.common.linalg.VectorUtil;
//import com.alibaba.alink.common.utils.AlinkViz;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.agnes.Agnes;
import com.alibaba.alink.operator.common.clustering.agnes.AgnesCluster;
import com.alibaba.alink.operator.common.clustering.agnes.AgnesModelInfoBatchOp;
import com.alibaba.alink.operator.common.clustering.agnes.AgnesSample;
import com.alibaba.alink.operator.common.clustering.agnes.Linkage;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.params.clustering.AgnesParams;

import java.util.ArrayList;
import java.util.List;

/**
 * @author guotao.gt
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT),
	@PortSpec(value = PortType.EVAL_METRICS),
})
@ReservedColsWithSecondInputSpec
@ParamSelectColumnSpec(name = "vectorCol", portIndices = 0, allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@ParamSelectColumnSpec(name = "idCol", portIndices = 0)
@NameCn("Agnes")
@NameEn("Agnes")
public final class AgnesBatchOp extends BatchOperator <AgnesBatchOp>
	implements AgnesParams <AgnesBatchOp>,
	//AlinkViz <AgnesBatchOp>,
	WithModelInfoBatchOp <AgnesModelInfoBatchOp.AgnesModelSummary, AgnesBatchOp, AgnesModelInfoBatchOp> {

	private static final long serialVersionUID = -7069169801410116405L;

	public AgnesBatchOp() {
		super(null);
	}

	public AgnesBatchOp(Params params) {
		super(params);
	}

	@Override
	public AgnesBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		final int k = this.getParams().get(K);
		final double distanceThreshold = getParams().get(DISTANCE_THRESHOLD);
		final DistanceType distanceType = get(DISTANCE_TYPE);
		final Linkage linkage = getParams().get(LINKAGE);
		ContinuousDistance distance = distanceType.getFastDistance();

		if (k <= 1 && distanceThreshold == Double.MAX_VALUE) {
			throw new RuntimeException("k should larger than 1,or distanceThreshold should be set");
		}
		TypeInformation idType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), this.getIdCol());

		DataSet <AgnesSample> data = in.select(new String[] {this.getIdCol(), this.getVectorCol()}).getDataSet()
			.map(new MapFunction <Row, AgnesSample>() {
				private static final long serialVersionUID = -4667000522433310128L;
				@Override
				public AgnesSample map(Row row) throws Exception {
					String idColValue = row.getField(0).toString();
					// the default clusterID is set as 0, will be set later.
					return new AgnesSample(idColValue, 0, VectorUtil.getDenseVector(row.getField(1)), 1.0);
				}
			});

		DataSet <AgnesCluster> clusters = data.mapPartition(
			new AgnesKernel(distanceThreshold, k, distance, linkage))
			.setParallelism(1);
		// start get cluster result
		DataSet <Row> dataRow = clusters.flatMap(
			new TransferClusterResult(idType));

		DataSet <Row> mergeInfo = clusters.flatMap(new MergeInfo(idType));

		TableSchema outputSchema = new TableSchema(new String[] {this.getIdCol(), this.getPredictionCol()},
			new TypeInformation[] {
				idType, AlinkTypes.LONG});

		this.setOutput(dataRow, outputSchema);
		this.setSideOutputTables(new Table[] {
			DataSetConversionUtil.toTable(getMLEnvironmentId(), mergeInfo,
				new TableSchema(new String[] {"NodeId", "MergeIteration", "ParentId"}, new TypeInformation[] {
					idType, AlinkTypes.LONG, idType}))
		});

		this.setOutput(dataRow, outputSchema);
		return this;
	}

	public static class AgnesKernel implements MapPartitionFunction <AgnesSample, AgnesCluster> {
		private static final long serialVersionUID = 886248302149838023L;
		private double distanceThreshold;
		private int k;
		private ContinuousDistance distance;
		private Linkage linkage;

		public AgnesKernel(double distanceThreshold, int k, ContinuousDistance distance, Linkage linkage) {
			this.distanceThreshold = distanceThreshold;
			this.k = k;
			this.distance = distance;
			this.linkage = linkage;
		}

		@Override
		public void mapPartition(Iterable <AgnesSample> values, Collector <AgnesCluster> out) throws Exception {
			List <AgnesSample> samples = new ArrayList <>();
			for (AgnesSample sample : values) {
				samples.add(sample);
			}

			List <AgnesCluster> clusters = Agnes.startAnalysis(samples, k, distanceThreshold, linkage, distance);
			for (AgnesCluster cluster : clusters) {
				out.collect(cluster);
			}
		}
	}

	public static class TransferClusterResult implements FlatMapFunction <AgnesCluster, Row> {
		private static final long serialVersionUID = 531203134457473817L;
		private long clusterId = 0;
		private TypeInformation idType;

		public TransferClusterResult(TypeInformation idType) {
			this.idType = idType;
		}

		@Override
		public void flatMap(AgnesCluster cluster, Collector <Row> out) throws Exception {
			for (AgnesSample dp : cluster.getAgnesSamples()) {
				out.collect(Row.of(EvaluationUtil.castTo(dp.getSampleId(), idType), clusterId));
			}
			clusterId++;
		}
	}

	public static class MergeInfo implements FlatMapFunction <AgnesCluster, Row> {
		private static final long serialVersionUID = 531203134457473817L;
		private TypeInformation idType;

		public MergeInfo(TypeInformation idType) {
			this.idType = idType;
		}

		@Override
		public void flatMap(AgnesCluster cluster, Collector <Row> out) throws Exception {
			for (AgnesSample dp : cluster.getAgnesSamples()) {
				out.collect(
					Row.of(EvaluationUtil.castTo(dp.getSampleId(), idType), dp.getMergeIter(), dp.getParentId()));
			}
		}
	}

	@Override
	public AgnesModelInfoBatchOp getModelInfoBatchOp() {
		return new AgnesModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
