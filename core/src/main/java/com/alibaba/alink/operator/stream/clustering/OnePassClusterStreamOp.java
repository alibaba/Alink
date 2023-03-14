package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansTrainModelData;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansUtil;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.clustering.OnePassClusterParams;

import static com.alibaba.alink.operator.common.clustering.kmeans.KMeansUtil.getKMeansPredictVector;
import static com.alibaba.alink.operator.common.clustering.kmeans.KMeansUtil.getKmeansPredictColIdxs;
import static com.alibaba.alink.operator.stream.clustering.StreamingKMeansStreamOp.initModel;
import static com.alibaba.alink.operator.stream.clustering.StreamingKMeansStreamOp.outputModel;

@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.KMEANS_MODEL, opType = OpType.BATCH),
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_DATA, opType = OpType.STREAM)
})
@OutputPorts(values = {
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_RESULT, opType = OpType.STREAM),
	@PortSpec(value = PortType.MODEL, desc = PortDesc.KMEANS_MODEL, opType = OpType.STREAM)

})
@ReservedColsWithSecondInputSpec
@NameCn("一趟聚类")
@NameEn("One Pass Cluster")
public final class OnePassClusterStreamOp extends StreamOperator <OnePassClusterStreamOp>
	implements OnePassClusterParams <OnePassClusterStreamOp> {
	private static final long serialVersionUID = -9023400083161571185L;
	BatchOperator batchModel = null;

	public OnePassClusterStreamOp(BatchOperator model) {
		super(new Params());
		this.batchModel = model;
	}

	public OnePassClusterStreamOp(BatchOperator model, Params params) {
		super(params);
		this.batchModel = model;
	}

	@Override
	public OnePassClusterStreamOp linkFrom(StreamOperator <?>... inputs) {
		checkOpSize(1, inputs);
		OutputColsHelper outputColsHelper = new OutputColsHelper(inputs[0].getSchema(),
			new String[] {getPredictionCol(), getPredictionDetailCol()},
			new TypeInformation[] {Types.LONG, Types.DOUBLE}, getReservedCols());

		DataStream <Tuple2 <Row, KMeansTrainModelData>> output = inputs[0].getDataStream()
			.map(new OnePassCluster(DirectReader.collect(batchModel), outputColsHelper,
				inputs[0].getSchema().getFieldNames(), this.getEpsilon(), this.getModelOutputInterval()))
			.setParallelism(1);

		DataStream <Row> predictData = output.filter(new FilterFunction <Tuple2 <Row, KMeansTrainModelData>>() {
			private static final long serialVersionUID = -4346461233209536122L;

			@Override
			public boolean filter(Tuple2 <Row, KMeansTrainModelData> value) throws Exception {
				return value.f0 != null;
			}
		}).map(new MapFunction <Tuple2 <Row, KMeansTrainModelData>, Row>() {
			private static final long serialVersionUID = 7997807036805771891L;

			@Override
			public Row map(Tuple2 <Row, KMeansTrainModelData> value) throws Exception {
				return value.f0;
			}
		});

		DataStream <KMeansTrainModelData> model = output.filter(
			new FilterFunction <Tuple2 <Row, KMeansTrainModelData>>() {
				private static final long serialVersionUID = 4166084406562047700L;

				@Override
				public boolean filter(Tuple2 <Row, KMeansTrainModelData> value) throws Exception {
					return value.f1 != null;
				}
			}).map(new MapFunction <Tuple2 <Row, KMeansTrainModelData>, KMeansTrainModelData>() {
			private static final long serialVersionUID = -5162839831147241620L;

			@Override
			public KMeansTrainModelData map(Tuple2 <Row, KMeansTrainModelData> value) throws Exception {
				return value.f1;
			}
		});

		this.setSideOutputTables(outputModel(model, getMLEnvironmentId()));
		this.setOutput(predictData, outputColsHelper.getResultSchema());
		return this;
	}

	private static class OnePassCluster extends RichMapFunction <Row, Tuple2 <Row, KMeansTrainModelData>> {
		private static final long serialVersionUID = -8013967247690938140L;
		private OutputColsHelper outputColsHelper;
		private KMeansTrainModelData modelData;
		private DataBridge dataBridge;
		private String[] colNames;
		private int[] colIdx;
		private double epsilon;
		private ContinuousDistance distance;
		private Integer modelOutputInterval;
		private int cnt;

		@Override
		public void open(Configuration params) {
			modelData = initModel(dataBridge);
			distance = this.modelData.params.distanceType.getFastDistance();
			this.colIdx = getKmeansPredictColIdxs(this.modelData.params,
				colNames);
		}

		OnePassCluster(DataBridge dataBridge, OutputColsHelper outputColsHelper, String[] colNames, double epsilon,
					   Integer modelOutputInterval) {
			this.epsilon = epsilon;
			this.outputColsHelper = outputColsHelper;
			this.modelOutputInterval = modelOutputInterval;
			this.dataBridge = dataBridge;
			this.colNames = colNames;
			cnt = 0;
		}

		@Override
		public Tuple2 <Row, KMeansTrainModelData> map(Row row) throws Exception {
			Vector record = getKMeansPredictVector(colIdx, row);
			Row result;
			Tuple2 <Integer, Double> output = KMeansUtil.getClosestClusterIndex(this.modelData, record, distance);
			if (output.f1 < epsilon) {
				DenseVector vec = modelData.getClusterVector(output.f0);
				double weight = modelData.getClusterWeight(output.f0);
				modelData.setClusterWeight(output.f0, weight + 1);
				vec.scaleEqual(weight / (weight + 1));
				vec.plusScaleEqual(record, 1 / weight);
				result = Row.of((long) output.f0, output.f1);
			} else {
				int length = modelData.centroids.size();
				DenseVector vec;
				if (record instanceof SparseVector) {
					vec = ((SparseVector) record).toDenseVector();
				} else {
					vec = (DenseVector) record;
				}
				KMeansTrainModelData.ClusterSummary clusterSummary = new KMeansTrainModelData.ClusterSummary(vec,
					length, 1.0);

				modelData.centroids.add(clusterSummary);
				modelData.params.k = modelData.centroids.size();
				result = Row.of((long) length, 0.0);
			}
			if (modelOutputInterval != null) {
				cnt++;
				if (cnt == modelOutputInterval) {
					cnt = 0;
					return Tuple2.of(outputColsHelper.getResultRow(row, result), modelData);
				} else {
					return Tuple2.of(outputColsHelper.getResultRow(row, result), null);
				}
			} else {
				return Tuple2.of(outputColsHelper.getResultRow(row, result), null);
			}
		}
	}
}
