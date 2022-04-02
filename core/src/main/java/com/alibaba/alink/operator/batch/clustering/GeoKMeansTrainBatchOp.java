package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansModelDataConverter;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.distance.HaversineDistance;
import com.alibaba.alink.params.clustering.GeoKMeansTrainParams;
import com.alibaba.alink.params.shared.clustering.HasKMeansWithHaversineDistanceType;

import static com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp.iterateICQ;
import static com.alibaba.alink.operator.common.clustering.kmeans.KMeansInitCentroids.initKmeansCentroids;

/**
 * This version of kmeans support haversine distance, which is used to calculate the great-circle distance.
 * <p>
 * (https://en.wikipedia.org/wiki/Haversine_formula)
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL, desc = PortDesc.KMEANS_MODEL)})
@ParamSelectColumnSpec(name = "latitudeCol", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@ParamSelectColumnSpec(name = "longitudeCol", portIndices = 0, allowedTypeCollections = {TypeCollections.NUMERIC_TYPES})
@NameCn("经纬度K均值聚类训练")
public final class GeoKMeansTrainBatchOp extends BatchOperator <GeoKMeansTrainBatchOp>
	implements GeoKMeansTrainParams <GeoKMeansTrainBatchOp> {
	private static final long serialVersionUID = 1190784726768283432L;

	public GeoKMeansTrainBatchOp() {
		this(null);
	}

	public GeoKMeansTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public GeoKMeansTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String latitudeColName = this.getLatitudeCol();
		final String longitudeColName = this.getLongitudeCol();
		FastDistance distance = new HaversineDistance();

		final int maxIter = this.getMaxIter();
		final double tol = this.getEpsilon();

		DataSet <FastDistanceVectorData> data = in
			.select(new String[] {latitudeColName, longitudeColName})
			.getDataSet()
			.rebalance()
			.map(new MapFunction <Row, FastDistanceVectorData>() {
				private static final long serialVersionUID = -5236022856006527961L;

				@Override
				public FastDistanceVectorData map(Row row) {
					Vector vec = new DenseVector(new double[] {((Number) row.getField(0)).doubleValue(),
						((Number) row.getField(1)).doubleValue()});
					return distance.prepareVectorData(Row.of(vec), 0);
				}
			});

		DataSet <Integer> vectorSize = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
			.fromElements(2);

		// Tuple3: clusterId, clusterWeight, clusterCentroid
		DataSet <FastDistanceMatrixData> initCentroid = initKmeansCentroids(data, distance, this.getParams(),
			vectorSize, getRandomSeed());

		DataSet <Row> finalCentroid = iterateICQ(initCentroid, data,
			vectorSize, maxIter, tol, distance, HasKMeansWithHaversineDistanceType.DistanceType.HAVERSINE, null,
			this.getLatitudeCol(),
			this.getLongitudeCol());

		// store the clustering model to the table
		this.setOutput(finalCentroid, new KMeansModelDataConverter().getModelSchema());

		return this;
	}

}
