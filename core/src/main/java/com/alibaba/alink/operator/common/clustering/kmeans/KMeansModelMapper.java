package com.alibaba.alink.operator.common.clustering.kmeans;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.params.clustering.KMeansPredictParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Find  the closest cluster center for every point.
 */
public class KMeansModelMapper extends ModelMapper {
	private static final long serialVersionUID = -7232694013661020935L;
	private KMeansPredictModelData modelData;
	private int[] colIdx;
	private FastDistance distance;
	private final boolean isPredDetail;
	private final boolean isPredDistance;

	public KMeansModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		isPredDetail = params.contains(KMeansPredictParams.PREDICTION_DETAIL_COL);
		isPredDistance = params.contains(KMeansPredictParams.PREDICTION_DISTANCE_COL);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Vector record;
		if (colIdx.length > 1) {
			record = new DenseVector(2);
			record.set(0, ((Number) selection.get(colIdx[0])).doubleValue());
			record.set(1, ((Number) selection.get(colIdx[1])).doubleValue());
		} else {
			record = VectorUtil.getVector(selection.get(colIdx[0]));
		}

		if (null == record) {
			result.set(0, null);
			if (isPredDetail) {
				result.set(1, null);
				if (isPredDistance) {
					result.set(2, null);
				}
			} else {
				if (isPredDistance) {
					result.set(1, null);
				}
			}
		} else {
			DenseMatrix distanceMatrix = new DenseMatrix(this.modelData.params.k, 1);
			FastDistanceVectorData vectorData = distance.prepareVectorData(Tuple2.of(record, null));
			double[] clusterDistances = KMeansUtil.getClusterDistances(
				vectorData,
				this.modelData.centroids,
				distance,
				distanceMatrix);
			int index = KMeansUtil.getMinPointIndex(clusterDistances, this.modelData.params.k);
			result.set(0, (long) index);
			if (isPredDetail) {
				double[] probs = KMeansUtil.getProbArrayFromDistanceArray(clusterDistances);
				DenseVector vec = new DenseVector(probs.length);
				for (int i = 0; i < this.modelData.params.k; i++) {
					vec.set((int) this.modelData.getClusterId(i), probs[i]);
				}
				result.set(1, vec.toString());
				if (isPredDistance) {
					result.set(2, clusterDistances[index]);
				}
			} else {
				if (isPredDistance) {
					result.set(1, clusterDistances[index]);
				}
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {
		String[] reservedColNames = params.get(KMeansPredictParams.RESERVED_COLS);
		String predResultColName = params.get(KMeansPredictParams.PREDICTION_COL);
		boolean isPredDetail = params.contains(KMeansPredictParams.PREDICTION_DETAIL_COL);
		boolean isPredDistance = params.contains(KMeansPredictParams.PREDICTION_DISTANCE_COL);
		List <String> outputCols = new ArrayList <>();
		List <TypeInformation> outputTypes = new ArrayList <>();
		outputCols.add(predResultColName);
		outputTypes.add(Types.LONG);
		if (isPredDetail) {
			outputCols.add(params.get(KMeansPredictParams.PREDICTION_DETAIL_COL));
			outputTypes.add(Types.STRING);
		}
		if (isPredDistance) {
			outputCols.add(params.get(KMeansPredictParams.PREDICTION_DISTANCE_COL));
			outputTypes.add(Types.DOUBLE);
		}
		return Tuple4.of(dataSchema.getFieldNames(), outputCols.toArray(new String[0]),
			outputTypes.toArray(new TypeInformation[0]), reservedColNames);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelData = new KMeansModelDataConverter().load(modelRows);
		this.distance = this.modelData.params.distanceType.getFastDistance();
		this.colIdx = KMeansUtil.getKmeansPredictColIdxs(this.modelData.params, getDataSchema().getFieldNames());
	}
}
