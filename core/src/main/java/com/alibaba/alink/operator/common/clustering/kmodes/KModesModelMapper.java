package com.alibaba.alink.operator.common.clustering.kmodes;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.distance.OneZeroDistance;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;

import java.util.List;

public class KModesModelMapper extends ModelMapper {

	private static final long serialVersionUID = 1212257106447281392L;
	private final boolean isPredDetail;
	private KModesModelData modelData;
	private int[] colIdx;
	private final OneZeroDistance distance = new OneZeroDistance();

	public KModesModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		isPredDetail = params.contains(ClusteringPredictParams.PREDICTION_DETAIL_COL);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelData = new KModesModel().load(modelRows);

		colIdx = new int[modelData.featureColNames.length];

		for (int i = 0; i < modelData.featureColNames.length; i++) {
			colIdx[i] = TableUtil.findColIndexWithAssert(getDataSchema().getFieldNames(), modelData
				.featureColNames[i]);
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		String[] record = new String[colIdx.length];
		for (int i = 0; i < record.length; i++) {
			record[i] = (String.valueOf(selection.get(colIdx[i])));
		}

		Tuple2 <Long, Double> tuple2 = getCluster(modelData.centroids, record, distance);
		result.set(0, tuple2.f0);
		if (isPredDetail) {
			result.set(1, tuple2.f1);
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {
		boolean isPredDetail = params.contains(ClusteringPredictParams.PREDICTION_DETAIL_COL);
		String[] resultCols = isPredDetail ? new String[] {params.get(ClusteringPredictParams.PREDICTION_COL),
			params.get(ClusteringPredictParams.PREDICTION_DETAIL_COL)} : new String[] {params.get(
			ClusteringPredictParams.PREDICTION_COL)};
		TypeInformation[] resultTypes = isPredDetail ? new TypeInformation[] {Types.LONG, Types.DOUBLE}
			: new TypeInformation[] {Types.LONG};

		return Tuple4.of(dataSchema.getFieldNames(), resultCols, resultTypes,
			params.get(ClusteringPredictParams.RESERVED_COLS));
	}

	private Tuple2 <Long, Double> getCluster(Iterable <Tuple3 <Long, Double, String[]>> centroids,
											 String[] sample,
											 OneZeroDistance oneZeroDistance) {
		long clusterId = -1;
		double d = Double.POSITIVE_INFINITY;

		for (Tuple3 <Long, Double, String[]> c : centroids) {
			double distance = oneZeroDistance.calc(sample, c.f2);
			if (distance < d) {
				clusterId = c.f0;
				d = distance;
			}
		}
		return new Tuple2 <>(clusterId, d);
	}
}
