package com.alibaba.alink.operator.common.clustering.dbscan;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;

import java.util.List;

public class DbscanModelMapper extends ModelMapper {
	private static final long serialVersionUID = -3771648601253028057L;
	private DbscanModelPredictData modelData = null;
	private int colIdx;

	public DbscanModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelData = new DbscanModelDataConverter().load(modelRows);
		colIdx = TableUtil.findColIndexWithAssert(getDataSchema().getFieldNames(), modelData.vectorColName);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		long clusterId = findCluster(VectorUtil.getVector(selection.get(colIdx)));
		result.set(0, clusterId);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {
		return Tuple4.of(dataSchema.getFieldNames(), new String[] {params.get(ClusteringPredictParams.PREDICTION_COL)},
			new TypeInformation[] {Types.LONG}, params.get(ClusteringPredictParams.RESERVED_COLS));
	}

	private long findCluster(Vector vec) {
		long clusterId = -1;
		double d = Double.POSITIVE_INFINITY;

		FastDistanceVectorData sample = modelData.baseDistance.prepareVectorData(Row.of(vec), 0);

		for (FastDistanceVectorData c : modelData.coreObjects) {
			double distance = modelData.baseDistance.calc(c, sample).get(0, 0);
			if (distance < d) {
				clusterId = (long) c.getRows()[0].getField(0);
				d = distance;
			}
		}
		if (d > modelData.epsilon) {
			//noise
			clusterId = Integer.MIN_VALUE;
		}
		return clusterId;
	}
}
