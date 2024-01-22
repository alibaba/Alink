package com.alibaba.alink.operator.local.clustering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.outlier.DbscanDetector.UnionJoin;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.clustering.DbscanLocalParams;
import com.alibaba.alink.params.clustering.DbscanTrainParams;
import com.alibaba.alink.params.feature.HasNumHashTables;
import com.alibaba.alink.params.feature.HasNumProjectionsPerTable;
import com.alibaba.alink.params.feature.HasProjectionWidth;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.clustering.Dbscan")
public class DbscanTrainLocalOp extends LocalOperator <DbscanTrainLocalOp>
	implements DbscanTrainParams <DbscanTrainLocalOp> {
	private static final int MAX_CONSIDERED_NEIGHBOR_NUM = 128;
	private static final int MAX_ACCURATE_DISTANCE_NUM = 1000000;
	private static final String NEIGHBOR_COL_NAME = "NEIGHBOR_COL";
	
	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		if (!getParams().contains(DbscanLocalParams.RADIUS)) {
			throw new AkIllegalOperatorParameterException("missing radius of Dbscan.");
		}
		if (!getParams().contains(DbscanLocalParams.TOP_N)) {
			throw new AkIllegalOperatorParameterException("missing topN of Dbscan.");
		}

		String vecCol = getSelectedCol();
		String idCol = getIdCol();
		int minPoints = getTopN();

		int idIndex = TableUtil.findColIndex(in.getSchema(), idCol);
		TypeInformation idType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), idCol);
		int numData = in.getOutputTable().getNumRow();
		int topN = Math.min(MAX_CONSIDERED_NEIGHBOR_NUM, numData - 1);
		topN = Math.max(topN, minPoints);

		Map <Object, Integer> objectIntegerMap = new HashMap <>();
		if (!idType.getTypeClass().equals(Integer.class)) {
			objectIntegerMap = new HashMap <>(numData);
			int idCnt = 0;
			for (Row row : in.getOutputTable().getRows()) {
				objectIntegerMap.put(row.getField(idIndex), idCnt++);
			}
		}

		Boolean useApproxAlgo = getParams().contains(HasNumHashTables.NUM_HASH_TABLES) &&
			getParams().contains(HasNumProjectionsPerTable.NUM_PROJECTIONS_PER_TABLE) &&
			getParams().contains(HasProjectionWidth.PROJECTION_WIDTH) ? true :
			numData > MAX_ACCURATE_DISTANCE_NUM ? true : false;
		LocalOperator neighbors = DbscanLocalOp.calculateNeighbors(in, in, topN, useApproxAlgo, getParams(),
			getParams());

		int nnPredictColIndex = TableUtil.findColIndex(neighbors.getSchema(), NEIGHBOR_COL_NAME);
		UnionJoin unionJoin = new UnionJoin(numData);
		Set <Integer> corePointSet = new HashSet <>();

		DbscanLocalOp.calculateCores(neighbors, corePointSet, unionJoin, objectIntegerMap, idType, nnPredictColIndex,
			idIndex,
			minPoints);
		Map <Integer, Integer> denseClusterId = DbscanLocalOp.generateDenseClusterId(corePointSet, unionJoin);

		int vecColIdx = TableUtil.findColIndex(in.getSchema(), vecCol);
		Row[] resultRows = new Row[corePointSet.size() + 1];
		resultRows[0] = Row.of(getParams().toJson());

		int resultNum = 1;
		for (int i : corePointSet) {
			int clusterId = denseClusterId.get(unionJoin.find(i));
			if (corePointSet.contains(i)) {
				resultRows[resultNum++] = Row.of(
					String.format("{\"center\":\"%s\",\"clusterId\":%d}",
						VectorUtil.toString((Vector) in.getOutputTable().getRow(i).getField(vecColIdx)),
						clusterId)
				);
			}
		}
		this.setOutputTable(new MTable(resultRows, "modelInfo string"));
	}
}
