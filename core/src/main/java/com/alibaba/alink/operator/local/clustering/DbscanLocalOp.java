package com.alibaba.alink.operator.local.clustering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.outlier.DbscanDetector.UnionJoin;
import com.alibaba.alink.operator.common.recommendation.KObjectUtil;
import com.alibaba.alink.operator.common.similarity.Solver;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.similarity.VectorApproxNearestNeighborPredictLocalOp;
import com.alibaba.alink.operator.local.similarity.VectorApproxNearestNeighborTrainLocalOp;
import com.alibaba.alink.operator.local.similarity.VectorNearestNeighborPredictLocalOp;
import com.alibaba.alink.operator.local.similarity.VectorNearestNeighborTrainLocalOp;
import com.alibaba.alink.params.clustering.DbscanLocalParams;
import com.alibaba.alink.params.clustering.DbscanTrainParams;
import com.alibaba.alink.params.feature.HasNumHashTables;
import com.alibaba.alink.params.feature.HasNumProjectionsPerTable;
import com.alibaba.alink.params.feature.HasProjectionWidth;
import com.alibaba.alink.params.nlp.HasIdCol;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.clustering.HasFastMetric;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.similarity.HasMaxNumCandidates;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborTrainParams;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DbscanLocalOp extends LocalOperator <DbscanLocalOp>
	implements DbscanLocalParams <DbscanLocalOp> {
	private static final int MAX_CONSIDERED_NEIGHBOR_NUM = 128;
	private static final int MAX_ACCURATE_DISTANCE_NUM = 1000000;
	private static final String NEIGHBOR_COL_NAME = "NEIGHBOR_COL";
	private static String CORE_TYPE = "CORE";
	private static String NEIGHBOR_TYPE = "NEIGHBOR";
	private static String NOISE_TYPE = "NOISE";

	public DbscanLocalOp() {this(new Params());}

	public DbscanLocalOp(Params params) {super(params);}

	private int nnPredictColIndex;
	private int idIndex;

	@Override
	public DbscanLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		if (!getParams().contains(DbscanLocalParams.RADIUS)) {
			throw new AkIllegalOperatorParameterException("In DbscanLocalOp,Not have parameter: radius.");
		}
		if (!getParams().contains(DbscanLocalParams.TOP_N)) {
			throw new AkIllegalOperatorParameterException("In DbscanLocalOp,Not have parameter: topN.");
		}
		if (!getParams().contains(DbscanLocalParams.PREDICTION_COL)) {
			throw new AkIllegalOperatorParameterException("In DbscanLocalOp,Not have parameter: predictionCol.");
		}

		String vecCol = getSelectedCol();
		String idCol = getIdCol();
		int minPoints = getTopN();
		String[] keepColNames = getReservedCols();

		idIndex = TableUtil.findColIndex(in.getSchema(), idCol);
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
		LocalOperator neighbors = calculateNeighbors(in, in, topN, useApproxAlgo, getParams(), getParams());

		nnPredictColIndex = getNearestNeighborResultColIndex(neighbors);
		UnionJoin unionJoin = new UnionJoin(numData);
		Set <Integer> corePointSet = new HashSet <>();

		calculateCores(neighbors, corePointSet, unionJoin, objectIntegerMap, idType, nnPredictColIndex, idIndex,
			minPoints);
		doClustering(neighbors, corePointSet, unionJoin, objectIntegerMap, idType);
		Map <Integer, Integer> denseClusterId = generateDenseClusterId(corePointSet, unionJoin);

		if (keepColNames == null) {
			keepColNames = in.getColNames();
		}

		List <String> outputColNames = new ArrayList <>();
		List <TypeInformation> outputColTypes = new ArrayList <>();
		outputColNames.add(getPredictionCol());
		outputColTypes.add(AlinkTypes.LONG);

		OutputColsHelper outputColsHelper = new OutputColsHelper(in.getSchema(),
			outputColNames.toArray(new String[0]), outputColTypes.toArray(new TypeInformation[0]), keepColNames);
		int outputArity = keepColNames.length + 1;

		int[] keepColsIndex = TableUtil.findColIndices(in.getColNames(), keepColNames);
		Row[] resultRows = new Row[numData];
		for (int i = 0; i < numData; i++) {
			int clusterSize = unionJoin.getClusterSize(i);
			int clusterId = clusterSize < minPoints ? -1 : denseClusterId.get(unionJoin.find(i));
			resultRows[i] = new Row(outputArity);
			Row row = in.getOutputTable().getRow(i);
			for (int j = 0; j < keepColsIndex.length; j++) {
				resultRows[i].setField(j, row.getField(keepColsIndex[j]));
			}
			resultRows[i].setField(outputArity - 1, (long) clusterId);
		}

		this.setOutputTable(new MTable(
			resultRows,
			outputColsHelper.getResultSchema()
		));

		return this;
	}

	public static List <Object> extractNeighborId(String res, TypeInformation idType) {
		List <Object> list = KObjectUtil.deserializeKObject(
			res, new String[] {"ID"}, new Type[] {idType.getTypeClass()}
		).get("ID");
		if (list == null) {
			throw new AkIllegalDataException("Extract NeighborId failed.");
		}
		return list;
	}

	public static int getNearestNeighborResultColIndex(LocalOperator neighbors) {
		if (neighbors == null) {
			return -1;
		}
		return TableUtil.findColIndex(neighbors.getSchema(), NEIGHBOR_COL_NAME);
	}

	private static int convertIdToInt(Map <Object, Integer> objectIntegerMap, Object id) {
		if (objectIntegerMap.isEmpty()) {
			return (int) id;
		} else {
			return objectIntegerMap.get(id);
		}
	}

	public static void calculateCores(LocalOperator neighbors, Set <Integer> corePointSet, UnionJoin unionJoin,
									  Map <Object, Integer> objectIntegerMap, TypeInformation idType,
									  int nnPredictColIndex, int idIndex, int minPoints) {
		for (Row row : neighbors.getOutputTable().getRows()) {
			String neighborString = (String) row.getField(nnPredictColIndex);
			Object idObject = row.getField(idIndex);
			List <Object> neighborList = extractNeighborId(neighborString, idType);
			int currentId = convertIdToInt(objectIntegerMap, idObject);
			if (neighborList.size() >= minPoints) {
				corePointSet.add(currentId);
				for (Object neighbor : neighborList) {
					int neighborId = convertIdToInt(objectIntegerMap, neighbor);
					if (corePointSet.contains(neighborId)) {
						unionJoin.join(currentId, neighborId);
					}
				}
			}
		}
	}

	public static Map <Integer, Integer> generateDenseClusterId(Set <Integer> corePointSet, UnionJoin unionJoin) {
		Map <Integer, Integer> clusterIdMap = new HashMap <>();
		int currentId = 0;
		for (int i : corePointSet) {
			int clusterId = unionJoin.find(i);
			if (!clusterIdMap.containsKey(clusterId)) {
				clusterIdMap.put(clusterId, currentId);
				currentId++;
			}
		}
		return clusterIdMap;
	}

	private void doClustering(LocalOperator neighbors, Set <Integer> corePointSet, UnionJoin unionJoin,
							  Map <Object, Integer> objectIntegerMap, TypeInformation idType) {
		for (Row row : neighbors.getOutputTable().getRows()) {
			String neighborString = (String) row.getField(nnPredictColIndex);
			Object idObject = row.getField(idIndex);
			int currentId = convertIdToInt(objectIntegerMap, idObject);
			List <Object> neighborList = extractNeighborId(neighborString, idType);
			if (!corePointSet.contains(currentId)) {
				for (Object neighbor : neighborList) {
					int neighborId = convertIdToInt(objectIntegerMap, neighbor);
					if (corePointSet.contains(neighborId)) {
						unionJoin.join(currentId, neighborId);
						break;
					}
				}
			}
		}
	}

	public static LocalOperator calculateNeighbors(LocalOperator doc, LocalOperator query, int topN,
												   Boolean useApproxAlgo, Params docParams, Params queryParams) {
		LocalOperator neighbors = null;
		if (doc == null) {return null;}
		if (!useApproxAlgo) {
			LocalOperator model = new VectorNearestNeighborTrainLocalOp()
				.setSelectedCol(docParams.get(HasSelectedCol.SELECTED_COL))
				.setIdCol(docParams.get(HasIdCol.ID_COL))
				.setMetric(docParams.get(HasFastMetric.METRIC))
				.linkFrom(doc);

			neighbors = new VectorNearestNeighborPredictLocalOp()
				.setNumThreads(queryParams.get(HasNumThreads.NUM_THREADS))
				.setRadius(docParams.get(DbscanTrainParams.RADIUS))
				.setTopN(topN)
				.setSelectedCol(queryParams.get(HasSelectedCol.SELECTED_COL))
				.setOutputCol(NEIGHBOR_COL_NAME)
				.linkFrom(model, query);
		} else {
			Metric metric = docParams.get(HasFastMetric.METRIC);
			VectorApproxNearestNeighborTrainParams.Metric approxMetric = null;
			switch (metric) {
				case EUCLIDEAN:
					approxMetric = VectorApproxNearestNeighborTrainParams.Metric.EUCLIDEAN;
					break;
				case JACCARD:
					approxMetric = VectorApproxNearestNeighborTrainParams.Metric.JACCARD;
					break;
				default:
					throw new AkUnsupportedOperationException(
						"ApproxNearestNeighbor algorithm only support EUCLIDEAN and JACCARD metric.");
			}
			System.out.println("" + new Date() + " Before VectorApproxNearestNeighborTrainLocalOp");
			long start = System.currentTimeMillis();
			LocalOperator <?> model = new VectorApproxNearestNeighborTrainLocalOp()
				.setSelectedCol(docParams.get(HasSelectedCol.SELECTED_COL))
				.setIdCol(docParams.get(HasIdCol.ID_COL))
				.setMetric(approxMetric)
				.setSolver(Solver.LOCAL_LSH)
				.setNumHashTables(docParams.get(HasNumHashTables.NUM_HASH_TABLES))
				.setNumProjectionsPerTable(docParams.get(HasNumProjectionsPerTable.NUM_PROJECTIONS_PER_TABLE))
				.setProjectionWidth(docParams.get(HasProjectionWidth.PROJECTION_WIDTH))
				.setMaxNumCandidates(docParams.get(HasMaxNumCandidates.MAX_NUM_CANDIDATES))
				.setNumThreads(queryParams.get(HasNumThreads.NUM_THREADS))
				.linkFrom(doc);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"VectorApproxNearestNeighborTrainLocalOp elapsed time:" + (System.currentTimeMillis() - start));
			}
			System.out.println("" + new Date() + " Before VectorApproxNearestNeighborPredictLocalOp");
			start = System.currentTimeMillis();
			neighbors = new VectorApproxNearestNeighborPredictLocalOp()
				.setNumThreads(queryParams.get(HasNumThreads.NUM_THREADS))
				.setRadius(docParams.get(DbscanTrainParams.RADIUS))
				.setTopN(topN)
				.setSelectedCol(queryParams.get(HasSelectedCol.SELECTED_COL))
				.setOutputCol(NEIGHBOR_COL_NAME)
				.linkFrom(model, query);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"VectorApproxNearestNeighborPredictLocalOp elapsed time:" + (System.currentTimeMillis() - start));
			}
		}
		System.out.println("" + new Date() + " After ANN");
		return neighbors;
	}
}
