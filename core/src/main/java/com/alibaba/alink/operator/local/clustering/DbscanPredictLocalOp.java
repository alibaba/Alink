package com.alibaba.alink.operator.local.clustering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.recommendation.KObjectUtil;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.params.clustering.DbscanLocalParams;
import com.alibaba.alink.params.clustering.DbscanPredictParams;
import com.alibaba.alink.params.feature.HasNumHashTables;
import com.alibaba.alink.params.feature.HasNumProjectionsPerTable;
import com.alibaba.alink.params.feature.HasProjectionWidth;
import com.alibaba.alink.params.shared.HasNumThreads;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

public class DbscanPredictLocalOp extends LocalOperator <DbscanPredictLocalOp> implements
	DbscanPredictParams <DbscanPredictLocalOp> {
	private static final int MAX_ACCURATE_DISTANCE_NUM = 1000000;

	@Override
	public DbscanPredictLocalOp linkFrom(LocalOperator <?>... inputs) {
		checkMinOpSize(2, inputs);
		if (!getParams().contains(DbscanLocalParams.PREDICTION_COL)) {
			throw new AkIllegalOperatorParameterException("In DbscanLocalOp,Not have parameter: predictionCol.");
		}
		LocalOperator model = inputs[0];
		List <Row> modelRows = model.getOutputTable().getRows();
		Params DbscanTrainParams = Params.fromJson((String) modelRows.get(0).getField(0));
		LocalOperator doc = deserializeModel(DbscanTrainParams, modelRows);

		LocalOperator data = inputs[1];
		int numData = data.getOutputTable().getNumRow();
		Boolean useApproxAlgo = getParams().contains(HasNumHashTables.NUM_HASH_TABLES) &&
			getParams().contains(HasNumProjectionsPerTable.NUM_PROJECTIONS_PER_TABLE) &&
			getParams().contains(HasProjectionWidth.PROJECTION_WIDTH) ? true :
			numData > MAX_ACCURATE_DISTANCE_NUM ? true : false;
		LocalOperator neighbors = DbscanLocalOp.calculateNeighbors(doc, data, 1, useApproxAlgo, DbscanTrainParams,
			getParams());
		int nnPredictColIndex = DbscanLocalOp.getNearestNeighborResultColIndex(neighbors);
		String[] keepColNames = getReservedCols();

		if (keepColNames == null) {
			keepColNames = data.getColNames();
		}

		List <String> outputColNames = new ArrayList <>();
		List <TypeInformation> outputColTypes = new ArrayList <>();
		outputColNames.add(getPredictionCol());
		outputColTypes.add(AlinkTypes.LONG);

		OutputColsHelper outputColsHelper = new OutputColsHelper(data.getSchema(),
			outputColNames.toArray(new String[0]), outputColTypes.toArray(new TypeInformation[0]), keepColNames);

		int outputArity = keepColNames.length + 1;

		int[] keepColsIndex = TableUtil.findColIndices(data.getColNames(), keepColNames);
		Row[] resultRows = new Row[numData];

		final TaskRunner taskRunner = new TaskRunner();
		int numThreads = LocalOperator.getDefaultNumThreads();
		if (getParams().contains(HasNumThreads.NUM_THREADS)) {
			numThreads = getParams().get(HasNumThreads.NUM_THREADS);
		}

		for (int t = 0; t < numThreads; ++t) {
			final int localStartSample = (int) AlinkLocalSession.DISTRIBUTOR.startPos(t, numThreads, numData);
			final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(t, numThreads, numData);
			final int localNSample = localStartSample + cnt;
			taskRunner.submit(() -> {
					for (int i = localStartSample; i < localNSample; i++) {
						boolean isNoisy = true;
						int clusterId = -1;
						Row row = data.getOutputTable().getRows().get(i);
						if (neighbors != null) {
							String neighborString = (String) row.getField(nnPredictColIndex);
							Map <String, List <Object>> nearestCore = KObjectUtil.deserializeKObject(neighborString,
								new String[] {"ID"}, new Type[] {Integer.class,}
							);
							List <Object> clusterIds = nearestCore.get("ID");
							isNoisy = clusterIds.size() == 0;
							clusterId = isNoisy ? -1 : (int) clusterIds.get(0);
						}
						if (isNoisy) {
							resultRows[i] = new Row(outputArity);
							for (int j = 0; j < keepColsIndex.length; j++) {
								resultRows[i].setField(j, row.getField(keepColsIndex[j]));
							}
							resultRows[i].setField(outputArity-1, -1L);
						} else {
							resultRows[i] = new Row(outputArity);
							for (int j = 0; j < keepColsIndex.length; j++) {
								resultRows[i].setField(j, row.getField(keepColsIndex[j]));
							}
							resultRows[i].setField(outputArity-1, (long)clusterId);
						}
					}
				}
			);
		}

		taskRunner.join();

		this.setOutputTable(new MTable(
			resultRows,
			outputColsHelper.getResultSchema()
		));

		return this;
	}

	private LocalOperator deserializeModel(Params meta, List <Row> data) {
		double epsilon = meta.get(DbscanLocalParams.RADIUS);
		String selectedCol = meta.get(DbscanLocalParams.SELECTED_COL);
		String idCol = meta.get(DbscanLocalParams.ID_COL);
		Row[] docRows = new Row[data.size() - 1];
		// get the model data
		if (data.size() == 1) {
			return null;
		}
		for (int i = 1; i < data.size(); i++) {
			Row row = data.get(i);
			try {
				ClusterSummary c = gson.fromJson((String) row.getField(0), ClusterSummary.class);
				Vector vec = VectorUtil.getVector(c.center);
				docRows[i - 1] = Row.of(vec, c.clusterId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		MTable modelData = new MTable(docRows, String.format("%s vector, %s int", selectedCol, idCol));
		LocalOperator doc = new MemSourceLocalOp(modelData);
		return doc;
	}

	private static class ClusterSummary implements Serializable {
		private static final long serialVersionUID = 8631961988528403010L;
		public String center;
		public Integer clusterId;
		public Double weight;
		public String type;

		public ClusterSummary(String center, Integer clusterId, Double weight, String type) {
			this.center = center;
			this.clusterId = clusterId;
			this.weight = weight;
			this.type = type;
		}

		public ClusterSummary(String center, Integer clusterId, Double weight) {
			this(center, clusterId, weight, null);
		}
	}
}
