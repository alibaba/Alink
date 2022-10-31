package com.alibaba.alink.operator.common.similarity.dataConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.similarity.LocalitySensitiveHashApproxFunctions;
import com.alibaba.alink.operator.common.similarity.Solver;
import com.alibaba.alink.operator.common.similarity.lsh.BaseLSH;
import com.alibaba.alink.operator.common.similarity.modeldata.LocalLSHModelData;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborLocalTrainParams;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborTrainParams;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LocalLSHModelDataConverter extends NearestNeighborDataConverter <LocalLSHModelData> {
	private static final long serialVersionUID = -6846015825612538416L;
	private static final int ROW_SIZE = 1;

	public LocalLSHModelDataConverter() {
		this.rowSize = ROW_SIZE;
	}

	@Override
	public TableSchema getModelDataSchema() {
		//noinspection deprecation
		return new TableSchema(new String[] {"MODEL_DATA"},
			new TypeInformation[] {TypeInformation.of(LocalLSHModelData.class)});
	}

	@Override
	public LocalLSHModelData loadModelData(List <Row> list) {
		LocalLSHModelData modelData = (LocalLSHModelData) list.get(1).getField(0);
		/*
		 In principle, this value should be set by users when using the prediction operator. But NearestNeighborsMapper
		 doesn't give ways to set this parameter. Therefore, this value is obtained from the training operator for now.
		*/
		int maxNumCandidates = meta.get(VectorApproxNearestNeighborLocalTrainParams.MAX_NUM_CANDIDATES);
		modelData.setMaxNumCandidates(maxNumCandidates);
		return modelData;
	}

	@Override
	public DataSet <Row> buildIndex(@SuppressWarnings("rawtypes") BatchOperator in, Params params) {
		throw new AkUnsupportedOperationException("BatchOperator is not supported.");
	}

	@Override
	public List <Row> buildIndex(MTable mt, Params params) {
		Solver solver = params.get(VectorApproxNearestNeighborTrainParams.SOLVER);
		AkPreconditions.checkArgument(solver.equals(Solver.LOCAL_LSH), "Not supported solver: " + solver.name());

		int numThreads = LocalOperator.getDefaultNumThreads();
		if (params.contains(HasNumThreads.NUM_THREADS)) {
			numThreads = params.get(HasNumThreads.NUM_THREADS);
		}
		final TaskRunner taskRunner = new TaskRunner();
		final int numRows = mt.getNumRow();

		// Convert values in vector columns to vectors, and calculate vector size.
		int[] localVectorSizes = new int[numThreads];
		{
			for (int k = 0; k < numThreads; ++k) {
				final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(k, numThreads, numRows);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(k, numThreads, numRows);
				final int finalK = k;
				taskRunner.submit(() -> {
					int localVectorSize = 0;
					for (int index = start; index < start + cnt; index += 1) {
						Row row = mt.getRow(index);
						Vector vector = VectorUtil.getVector(row.getField(1));
						if (vector instanceof DenseVector) {
							localVectorSize = Math.max(localVectorSize, vector.size());
						} else if (vector instanceof SparseVector) {
							if (vector.size() != -1) {
								localVectorSize = Math.max(localVectorSize, vector.size());
							} else {
								int[] indices = ((SparseVector) vector).getIndices();
								localVectorSize = Math.max(localVectorSize, indices[indices.length - 1] + 1);
							}
						}
						row.setField(1, vector);
					}
					localVectorSizes[finalK] = localVectorSize;
				});
			}
			taskRunner.join();
		}
		int vectorSize = 0;
		for (int i = 0; i < numThreads; i += 1) {
			vectorSize = Math.max(vectorSize, localVectorSizes[i]);
		}

		BaseLSH lsh = LocalitySensitiveHashApproxFunctions.buildLSH(params, vectorSize);
		int numHashTables = params.get(VectorApproxNearestNeighborTrainParams.NUM_HASH_TABLES);

		ConcurrentHashMap <Long, Collection <Object>> indexMap = new ConcurrentHashMap <>();
		ConcurrentHashMap <Object, Vector> dataMap = new ConcurrentHashMap <>();
		{
			for (int k = 0; k < numThreads; ++k) {
				final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(k, numThreads, numRows);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(k, numThreads, numRows);
				taskRunner.submit(() -> {
					for (int index = start; index < start + cnt; index += 1) {
						Row row = mt.getRow(index);
						Object id = row.getField(0);
						Vector vector = (Vector) row.getField(1);
						dataMap.put(id, vector);
						int[] hashValues = lsh.hashFunction(vector);
						// NOTE: the following process is different from that in LSHModelDataConverter.
						// See {@link LocalHashModelData}.
						for (int i = 0; i < hashValues.length; i += 1) {
							long hashValueWithIndex = ((long) hashValues[i]) * numHashTables + i;
							indexMap.compute(hashValueWithIndex, (key, value) -> {
								Collection <Object> c = (null == value) ? new ConcurrentLinkedQueue <>() : value;
								c.add(id);
								return c;
							});
						}
					}
				});
			}
			taskRunner.join();
		}

		LocalLSHModelData modelData = new LocalLSHModelData(numHashTables, indexMap, dataMap, lsh);
		modelData.setIdType(getIdType());

		RowCollector rowCollector = new RowCollector();
		save(Tuple2.of(params, Collections.singletonList(Row.of(modelData))), rowCollector);
		return rowCollector.getRows();
	}
}
