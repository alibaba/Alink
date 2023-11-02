package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import com.alibaba.alink.operator.common.recommendation.KObjectUtil;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.operator.common.similarity.dataConverter.VectorModelDataConverter;
import com.alibaba.alink.operator.common.similarity.modeldata.VectorModelData;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.AkSourceLocalOp;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.operator.local.utils.TopK;
import com.alibaba.alink.params.shared.HasModelFilePath;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.flink.ml.tf2.shaded.com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Find the nearest neighbor of query vectors.
 */
@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = VectorNearestNeighborTrainLocalOp.class),
	@PortSpec(PortType.DATA)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量最近邻预测")
public class VectorNearestNeighborPredictLocalOp extends LocalOperator <VectorNearestNeighborPredictLocalOp>
	implements NearestNeighborPredictParams <VectorNearestNeighborPredictLocalOp>,
	HasModelFilePath <VectorNearestNeighborPredictLocalOp> {

	public VectorNearestNeighborPredictLocalOp() {
		this(new Params());
	}

	public VectorNearestNeighborPredictLocalOp(Params params) {
		super(params);
	}

	@Override
	public VectorNearestNeighborPredictLocalOp linkFrom(LocalOperator <?>... inputs) {

		checkMinOpSize(1, inputs);

		Integer topN = getTopN();
		Double radius = getRadius();
		AkPreconditions.checkArgument(!(topN == null && radius == null), "Must give topN or radius!");

		LocalOperator <?> model = inputs.length == 2 ? inputs[0] : null;
		LocalOperator <?> input = inputs.length == 2 ? inputs[1] : inputs[0];

		if (model == null && getParams().get(HasModelFilePath.MODEL_FILE_PATH) != null) {
			model = new AkSourceLocalOp().setFilePath(getModelFilePath());
		} else if (model == null) {
			throw new AkIllegalOperatorParameterException("One of model or modelFilePath should be set.");
		}

		try {
			VectorModelDataConverter dataConverter = new VectorModelDataConverter();
			dataConverter.setIdType(model.getColTypes()[model.getColNames().length - 1]);
			VectorModelData modelData = dataConverter.load(model.getOutputTable().getRows());

			if (!(modelData.fastDistance instanceof EuclideanDistance)
				&& !(modelData.fastDistance instanceof CosineDistance)) {
				this.setOutputTable(
					new SubVectorNearestNeighborPredictLocalOp(getParams()).linkFrom(inputs).getOutputTable()
				);
				return this;
			}
			boolean cosine = modelData.fastDistance instanceof CosineDistance;

			modelData.dictData.get(0).getRows();
			boolean isMatrix = true;
			int dictAgg = 0;
			int dimAgg = 0;
			for (int k = 0; k < modelData.dictData.size(); k++) {
				if (modelData.dictData.get(k) instanceof FastDistanceMatrixData) {
					dictAgg += ((FastDistanceMatrixData) modelData.dictData.get(k)).getVectors().numCols();
					dimAgg = ((FastDistanceMatrixData) modelData.dictData.get(k)).getVectors().numRows();
				} else {isMatrix = false;}
			}

			final int nDict = dictAgg;
			final int dim = dimAgg;

			if (!isMatrix) {
				throw new AkIllegalDataException("Data is not dense vector.");
			}

			DenseMatrix matDict = new DenseMatrix(dim, nDict);
			double[] data = matDict.getData();
			double[] sqDict = new double[nDict];
			Object[] objDict = new Object[nDict];

			int offset = 0;
			for (int k = 0; k < modelData.dictData.size(); k++) {
				FastDistanceMatrixData mat = (FastDistanceMatrixData) modelData.dictData.get(k);
				int m = mat.getRows().length;
				System.arraycopy(mat.getVectors().getData(), 0, data, offset * dim, m * dim);
				if (!cosine) {
					System.arraycopy(mat.getLabel().getData(), 0, sqDict, offset, m);
				}
				Row[] rows = mat.getRows();
				for (int i = 0; i < m; i++) {
					objDict[offset + i] = rows[i].getField(0);
				}
				offset += m;
			}

			MTable mtQuery = input.getOutputTable();
			int nQuery = mtQuery.getNumRow();
			int indexVecCol = TableUtil.findColIndex(mtQuery.getSchema(), getSelectedCol());

			Row[] resultRows = new Row[nQuery];
			final int MAX_NUM_SUB_QUERY = 256;

			final TaskRunner taskRunner = new TaskRunner();

			int numThreads = LocalOperator.getParallelism();

			if (getParams().contains(HasNumThreads.NUM_THREADS)) {
				numThreads *= getParams().get(HasNumThreads.NUM_THREADS);
			}

			for (int t = 0; t < numThreads; ++t) {
				final int localStartQuery = (int) AlinkLocalSession.DISTRIBUTOR.startPos(t, numThreads, nQuery);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(t, numThreads, nQuery);
				final int localNQuery = localStartQuery + cnt;

				taskRunner.submit(() -> {
						for (int startQuery = localStartQuery; startQuery < localNQuery; startQuery += MAX_NUM_SUB_QUERY) {
							final int nSubQuery = Math.min(localNQuery - startQuery, MAX_NUM_SUB_QUERY);

							int localOffset;

							DenseMatrix matQuery = new DenseMatrix(dim, nSubQuery);
							double[] sqQuery = new double[nSubQuery];
							for (int i = 0; i < nSubQuery; i++) {
								DenseVector denseVector =
									VectorUtil.getDenseVector(mtQuery.getRow(i + startQuery).getField(indexVecCol));
								if (cosine) {
									sqQuery[i] = denseVector.normL2();
									denseVector.scaleEqual(1.0 / sqQuery[i]);
								} else {
									sqQuery[i] = denseVector.normL2Square();
								}
								System.arraycopy(denseVector.getData(), 0, matQuery.getData(), i * dim, dim);
							}

							DenseMatrix res = new DenseMatrix(nDict, nSubQuery);
							if (cosine) {
								Arrays.fill(res.getData(), 1.0);
								BLAS.gemm(-1.0, matDict, true, matQuery, false, 1.0, res);
							} else {
								BLAS.gemm(-2.0, matDict, true, matQuery, false, 0.0, res);
							}
							final double[] localData = res.getData();
							if (!cosine) {
								for (int k = 0; k < nSubQuery; k++) {
									localOffset = k * nDict;
									for (int i = 0; i < nDict; i++) {
										//data[offset + i] += sqDict[i] + sqQuery[k];
										localData[localOffset + i] = Math.sqrt(
											Math.abs(localData[localOffset + i] + sqDict[i] + sqQuery[k]));
									}
								}
							}

							if (true) {

								final int[] indices = new int[nDict];
								//final ArrayList<Object>

								for (int k = 0; k < nSubQuery; k++) {
									int localStart = k * nDict;

									for (int i = 0; i < nDict; ++i) {
										indices[i] = localStart + i;
									}

									TopK.heapMinTopK(indices, localData, topN, 0, nDict);

									List <Object> items = new ArrayList <>();
									List <Double> metrics = new ArrayList <>();

									for (int i = 0; i < Math.min(topN, nDict); ++i) {
										double metric = localData[indices[i]];
										if (null == radius || metric <= radius) {
											items.add(objDict[indices[i] - localStart]);
											metrics.add(metric);
										}
									}

									resultRows[startQuery + k] = RowUtil.merge(
										input.getOutputTable().getRow(startQuery + k),
										KObjectUtil.serializeRecomm("ID", items, ImmutableMap.of("METRIC", metrics))
									);
								}

							} else {

								PriorityQueue <Tuple2 <Double, Object>> priorityQueue
									= new PriorityQueue <>(modelData.getQueueComparator());

								for (int k = 0; k < nSubQuery; k++) {
									Tuple2 <Double, Object> head = null;
									localOffset = k * nDict;
									for (int i = 0; i < nDict; i++) {
										Tuple2 <Double, Object> newValue = Tuple2.of(localData[localOffset + i], i);
										if (priorityQueue.size() < topN) {
											priorityQueue.add(newValue);
											head = priorityQueue.peek();
										} else {
											if (priorityQueue.comparator().compare(head, newValue) < 0) {
												Tuple2 <Double, Object> peek = priorityQueue.poll();
												peek.f0 = newValue.f0;
												peek.f1 = newValue.f1;
												priorityQueue.add(peek);
												head = priorityQueue.peek();
											}
										}
									}

									List <Object> items = new ArrayList <>();
									List <Double> metrics = new ArrayList <>();
									if (null == radius) {
										while (!priorityQueue.isEmpty()) {
											Tuple2 <Double, Object> result = priorityQueue.poll();
											items.add(objDict[(Integer) result.f1]);
											metrics.add(result.f0);
										}
									} else {
										while (!priorityQueue.isEmpty()) {
											Tuple2 <Double, Object> result = priorityQueue.poll();
											if (result.f0 <= radius) {
												items.add(objDict[(Integer) result.f1]);
												metrics.add(result.f0);
											}
										}
									}
									Collections.reverse(items);
									Collections.reverse(metrics);
									priorityQueue.clear();

									resultRows[startQuery + k] = RowUtil.merge(
										input.getOutputTable().getRow(startQuery + k),
										KObjectUtil.serializeRecomm("ID", items, ImmutableMap.of("METRIC", metrics))
									);
									//	resultRows[localStartQuery + k] = RowUtil.merge(
									//		input.getOutputTable().getRow(localStartQuery + k),"");
									//}
								}
							}
							if (AlinkGlobalConfiguration.isPrintProcessInfo()
								&& (startQuery - localStartQuery) % (MAX_NUM_SUB_QUERY * 2) == 0) {
								System.out.printf("one thread predict %d vec\n", startQuery - localStartQuery);
							}
						}
					}
				);

			}

			taskRunner.join();

			this.setOutputTable(new MTable(
				resultRows,
				TableUtil.schema2SchemaStr(input.getSchema()) + ", " + getOutputCol() + " string"
			));
			return this;
		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}
	}

	/**
	 * Find the nearest neighbor of query vectors.
	 */
	@InputPorts(values = {@PortSpec(value = PortType.MODEL, suggestions = VectorNearestNeighborTrainLocalOp.class),
		@PortSpec(PortType.DATA)})
	@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
	@NameCn("向量最近邻预测")
	static class SubVectorNearestNeighborPredictLocalOp extends
		ModelMapLocalOp <SubVectorNearestNeighborPredictLocalOp>
		implements NearestNeighborPredictParams <SubVectorNearestNeighborPredictLocalOp> {

		public SubVectorNearestNeighborPredictLocalOp() {
			this(new Params());
		}

		public SubVectorNearestNeighborPredictLocalOp(Params params) {
			super(NearestNeighborsMapper::new, params);
		}
	}
}
