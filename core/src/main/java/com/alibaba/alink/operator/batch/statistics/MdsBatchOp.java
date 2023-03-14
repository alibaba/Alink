package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.EigenSolver;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.common.utils.TableUtil;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;

import com.alibaba.alink.params.statistics.MdsParams;

import java.util.ArrayList;

/**
 * @author Fan Hong
 */

/**
 * Multi-Dimensional Scaling, MDS, is a dimension reduction techniques for high-dimensional data.
 * MDS reduces (projects or embeds) data into a lower-dimensional, usually 2D, space.
 * The object of MDS is to keep the distances between data items in the original space as much as possible.
 * Therefore, MDS can be used to perceive clusters or outliers.
 */
@NameCn("Multi-Dimensional Scaling")
@NameEn("Multi-Dimensional Scaling")
public class MdsBatchOp extends BatchOperator<MdsBatchOp> implements MdsParams <MdsBatchOp> {

	private static final long serialVersionUID = 7353869732042122439L;

	/**
	 * Default constructor
	 */
	public MdsBatchOp() {
		super(null);
	}

	/**
	 * Constructor
	 *
	 * @param params: parameters
	 */
	public MdsBatchOp(Params params) {
		super(params);
	}

	@Override
	public MdsBatchOp linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();
		if (selectedColNames == null) {
			selectedColNames = TableUtil.getNumericCols(in.getSchema());
		}

		String[] keepColNames = getReservedCols();
		if (keepColNames == null) {
			keepColNames = in.getSchema().getFieldNames();
		}

		final String coordsColNamePrefix = getOutputColPrefix();
		final Integer numDimensions = getDim();

		// Map column names to column indices
		String[] allColNames = in.getColNames();
		TypeInformation <?>[] allColTypes = in.getColTypes();

		int numSelectedColNames = selectedColNames.length;
		final int[] selectedColIndices = new int[numSelectedColNames];
		for (int i = 0; i < numSelectedColNames; i += 1) {
			selectedColIndices[i] = TableUtil.findColIndexWithAssertAndHint(allColNames, selectedColNames[i]);
		}

		int numKeepColNames = keepColNames.length;
		final int[] keepColIndices = new int[numKeepColNames];
		for (int i = 0; i < numKeepColNames; i += 1) {
			keepColIndices[i] = TableUtil.findColIndexWithAssertAndHint(allColNames, keepColNames[i]);
		}

		DataSet <Row> out = in.getDataSet()
			.mapPartition(new MdsComputationMapPartitionFunction(numDimensions, selectedColIndices, keepColIndices))
			.setParallelism(1);

		ArrayList <String> colNames = new ArrayList <>();
		ArrayList <TypeInformation <?>> colTypes = new ArrayList <>();
		for (int i = 0; i < numDimensions; i += 1) {
			colNames.add(new StringBuilder().append(coordsColNamePrefix).append(i).toString());
			colTypes.add(Types.DOUBLE);
		}

		for (int i = 0; i < numKeepColNames; i += 1) {
			colNames.add(allColNames[keepColIndices[i]]);
			colTypes.add(allColTypes[keepColIndices[i]]);
		}

		setOutput(out, colNames.toArray(new String[0]), colTypes.toArray(new TypeInformation <?>[0]));

		return this;
	}

	public static class MdsComputationMapPartitionFunction extends RichMapPartitionFunction <Row, Row> {

		private static final long serialVersionUID = 5257680310195705244L;
		private int numDimensions;
		private int[] selectedColIndices;
		private int[] keepColIndices;

		public MdsComputationMapPartitionFunction(int numDimensions, int[] selectedColIndices, int[] keepColIndices) {
			this.numDimensions = numDimensions;
			this.selectedColIndices = selectedColIndices;
			this.keepColIndices = keepColIndices;
		}

		@Override
		public void mapPartition(Iterable <Row> iterable, Collector <Row> collector) throws Exception {
			ArrayList <Row> rowList = new ArrayList <>();
			for (Row row : iterable) {
				rowList.add(row);
			}

			// extract data for computation
			int numCols = this.selectedColIndices.length;
			ArrayList <double[]> dataList = new ArrayList <>();
			for (Row row : rowList) {
				double[] item = new double[numCols];
				for (int i = 0; i < numCols; i += 1) {
					item[i] = (double) row.getField(this.selectedColIndices[i]);
				}
				dataList.add(item);
			}

			int n = dataList.size();
			System.out.println(n);

			double[][] data = new double[n][numCols];
			data = dataList.toArray(data);

			// Perform MDS computation
			MdsComputation mdsComputation = new MdsComputation(n, numCols, data, numDimensions);
			DenseVector[] coordinates = mdsComputation.compute();

			// Collect results
			int k = 0;
			for (Row row : rowList) {
				Row out = new Row(numDimensions + keepColIndices.length);
				for (int i = 0; i < numDimensions; i += 1) {
					out.setField(i, coordinates[i].get(k));
				}
				for (int i = 0; i < keepColIndices.length; i += 1) {
					out.setField(numDimensions + i, row.getField(keepColIndices[i]));
				}
				//            System.out.println(row);
				collector.collect(out);
				k += 1;
			}
		}

		class MdsComputation {
			private int n;
			private int m;
			private double[][] data;
			private int k;

			MdsComputation(int n, int m, double[][] data, int k) {
				this.n = n;
				this.m = m;
				this.data = data;
				this.k = k;
			}

			double computeDistance(int n, double[] d0, double[] d1) {
				double sum = 0;
				for (int i = 0; i < n; i += 1) {
					sum += Math.pow(d0[i] - d1[i], 2.);
				}
				return Math.sqrt(sum);
			}

			double[][] computeDistanceMatrix(int n, int m, double[][] data) {
				double[][] dist = new double[n][n];
				for (int i = 0; i < n; i += 1) {
					dist[i] = new double[n];
					for (int j = 0; j < i; j += 1) {
						dist[i][j] = dist[j][i] = computeDistance(m, data[i], data[j]);
					}
					dist[i][i] = 0;
				}
				return dist;
			}

			DenseVector[] compute() {
				// STEP 0: compute the distances matrix between data items
				double[][] dist = computeDistanceMatrix(n, m, data);

				// STEP 1: double-center the matrix
				double rowSum[] = new double[n];
				double colSum[] = new double[n];
				double totalSum = 0;
				for (int i = 0; i < n; i += 1) {
					for (int j = 0; j < n; j += 1) {
						rowSum[i] += dist[i][j];
						colSum[j] += dist[i][j];
						totalSum += dist[i][j];
					}
				}
				for (int i = 0; i < n; i += 1) {
					for (int j = 0; j < n; j += 1) {
						dist[i][j] += rowSum[i] / n + colSum[j] / n - totalSum / n / n;
					}
				}

				// STEP 2: get the k largest eigenvalues and eigenvectors
				//get eig values and eig vectors
				DenseMatrix dm = new DenseMatrix(dist);
				double epsilon = 1e-6;
				scala.Tuple2 <DenseVector, DenseMatrix> eigens = EigenSolver.solve(dm, k, epsilon, 300);

				// STEP 3: obtain the embedding coordinates
				DenseVector[] coords = new DenseVector[eigens._2.numCols()];
				for (int i = 0; i < coords.length; i++) {
					coords[i] = new DenseVector(eigens._2.getColumn(i).clone());
				}
				for (int i = 0; i < k; i += 1) {
					coords[i].scaleEqual(eigens._1.get(i));
				}
				return coords;
			}
		}
	}
}
