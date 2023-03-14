package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.jama.JMatrixFunc;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.VizData;
import com.alibaba.alink.common.viz.VizDataWriterInterface;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.params.statistics.HasStatLevel_L1.StatLevel;
import com.alibaba.alink.params.statistics.MultiCollinearityBatchParams;

import java.util.ArrayList;
import java.util.Arrays;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = {
	@PortSpec(PortType.DATA)
})

@ParamSelectColumnSpec(name = "selectedCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)

@NameCn("多重共线性")
@NameEn("MultiCollinearity")
public class MultiCollinearityBatchOp extends BatchOperator <MultiCollinearityBatchOp>
	implements MultiCollinearityBatchParams <MultiCollinearityBatchOp> {

	private static final long serialVersionUID = -3276749170439192468L;

	public MultiCollinearityBatchOp() {
		super(null);
	}

	public MultiCollinearityBatchOp(Params params) {
		super(params);
	}

	@Override
	public MultiCollinearityBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String[] selectedColNames = getSelectedCols();

		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		DataSet <Multicollinearity> multiDataSet = StatisticsHelper.getSRT(in.select(selectedColNames), StatLevel.L3)
			.map(new MapFunction <SummaryResultTable, Multicollinearity>() {
				@Override
				public Multicollinearity map(SummaryResultTable srt) throws Exception {
					return Multicollinearity.calc(srt, selectedColNames);
				}
			});

		this.setOutput(
			multiDataSet.flatMap(new FlatMapFunction <Multicollinearity, Row>() {
				@Override
				public void flatMap(Multicollinearity multi, Collector <Row> out) throws Exception {
					for (int i = 0; i < multi.nameX.length; i++) {
						Row row = new Row(5 + selectedColNames.length);
						row.setField(0, multi.nameX[i]);
						row.setField(1, multi.VIF[i]);
						row.setField(2, multi.TOL[i]);
						row.setField(3, multi.eigenValues[i]);
						row.setField(4, multi.CI[i]);
						for (int j = 0; j < selectedColNames.length; j++) {
							row.setField(5 + j, multi.VarProp[i][j]);
						}
						out.collect(row);
					}
				}
			}),
			mergeCols(new String[] {"feature_name", "vif", "tof", "eigenvalue", "condition_index"},
				selectedColNames),
			mergeColTypes(new TypeInformation[] {Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE}, Types.DOUBLE, selectedColNames.length)
		);

		return this;
	}

	private String[] mergeCols(String[] left, String[] right) {
		String[] result = new String[left.length + right.length];
		System.arraycopy(left, 0, result, 0, left.length);
		System.arraycopy(right, 0, result, left.length, right.length);
		return result;
	}

	private TypeInformation <?>[] mergeColTypes(TypeInformation <?>[] left,
												TypeInformation <?> right,
												int rightLength) {
		TypeInformation <?>[] result = new TypeInformation <?>[left.length + rightLength];
		System.arraycopy(left, 0, result, 0, left.length);
		for (int i = 0; i < rightLength; i++) {
			result[left.length + i] = right;
		}
		return result;
	}

	public static class MulticollinearityFlatMap implements FlatMapFunction <SummaryResultTable, Row> {

		private static final long serialVersionUID = 7050574386992532014L;
		private String functionName;
		private VizDataWriterInterface node;

		public MulticollinearityFlatMap(VizDataWriterInterface node, String functionName) {
			this.functionName = functionName;
			this.node = node;
		}

		@Override
		public void flatMap(SummaryResultTable srt, Collector <Row> collector) throws Exception {
			try {
				long timestamp = System.currentTimeMillis();
				Multicollinearity mcl = Multicollinearity.calc(srt, null);

				MulticollinearityResult mlr = new MulticollinearityResult();
				int rowLen = mcl.nameX.length;
				int colLen = rowLen + 4;
				mlr.rowNames = mcl.nameX;
				mlr.colNames = new String[colLen];
				mlr.colNames[0] = "vif";
				mlr.colNames[1] = "tol";
				mlr.colNames[2] = "eigenvalue";
				mlr.colNames[3] = "condition_indx";
				System.arraycopy(mcl.nameX, 0, mlr.colNames, 4, rowLen);
				mlr.data = new double[rowLen][colLen];
				for (int i = 0; i < rowLen; i++) {
					mlr.data[i][0] = mcl.VIF[i];
					mlr.data[i][1] = mcl.TOL[i];
					mlr.data[i][2] = mcl.eigenValues[i];
					mlr.data[i][3] = mcl.CI[i];
					System.arraycopy(mcl.VarProp[i], 0, mlr.data[i], 4, rowLen);
				}

				//simple summary
				Row row = new Row(7);
				String json = gson.toJson(mlr);
				String resultType = "";
				row.setField(0, functionName);
				row.setField(1, resultType);
				row.setField(2, json);
				row.setField(3, timestamp);

				collector.collect(row);

				{
					int dataId = 0;
					if (functionName.equals("AllStat")) {
						dataId = 1;
					}

					ArrayList <VizData> vizDataList = new ArrayList <>();
					vizDataList.add(new VizData(dataId, json, timestamp));

					node.writeStreamData(vizDataList);
				}

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public static class Multicollinearity {
		/**
		 * *
		 * 变量名称
		 */
		public String[] nameX;
		/**
		 * *
		 * 方差膨胀因子（Variance Inflation Factor，VIF）
		 */
		public double[] VIF;
		/**
		 * *
		 * 容忍度
		 */
		public double[] TOL;
		/**
		 * *
		 * 相关系数矩阵的特征值
		 */
		public double[] eigenValues;
		/**
		 * *
		 * 条件指数
		 */
		public double[] CI;
		/**
		 * *
		 * 方差比例(Variance Proportions)
		 */
		public double[][] VarProp;
		/**
		 * *
		 * 相关系数矩阵
		 */
		double[][] correlation;
		/**
		 * *
		 * 相关系数矩阵的条件数
		 */
		double kappa;
		/**
		 * *
		 * 相关系数矩阵的最小特征值
		 */
		double lambdaMin;
		/**
		 * *
		 * 相关系数矩阵的最大特征值
		 */
		double lambdaMax;
		/**
		 * *
		 * 相关系数矩阵的最小特征向量
		 */
		double[] vectorMin;

		public static Multicollinearity calc(SummaryResultTable srt, String[] nameX) throws Exception {
			if (srt == null) {
				throw new Exception("srt is null!");
			}
			if (nameX == null) {
				nameX = srt.colNames;
			}
			int nx = nameX.length;
			int[] indexX = new int[nx];
			for (int i = 0; i < nx; i++) {
				indexX[i] = TableUtil.findColIndexWithAssert(srt.colNames, nameX[i]);
				Class type = srt.col(indexX[i]).dataType;
				if (type != Double.class && type != Long.class && type != Boolean.class) {
					throw new Exception("col type must be double, bigint , boolean!");
				}
				if (srt.col(indexX[i]).count == 0) {
					throw new Exception(nameX[i] + " count is zero, please choose cols again!");
				}
				if (srt.col(indexX[i]).countMissValue > 0 || srt.col(indexX[i]).countNanValue > 0) {
					throw new Exception("col " + nameX[i] + " has null value or nan value!");
				}
			}

			double[][] matCorr = srt.getCorr();

			double[][] correlation = new double[nx][nx];
			for (int i = 0; i < nx; i++) {
				for (int j = 0; j < nx; j++) {
					correlation[i][j] = matCorr[indexX[i]][indexX[j]];
				}
			}

			DenseMatrix[] ed = JMatrixFunc.eig(new DenseMatrix(correlation));

			Multicollinearity mcr = new Multicollinearity();
			mcr.correlation = correlation;
			mcr.nameX = new String[nx];
			mcr.vectorMin = new double[nx];
			for (int i = 0; i < nx; i++) {
				mcr.nameX[i] = nameX[i];
				mcr.vectorMin[i] = ed[0].get(i, 0);
			}

			mcr.eigenValues = new double[nx];
			for (int i = 0; i < nx; i++) {
				double d = ed[1].get(i, i);
				if (d < 1E-12) {
					d = 1E-12;
				}
				mcr.eigenValues[nx - 1 - i] = d;
			}

			mcr.lambdaMax = mcr.eigenValues[0];
			mcr.lambdaMin = mcr.eigenValues[nx - 1];
			mcr.kappa = mcr.lambdaMax / mcr.lambdaMin;

			mcr.CI = new double[nx];
			for (int i = 0; i < nx; i++) {
				mcr.CI[i] = Math.sqrt(mcr.lambdaMax / mcr.eigenValues[i]);
			}

			double[][] q = new double[nx][nx];
			double[] sq = new double[nx];
			for (int j = 0; j < nx; j++) {
				sq[j] = 0;
				for (int i = 0; i < nx; i++) {
					q[i][j] = ed[0].get(j, nx - 1 - i);
					q[i][j] = q[i][j] * q[i][j] / mcr.eigenValues[i];
					sq[j] += q[i][j];
				}
			}

			mcr.VarProp = new double[nx][nx];
			for (int i = 0; i < nx; i++) {
				for (int j = 0; j < nx; j++) {
					mcr.VarProp[i][j] = q[i][j] / sq[j];
				}
			}

			mcr.VIF = new double[nx];
			mcr.TOL = new double[nx];

			double thresholdVIF = 100000;
			ArrayList <String> restcols = new ArrayList <String>();
			restcols.addAll(Arrays.asList(nameX));
			double[][] cov = srt.getCov();
			for (int i = nameX.length - 1; i >= 0; i--) {
				ArrayList <String> cols = new ArrayList <String>();
				cols.addAll(Arrays.asList(nameX));
				cols.remove(i);
				double r2 = getR2(srt, nameX[i], cols.toArray(new String[0]), cov);
				mcr.VIF[i] = 1.0 / (1.0 - r2);
				mcr.TOL[i] = 1.0 / mcr.VIF[i];
				if (mcr.VIF[i] > thresholdVIF) {
					restcols.remove(nameX[i]);
				}
			}
			for (int i = nameX.length - 1; i >= 0; i--) {
				if (mcr.VIF[i] <= thresholdVIF) {
					ArrayList <String> cols = new ArrayList <String>();
					cols.addAll(restcols);
					cols.remove(nameX[i]);
					double r2 = getR2(srt, nameX[i], cols.toArray(new String[0]), cov);
					mcr.VIF[i] = 1.0 / (1.0 - r2);
					mcr.TOL[i] = 1.0 / mcr.VIF[i];
				}
			}

			return mcr;
		}

		static double getR2(SummaryResultTable srt, int indexY, int[] indexX, String nameY, String[] nameX, double[][] cov)
			throws Exception {
			if (srt.col(0).countTotal == 0) {
				throw new Exception("table is empty!");
			}
			if (srt.col(0).countTotal < nameX.length) {
				throw new Exception("record size Less than features size!");
			}

			int nx = indexX.length;
			long N = srt.col(indexY).count;
			if (N == 0) {
				throw new Exception("Y valid value num is zero!");
			}

			//将count == 0 and cov == 0的去掉
			ArrayList <Integer> nameXList = new ArrayList <Integer>();
			for (int anIndexX : indexX) {
				if (srt.col(anIndexX).count != 0 && cov[anIndexX][indexY] != 0) {
					//            if (srt.col(indexX[i]).count != 0) {
					nameXList.add(anIndexX);
				}
			}
			indexX = new int[nameXList.size()];
			for (int i = 0; i < indexX.length; i++) {
				indexX[i] = nameXList.get(i);
			}
			nx = indexX.length;

			double[] XBar = new double[nx];
			for (int i = 0; i < nx; i++) {
				XBar[i] = srt.col(indexX[i]).mean();
			}
			double yBar = srt.col(indexY).mean();

			DenseMatrix A = new DenseMatrix(nx, nx);
			for (int i = 0; i < nx; i++) {
				for (int j = 0; j < nx; j++) {
					A.set(i, j, cov[indexX[i]][indexX[j]]);
				}
			}
			DenseMatrix C = new DenseMatrix(nx, 1);
			for (int i = 0; i < nx; i++) {
				C.set(i, 0, cov[indexX[i]][indexY]);
			}

			//        JMatrix BetaMatrix = A.solveLS(C);
			DenseMatrix BetaMatrix = null;
			try {
				BetaMatrix = A.solve(C);
			} catch (Exception ex) {
				BetaMatrix = A.solveLS(C);
			}

			double[] beta = new double[nx + 1];
			double d = yBar;
			for (int i = 0; i < nx; i++) {
				beta[i + 1] = BetaMatrix.get(i, 0);
				d -= XBar[i] * beta[i + 1];
			}
			beta[0] = d;

			double S = srt.col(nameY).variance() * (srt.col(nameY).count - 1);
			double alpha = beta[0] - yBar;
			double U = 0.0;
			U += alpha * alpha * N;
			for (int i = 0; i < nx; i++) {
				U += 2 * alpha * srt.col(indexX[i]).sum * beta[i + 1];
			}
			for (int i = 0; i < nx; i++) {
				for (int j = 0; j < nx; j++) {
					U += beta[i + 1] * beta[j + 1] * (cov[indexX[i]][indexX[j]] * (N - 1) +
						srt.col(indexX[i]).mean() * srt.col(indexX[j]).mean() * N);
				}
			}
			double s = U / S;
			if (s < 0) {
				s = 0;
			} else if (s > 1) {
				s = 1;
			}
			return s;
		}

		static double getR2(SummaryResultTable srt, String nameY, String[] nameX, double[][] cov)
			throws Exception {
			if (srt == null) {
				throw new Exception("srt must not null!");
			}
			String[] colNames = srt.colNames;
			Class[] types = new Class[colNames.length];
			for (int i = 0; i < colNames.length; i++) {
				types[i] = srt.col(i).dataType;
			}
			int indexY = TableUtil.findColIndexWithAssertAndHint(colNames, nameY);
			Class typeY = types[indexY];
			if (typeY != Double.class && typeY != Long.class) {
				throw new Exception("col type must be double or bigint!");
			}
			if (nameX.length == 0) {
				throw new Exception("nameX must input!");
			}
			for (String aNameX : nameX) {
				int indexX = TableUtil.findColIndexWithAssertAndHint(colNames, aNameX);
				Class typeX = types[indexX];
				if (typeX != Double.class && typeX != Long.class) {
					throw new Exception("col type must be double or bigint!");
				}
			}
			int nx = nameX.length;
			int[] indexX = new int[nx];
			for (int i = 0; i < nx; i++) {
				indexX[i] = TableUtil.findColIndexWithAssert(srt.colNames, nameX[i]);
			}

			return getR2(srt, indexY, indexX, nameY, nameX, cov);
		}
	}

	static class MulticollinearityResult implements AlinkSerializable {
		String[] colNames; //vif, tol,eigenvalue, condition_index,col0, col1, ....
		String[] rowNames; //col0, col1, col2
		double[][] data;
	}
}
