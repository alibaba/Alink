package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.slidingwindow.windowtree.DeepCloneable;
import com.alibaba.alink.operator.common.statistics.basicstat.WindowTable;
import com.alibaba.alink.params.statistics.HasStatLevel_L1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author yangxu
 */
public class SummaryResultTable implements Serializable, Cloneable, DeepCloneable <SummaryResultTable> {

	private static final long serialVersionUID = -4193449730900308805L;
	/**
	 * 各列名称
	 */
	public String[] colNames;
	public double[][] dotProduction = null;
	/**
	 * *
	 * 各列的基本统计信息
	 */
	public SummaryResultCol[] src;

	public SummaryResultTable(String[] nameCols) {
		int n = nameCols.length;
		this.colNames = new String[n];
		System.arraycopy(nameCols, 0, this.colNames, 0, n);
		//        this.cov = new double[n][n];
		this.src = new SummaryResultCol[n];
		for (int i = 0; i < n; i++) {
			src[i] = new SummaryResultCol();
			src[i].colName = this.colNames[i];
		}
	}

	@Override
	public SummaryResultTable deepClone() {
		return this.clone();
	}

	/***
	 * 实现clone方法 //todo 检查+测试
	 */
	@Override
	public SummaryResultTable clone() {
		String[] colNames = this.colNames;
		SummaryResultCol[] src = new SummaryResultCol[this.src.length];
		for (int i = 0; i < this.src.length; ++i) {
			src[i] = this.src[i].clone();
		}
		double[][] dotProduction = null;
		if (this.dotProduction != null) {
			dotProduction = new double[this.dotProduction.length][];
			for (int i = 0; i < this.dotProduction.length; ++i) {
				dotProduction[i] = this.dotProduction[i].clone();
			}
		}
		return new SummaryResultTable(colNames, src, dotProduction);
	}

	/***
	 * 创建一个全0初始值的SummaryResultTable
	 * @param colNames
	 * @param colTypes
	 * @param statLevel
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public SummaryResultTable(String[] colNames, Class <?>[] colTypes, HasStatLevel_L1.StatLevel statLevel)
		throws IllegalAccessException, InstantiationException {
		Row empty = new Row(colNames.length);

		for (int i = 0; i < colNames.length; ++i) {
			Object zero = "0";
			if (Double.class == colTypes[i]) {
				zero = Double.valueOf("0");
			} else if (Float.class == colTypes[i]) {
				zero = Float.valueOf("0");
			} else if (Long.class == colTypes[i]) {
				zero = Long.valueOf("0");
			} else if (Integer.class == colTypes[i]) {
				zero = Integer.valueOf("0");
			} else if (Short.class == colTypes[i]) {
				zero = Short.valueOf("0");
			} else if (Byte.class == colTypes[i]) {
				zero = Byte.valueOf("0");
			}
			empty.setField(i, zero);
		}
		ArrayList <Row> emptyArray = new ArrayList <>(1);
		emptyArray.add(empty);
		WindowTable wt = new WindowTable(colNames, colTypes, emptyArray);
		SummaryResultTable srt = Summary2.streamSummary(wt, colNames, 10, 10, 100, 10, statLevel);
		for (int i = 0; i < srt.src.length; ++i) {
			srt.src[i].countTotal = srt.src[i].count = 0;
		}
		this.colNames = srt.colNames;
		this.src = srt.src;
		this.dotProduction = srt.dotProduction;
	}

	public SummaryResultTable(String[] colNames, SummaryResultCol[] src, double[][] dotProduction) {
		this.colNames = colNames;
		this.dotProduction = dotProduction;
		this.src = src;
	}

	public static SummaryResultTable combine(SummaryResultTable srt1, SummaryResultTable srt2) {
		if (null == srt1.colNames || 0 == srt1.colNames.length || null == srt2.colNames || 0 == srt2.colNames.length
			|| srt1.colNames.length != srt2.colNames.length) {
			throw new AkIllegalStateException("Col names are empty, or not matched!");
		}
		int nCol = srt1.colNames.length;
		for (int i = 0; i < nCol; i++) {
			if (!srt1.colNames[i].equals(srt2.colNames[i])) {
				throw new AkIllegalStateException("Col names are not matched!");
			}
		}

		SummaryResultTable srt = new SummaryResultTable(srt1.colNames);
		for (int i = 0; i < nCol; i++) {
			srt.src[i] = SummaryResultCol.combine(srt1.src[i], srt2.src[i]);
		}

		if (srt1.dotProduction != null && srt2.dotProduction != null) {
			srt.dotProduction = new double[nCol][nCol];
			for (int i = 0; i < nCol; i++) {
				for (int j = 0; j < nCol; j++) {
					srt.dotProduction[i][j] = srt1.dotProduction[i][j] + srt2.dotProduction[i][j];
				}
			}
		}

		return srt;
	}

	public SRT toSRT() {
		SRT srt = new SRT();
		srt.colNames = this.colNames;
		int n = this.colNames.length;
		srt.src = new SRC[n];
		for (int i = 0; i < n; i++) {
			srt.src[i] = this.src[i].toSRC();
		}
		srt.dotProduction = this.dotProduction;

		return srt;
	}

	public void combine(SummaryResultTable srt) throws CloneNotSupportedException {
		if (null == colNames || 0 == colNames.length ||
			null == srt.colNames || 0 == srt.colNames.length ||
			colNames.length != srt.colNames.length) {
			throw new AkIllegalStateException("Col names are empty, or not matched!");
		}
		int nCol = colNames.length;
		for (int i = 0; i < nCol; i++) {
			if (!colNames[i].equals(srt.colNames[i])) {
				throw new AkIllegalStateException("Col names are not matched!");
			}
		}

		for (int i = 0; i < nCol; i++) {
			src[i].combine(srt.src[i]);
		}

		if (dotProduction != null && srt.dotProduction != null) {
			srt.dotProduction = new double[nCol][nCol];
			for (int i = 0; i < nCol; i++) {
				for (int j = 0; j < nCol; j++) {
					srt.dotProduction[i][j] += srt.dotProduction[i][j];
				}
			}
		}
	}

	/**
	 * *
	 * 获取协方差矩阵
	 *
	 * @return 协方差矩阵
	 */
	public double[][] getCov() {
		int nStat = this.colNames.length;
		if (src[0].countTotal == 0) {
			double[][] tmp = new double[nStat][nStat];
			for (int i = 0; i < nStat; i++) {
				for (int j = 0; j < nStat; j++) {
					tmp[i][j] = Double.NaN;
				}
			}
			return tmp;
		}

		if (null != this.dotProduction) {
			double[][] cov = new double[nStat][nStat];
			for (int i = 0; i < nStat; i++) {
				for (int j = i; j < nStat; j++) {
					double d = this.dotProduction[i][j];
					long cnt = src[i].count;
					d = (d - src[i].sum * src[j].sum / cnt) / (cnt - 1);
					cov[i][j] = d;
					cov[j][i] = d;
				}
			}
			for (int i = 0; i < nStat; i++) {
				if (String.class == src[i].dataType || Date.class == src[i].dataType) {
					for (int j = 0; j < nStat; j++) {
						cov[i][j] = Double.NaN;
						cov[j][i] = Double.NaN;
					}
				}
			}
			return cov;
		} else {
			throw new AkUnsupportedOperationException("Not implemented yet!");
		}

	}

	/**
	 * *
	 * 获取相关系数矩阵
	 *
	 * @return 相关系数矩阵
	 */
	public double[][] getCorr() {

		int numCal = src.length;

		if (src[0].countTotal == 0) {
			double[][] tmp2 = new double[numCal][numCal];
			for (int i = 0; i < numCal; i++) {
				for (int j = 0; j < numCal; j++) {
					tmp2[i][j] = Double.NaN;
				}
			}
			return tmp2;
		}

		double[][] cov_tmp = getCov();
		for (int i = 0; i < numCal; i++) {
			for (int j = i; j < numCal; j++) {
				if (Double.isNaN(cov_tmp[i][j])) {
					cov_tmp[i][j] = Double.NaN;
				} else if (cov_tmp[i][j] == 0) {
					cov_tmp[i][j] = 0;
				} else {
					double d = cov_tmp[i][j] / src[i].standardDeviation() / src[j].standardDeviation();
					cov_tmp[i][j] = d;
					cov_tmp[j][i] = d;
				}
			}
		}

		for (int i = 0; i < numCal; i++) {
			if (!Double.isNaN(cov_tmp[i][i])) {
				cov_tmp[i][i] = 1.0;
			}
		}
		for (int i = 0; i < numCal; i++) {
			for (int j = 0; j < numCal; j++) {
				if (i != j) {
					if (Double.isNaN(cov_tmp[i][i])) {

					} else if (cov_tmp[i][i] > 1.0) {
						cov_tmp[i][i] = 1.0;
					} else if (cov_tmp[i][i] < -1.0) {
						cov_tmp[i][i] = -1.0;
					}
				}
			}
		}

		return cov_tmp;

	}

	/**
	 * *
	 * 获取指定名称的列基本统计信息
	 *
	 * @param colname 列的名称
	 * @return 指定名称的列基本统计信息
	 * @throws Exception
	 */
	public SummaryResultCol col(String colname) {
		//        GetSRT();
		return src[TableUtil.findColIndexWithAssert(this.colNames, colname)];
	}

	/**
	 * *
	 * 获取指定索引的列基本统计信息
	 *
	 * @param colIndex 列的索引
	 * @return 指定索引的列基本统计信息
	 * @throws Exception
	 */
	public SummaryResultCol col(int colIndex) {
		//        GetSRT();
		return src[colIndex];
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		int n = src.length;
		sbd.append("###### Summary Infomation ######\n");
		for (int i = 0; i < n; i++) {
			sbd.append("Column: ");
			sbd.append(colNames[i]);
			sbd.append("\n");
			sbd.append(src[i].toString());
			sbd.append("---------------------------------------\n");
		}

		if (this.dotProduction != null) {
			double[][] cov = this.getCov();
			sbd.append("Cov:\n");
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < n; j++) {
					sbd.append(cov[i][j]);
					sbd.append("\t");
				}
				sbd.append("\n");
			}

			double[][] corr = this.getCorr();
			sbd.append("Corr:\n");
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < n; j++) {
					sbd.append(corr[i][j]);
					sbd.append("\t");
				}
				sbd.append("\n");
			}
		}
		sbd.append("#######################################\n");
		return sbd.toString();
	}

}
