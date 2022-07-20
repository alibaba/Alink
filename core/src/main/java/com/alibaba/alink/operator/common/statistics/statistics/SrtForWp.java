package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.common.utils.TableUtil;

import java.util.HashSet;

public class SrtForWp implements AlinkSerializable {
	public String[] colNames;
	SummaryResultCol[] src;
	HashSet <Object>[] distinctValues;

	SrtForWp(String[] nameCols) {
		int n = nameCols.length;
		this.colNames = new String[n];
		System.arraycopy(nameCols, 0, this.colNames, 0, n);
		this.src = new SummaryResultCol[n];
		for (int i = 0; i < n; i++) {
			src[i] = new SummaryResultCol();
			src[i].colName = this.colNames[i];
		}
		this.distinctValues = new HashSet[n];
	}

	public SummaryResultCol col(int i) {
		return src[i];
	}

	public SummaryResultCol col(String colName) {
		return src[TableUtil.findColIndex(colNames, colName)];
	}

	public HashSet <Object> freq(int i) {
		return distinctValues[i];
	}

	public SrtForWp copy(boolean needFreq) {
		SrtForWp srtCopy = new SrtForWp(colNames);
		for (int i = 0; i < colNames.length; i++) {
			srtCopy.src[i] = col(i).copy();
			if (needFreq) {
				if (distinctValues[i] != null) {
					srtCopy.distinctValues[i] = new HashSet <>();
					for (Object value : distinctValues[i]) {
						srtCopy.distinctValues[i].add(value);
					}
				}
			}
		}

		return srtCopy;
	}

	public void combine(SrtForWp srt, boolean needFreq) throws CloneNotSupportedException {
		if (null == colNames || 0 == colNames.length ||
			null == srt.colNames || 0 == srt.colNames.length ||
			colNames.length != srt.colNames.length) {
			throw new AkIllegalOperatorParameterException("Col names are empty, or not matched!");
		}
		int nCol = colNames.length;
		for (int i = 0; i < nCol; i++) {
			if (!colNames[i].equals(srt.colNames[i])) {
				throw new AkIllegalOperatorParameterException("Col names are not matched!");
			}
		}

		for (int i = 0; i < nCol; i++) {
			src[i].combine(srt.src[i]);
		}

		if (needFreq) {
			for (int i = 0; i < nCol; i++) {
				if (distinctValues[i] != null && srt.distinctValues[i] != null) {
					for (Object value : srt.distinctValues[i]) {
						distinctValues[i].add(value);
					}
				}
			}
		} else {
			for (int i = 0; i < nCol; i++) {
				distinctValues[i] = null;
			}
		}

	}

}
