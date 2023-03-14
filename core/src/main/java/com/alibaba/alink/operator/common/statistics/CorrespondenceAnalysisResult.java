package com.alibaba.alink.operator.common.statistics;

import com.alibaba.alink.common.utils.AlinkSerializable;

public class CorrespondenceAnalysisResult implements AlinkSerializable {
	public int nrow;
	public int ncol;
	public double[] sv;
	public double[] pct;
	public String[] rowTags;
	public String[] colTags;
	public double[][] rowPos;
	public double[][] colPos;
	public String rowLegend;
	public String colLegend;
}
