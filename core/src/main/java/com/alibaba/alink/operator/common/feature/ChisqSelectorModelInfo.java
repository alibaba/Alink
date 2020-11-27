package com.alibaba.alink.operator.common.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.feature.BasedChisqSelectorParams;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * chisq selector model info.
 */
public class ChisqSelectorModelInfo implements Serializable {
	private static final long serialVersionUID = -5530686415831114918L;
	protected ChiSquareTestResult[] chiSqs;
	protected String[] colNames;
	protected String[] siftOutColNames;
	protected BasedChisqSelectorParams.SelectorType selectorType;
	protected int numTopFeatures;
	protected double percentile;
	protected double fpr;
	protected double fdr;
	protected double fwe;

	public ChisqSelectorModelInfo() {

	}

	public ChisqSelectorModelInfo(List <Row> rows) {
		ChisqSelectorModelInfo modelInfo = new ChiSqSelectorModelDataConverter().load(rows);
		this.chiSqs = modelInfo.chiSqs;
		this.colNames = modelInfo.colNames;
		this.siftOutColNames = modelInfo.siftOutColNames;
		this.selectorType = modelInfo.selectorType;
		this.numTopFeatures = modelInfo.numTopFeatures;
		this.percentile = modelInfo.percentile;
		this.fpr = modelInfo.fpr;
		this.fdr = modelInfo.fdr;
		this.fwe = modelInfo.fwe;
	}

	public double chisq(String colName) {
		return chiSqs[getIdx(chiSqs, colName)].getValue();
	}

	public double pValue(String colName) {
		return chiSqs[getIdx(chiSqs, colName)].getP();
	}

	public BasedChisqSelectorParams.SelectorType getSelectorType() {
		return selectorType;
	}

	public int getNumTopFeatures() {
		return numTopFeatures;
	}

	public double getPercentile() {
		return percentile;
	}

	public double getFpr() {
		return fpr;
	}

	public double getFdr() {
		return fdr;
	}

	public double getFwe() {
		return fwe;
	}

	public int getSelectorNum() {
		return this.siftOutColNames.length;
	}

	public String[] getColNames() {
		return this.colNames;
	}

	public String[] getSiftOutColNames() {
		return this.siftOutColNames;
	}

	@Override
	public String toString() {
		int n = this.chiSqs.length;
		StringBuilder sbd = new StringBuilder()
			.append(PrettyDisplayUtils.displayHeadline("ChisqSelectorModelInfo", '-'));

		sbd.append("Number of Selector Features: " + getSelectorNum() + "\n");
		sbd.append("Number of Features: " + chiSqs.length + "\n");
		sbd.append("Type of Selector: " + this.selectorType.name() + "\n");

		List <ChiSquareTestResult> chisqList = Arrays.asList(chiSqs);

		switch (this.selectorType) {
			case NumTopFeatures:
				chisqList.sort(new ChisqSelectorUtil.RowAscComparator());
				sbd.append("Number of Top Features: " + this.numTopFeatures + "\n");
				break;
			case PERCENTILE:
				chisqList.sort(new ChisqSelectorUtil.RowAscComparator());
				sbd.append("Percentile of Features: " + this.percentile + "\n");
				break;
			case FDR:
				chisqList.sort(new ChisqSelectorUtil.RowAscComparator(true));
				sbd.append("FDR of Features: " + this.fdr + "\n");
				break;
			case FPR:
				chisqList.sort(new ChisqSelectorUtil.RowAscComparator(true));
				sbd.append("FPR of Features: " + this.fpr + "\n");
				break;
			case FWE:
				chisqList.sort(new ChisqSelectorUtil.RowAscComparator(true));
				sbd.append("FWE of Features: " + this.fwe + "\n");
				break;

		}
		String[] colcolNames = new String[] {"ColName", "ChiSquare", "PValue", "DF", "Selected"};
		Object[][] vals = new Object[n][5];

		if (colNames == null) {
			colcolNames[0] = "VectorIndex";
		}

		for (int i = 0; i < n && i < chiSqs.length; i++) {
			ChiSquareTestResult chisq = chisqList.get(i);
			vals[i][0] = chisq.getColName();
			vals[i][1] = chisq.getValue();
			vals[i][2] = chisq.getP();
			vals[i][3] = chisq.getDf();
			vals[i][4] = i < getSelectorNum();
		}

		sbd.append("Selector Indices: " + "\n");
		sbd.append(PrettyDisplayUtils
			.indentLines(PrettyDisplayUtils.displayTable(vals, n, 5, null, colcolNames, null, n, 5), 4));

		return sbd.toString();
	}

	static int getIdx(ChiSquareTestResult[] test, String colName) {
		for (int i = 0; i < test.length; i++) {
			if (colName.equals(test[i].getColName())) {
				return i;
			}
		}
		return -1;
	}

}
