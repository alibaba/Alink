package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.dataproc.HasStrategy.Strategy;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.common.utils.PrettyDisplayUtils.displayList;

public class ImputerModelInfo implements Serializable {
	private static final long serialVersionUID = -5096380869679417500L;
	Strategy strategy;
	String fillValue;
	double[] values;
	public String[] selectedCols;
	public String vectorCol;

	public ImputerModelInfo(List <Row> rows) {
		Tuple3 <Strategy, double[], String> modelData = new ImputerModelDataConverter().load(rows);
		this.strategy = modelData.f0;
		this.fillValue = modelData.f2;
		this.values = modelData.f1;
	}

	public double[] getFillValues() {
		return this.values;
	}

	public String getStrategy() {
		return this.strategy.name();
	}

	public String fillValue() {
		return this.fillValue;
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		res.append(PrettyDisplayUtils.displayHeadline("ImputerModelInfo", '-') + "\n");
		res.append("Strategy: " + getStrategy() + "\n");
		if (this.selectedCols == null) {
			res.append("VectorCol: " + this.vectorCol + "\n");
		}

		String[] rowNames = new String[1];
		Object[][] vals;
		int nRows = 1;
		int nCols;
		String[] colNames = this.selectedCols;
		if (colNames == null && this.values != null) {
			colNames = new String[this.values.length];
			for (int i = 0; i < values.length; i++) {
				colNames[i] = String.valueOf(i);
			}
		}

		switch (this.strategy) {
			case MEAN:
				rowNames[0] = "Mean";
				break;
			case MIN:
				rowNames[0] = "Min";
				break;
			case MAX:
				rowNames[0] = "Max";
				break;
			case VALUE:
				rowNames[0] = "Value";
				break;
		}

		switch (this.strategy) {
			case MEAN:
			case MIN:
			case MAX:
				res.append("\n");
				nCols = this.values.length;
				vals = new Object[1][this.values.length];
				for (int i = 0; i < this.values.length; i++) {
					vals[0][i] = this.values[i];
				}
				res.append(PrettyDisplayUtils.displayTable(vals, nRows, nCols,
					rowNames, colNames, null,
					100, 100) + "\n");
				break;
			case VALUE:
				if (this.selectedCols != null) {
					res.append("\n");
					nCols = this.selectedCols.length;
					vals = new Object[1][this.selectedCols.length];
					for (int i = 0; i < this.selectedCols.length; i++) {
						vals[0][i] = this.fillValue;
					}
					res.append(PrettyDisplayUtils.displayTable(vals, nRows, nCols,
						rowNames, colNames, null,
						100, 100) + "\n");
				} else {
					res.append("Value: " + this.fillValue() + "\n");
				}
				break;
		}
		return res.toString();
	}

}
