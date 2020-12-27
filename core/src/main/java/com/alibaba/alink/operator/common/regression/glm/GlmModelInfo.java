package com.alibaba.alink.operator.common.regression.glm;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.regression.GlmModelData;
import com.alibaba.alink.operator.common.regression.GlmModelDataConverter;
import com.alibaba.alink.operator.common.regression.glm.GlmUtil.GlmModelSummary;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.regression.GlmTrainParams.Family;
import com.alibaba.alink.params.regression.GlmTrainParams.Link;

import java.io.Serializable;
import java.util.List;

public class GlmModelInfo implements Serializable {
	public String[] featureColNames;
	public String labelColName;
	public Family family;
	public double variancePower;
	public Link link;
	public double linkPower;
	public double[] coefficients;
	public double intercept;
	public boolean fitIntercept;
	public GlmModelSummary summary;

	public GlmModelInfo(List <Row> rows) {
		GlmModelData modelData = new GlmModelDataConverter().load(rows);
		this.featureColNames = modelData.featureColNames;
		this.labelColName = modelData.labelColName;
		this.family = modelData.familyName;
		this.link = modelData.linkName;
		this.coefficients = modelData.coefficients;
		this.fitIntercept = modelData.fitIntercept;
		this.intercept = modelData.intercept;
		this.summary = modelData.modelSummary;
		if (this.link == null) {
			this.link = Link.valueOf(new FamilyLink(this.family, variancePower, this.link, linkPower).getLinkName());
		}
	}

	public double getIntercept() {
		return intercept;
	}

	public String[] getFeatureColNames() {
		return featureColNames;
	}

	public String getLabelColName() {
		return labelColName;
	}

	public Family getFamily() {
		return family;
	}

	public double getVariancePower() {
		return variancePower;
	}

	public Link getLink() {
		return link;
	}

	public double getLinkPower() {
		return linkPower;
	}

	public double[] getCoefficients() {
		return coefficients;
	}

	public boolean isFitIntercept() {
		return fitIntercept;
	}

	public double getDegreeOfFreedom() {
		return summary.degreeOfFreedom;
	}

	public double getResidualDegreeOfFreeDom() {
		return summary.residualDegreeOfFreeDom;
	}

	public double getResidualDegreeOfFreedomNull() {
		return summary.residualDegreeOfFreedomNull;
	}

	public double getAic() {
		return summary.aic;
	}

	public double getDispersion() {
		return summary.dispersion;
	}

	public double getDeviance() {
		return summary.deviance;
	}

	public double getNullDeviance() {
		return summary.nullDeviance;
	}

	public double[] getTValues() {
		return summary.tValues;
	}

	public double[] getPValues() {
		return summary.pValues;
	}

	public double[] getStdErrors() {
		return summary.coefficientStandardErrors;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		StringBuilder res = new StringBuilder();
		res.append(PrettyDisplayUtils.displayHeadline("GlmModelInfo", '-') + "\n");
		res.append("label: " + labelColName + "\n");
		res.append("family: " + family.name() + "\n");
		res.append("link: " + link.name() + "\n");
		res.append("rank: " + summary.rank + "\n");
		res.append("degreeOfFreedom: " + summary.degreeOfFreedom + "\n");
		res.append("residualDegreeOfFreeDom: " + summary.residualDegreeOfFreeDom + "\n");
		res.append("residualDegreeOfFreedomNull: " + summary.residualDegreeOfFreedomNull + "\n");
		res.append("aic: " + summary.aic + "\n");
		res.append("dispersion: " + summary.dispersion + "\n");
		res.append("deviance: " + summary.deviance + "\n");
		res.append("nullDeviance: " + summary.nullDeviance + "\n");

		String[] rowNames = null;
		if (this.fitIntercept) {
			rowNames = new String[1 + this.featureColNames.length];
			rowNames[0] = "Intercept";
			System.arraycopy(featureColNames, 0, rowNames, 1, featureColNames.length);
		} else {
			rowNames = new String[this.featureColNames.length];
			System.arraycopy(featureColNames, 0, rowNames, 0, featureColNames.length);
		}
		String[] colNames = new String[] {"coef", "stdErr", "z", "P>|z|"};

		Object[][] data = new Object[rowNames.length][colNames.length];
		int startIdx = 0;
		if (this.fitIntercept) {
			data[0][0] = this.intercept;
			data[0][1] = this.summary.coefficientStandardErrors[featureColNames.length];
			data[0][2] = this.summary.tValues[featureColNames.length];
			data[0][3] = this.summary.pValues[featureColNames.length];
			startIdx = 1;
		}

		for (int i = 0; i < featureColNames.length; i++) {
			data[startIdx + i][0] = this.coefficients[i];
			data[startIdx + i][1] = this.summary.coefficientStandardErrors[i];
			data[startIdx + i][2] = this.summary.tValues[i];
			data[startIdx + i][3] = this.summary.pValues[i];
		}

		res.append("\n");
		res.append(PrettyDisplayUtils.displayTable(data, rowNames.length, colNames.length,
			rowNames, colNames, null,
			100, 100) + "\n");

		return res.toString();
	}

}
