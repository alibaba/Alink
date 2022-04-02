package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.operator.common.tree.viz.TreeModelViz;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import javax.imageio.ImageIO;
import javax.imageio.stream.FileImageOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.jfree.chart.ChartColor;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.HorizontalAlignment;
import org.jfree.ui.RectangleEdge;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * Base classes for binary classification evaluation metrics and outlier evaluation metrics.
 * <p>
 * {@link BinaryClassMetrics} is actually identical to this class, except the type T is set.
 */
public class BaseBinaryClassMetrics<T extends BaseBinaryClassMetrics<T>> extends BaseSimpleClassifierMetrics <T> {
	private static final long serialVersionUID = -4566069100819806462L;

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("Metrics:", '-'));
		String[] labels = getLabelArray();

		String[][] confusionMatrixStr = new String[2][2];
		long[][] confusionMatrix = getConfusionMatrix();
		confusionMatrixStr[0][0] = String.valueOf(confusionMatrix[0][0]);
		confusionMatrixStr[0][1] = String.valueOf(confusionMatrix[0][1]);
		confusionMatrixStr[1][0] = String.valueOf(confusionMatrix[1][0]);
		confusionMatrixStr[1][1] = String.valueOf(confusionMatrix[1][1]);

		sbd.append("Auc:").append(PrettyDisplayUtils.display(getAuc())).append("\t")
			.append("Accuracy:").append(PrettyDisplayUtils.display(getAccuracy())).append("\t")
			.append("Precision:").append(PrettyDisplayUtils.display(getPrecision())).append("\t")
			.append("Recall:").append(PrettyDisplayUtils.display(getRecall())).append("\t")
			.append("F1:").append(PrettyDisplayUtils.display(getF1())).append("\t")
			.append("LogLoss:").append(PrettyDisplayUtils.display(getLogLoss())).append("\n")
			.append(PrettyDisplayUtils.displayTable(confusionMatrixStr,
				2, 2, labels, labels, "Pred\\Real"));
		return sbd.toString();
	}

	static final ParamInfo <double[][]> ROC_CURVE = ParamInfoFactory
		.createParamInfo("RocCurve", double[][].class)
		.setDescription("auc")
		.setRequired()
		.build();

	public static final ParamInfo <Double> AUC = ParamInfoFactory
		.createParamInfo("AUC", Double.class)
		.setDescription("auc")
		.setRequired()
		.build();

	public static final ParamInfo <Double> GINI = ParamInfoFactory
		.createParamInfo("GINI", Double.class)
		.setDescription("GINI")
		.setRequired()
		.build();

	public static final ParamInfo <Double> KS = ParamInfoFactory
		.createParamInfo("K-S", Double.class)
		.setDescription("ks")
		.setRequired()
		.build();

	public static final ParamInfo <Double> PRC = ParamInfoFactory
		.createParamInfo("PRC", Double.class)
		.setDescription("ks")
		.setRequired()
		.build();

	static final ParamInfo <double[][]> RECALL_PRECISION_CURVE = ParamInfoFactory
		.createParamInfo("RecallPrecisionCurve", double[][].class)
		.setDescription("recall precision curve")
		.setRequired()
		.build();

	static final ParamInfo <double[][]> LIFT_CHART = ParamInfoFactory
		.createParamInfo("LiftChart", double[][].class)
		.setDescription("liftchart")
		.setRequired()
		.build();

	public static final ParamInfo <double[][]> LORENZ_CURVE = ParamInfoFactory
		.createParamInfo("LorenzCurve", double[][].class)
		.setDescription("lorenzCurve")
		.setRequired()
		.build();

	static final ParamInfo <double[]> THRESHOLD_ARRAY = ParamInfoFactory
		.createParamInfo("ThresholdArray", double[].class)
		.setDescription("threshold list")
		.setRequired()
		.build();

	static final ParamInfo <Double> PRECISION = ParamInfoFactory
		.createParamInfo("Precision", Double.class)
		.setDescription("precision")
		.setRequired()
		.build();

	static final ParamInfo <Double> RECALL = ParamInfoFactory
		.createParamInfo("Recall", Double.class)
		.setDescription("recall")
		.setRequired()
		.build();

	static final ParamInfo <Double> F1 = ParamInfoFactory
		.createParamInfo("F1", Double.class)
		.setDescription("f1")
		.setRequired()
		.build();

	public BaseBinaryClassMetrics(Row row) {
		super(row);
	}

	public BaseBinaryClassMetrics(Params params) {
		super(params);
	}

	public Tuple2 <double[], double[]> getRocCurve() {
		double[][] curve = getParams().get(ROC_CURVE);
		return Tuple2.of(curve[0], curve[1]);
	}

	public Tuple2 <double[], double[]> getLorenzeCurve() {
		double[][] curve = getParams().get(LORENZ_CURVE);
		return Tuple2.of(curve[0], curve[1]);
	}

	public Double getPrecision() {
		return get(PRECISION);
	}

	public Double getRecall() {
		return get(RECALL);
	}

	public Double getF1() {
		return get(F1);
	}

	public Double getAuc() {
		return get(AUC);
	}

	public Double getGini() {
		return get(GINI);
	}

	public Double getKs() {
		return get(KS);
	}

	public Double getPrc() {
		return get(PRC);
	}

	public Tuple2 <double[], double[]> getRecallPrecisionCurve() {
		double[][] curve = getParams().get(RECALL_PRECISION_CURVE);
		return Tuple2.of(curve[0], curve[1]);
	}

	public Tuple2 <double[], double[]> getLiftChart() {
		double[][] curve = getParams().get(LIFT_CHART);
		return Tuple2.of(curve[0], curve[1]);
	}

	public double[] getThresholdArray() {
		return get(THRESHOLD_ARRAY);
	}

	public Tuple2 <double[], double[]> getPrecisionByThreshold() {
		return Tuple2.of(getParams().get(THRESHOLD_ARRAY), getParams().get(PRECISION_ARRAY));
	}

	public Tuple2 <double[], double[]> getSpecificityByThreshold() {
		return Tuple2.of(getParams().get(THRESHOLD_ARRAY), getParams().get(SPECIFICITY_ARRAY));
	}

	public Tuple2 <double[], double[]> getSensitivityByThreshold() {
		return Tuple2.of(getParams().get(THRESHOLD_ARRAY), getParams().get(SENSITIVITY_ARRAY));
	}

	public Tuple2 <double[], double[]> getRecallByThreshold() {
		return Tuple2.of(getParams().get(THRESHOLD_ARRAY), getParams().get(RECALL_ARRAY));
	}

	public Tuple2 <double[], double[]> getF1ByThreshold() {
		return Tuple2.of(getParams().get(THRESHOLD_ARRAY), getParams().get(F1_ARRAY));
	}

	public Tuple2 <double[], double[]> getAccuracyByThreshold() {
		return Tuple2.of(getParams().get(THRESHOLD_ARRAY), getParams().get(ACCURACY_ARRAY));
	}

	public Tuple2 <double[], double[]> getKappaByThreshold() {
		return Tuple2.of(getParams().get(THRESHOLD_ARRAY), getParams().get(KAPPA_ARRAY));
	}

	static void saveAsImage(String path,
							boolean isOverwrite,
							String title,
							String xLabel,
							String yLabel,
							String[] keys,
							Tuple2 <String, Double> subTitle,
							Tuple2 <double[], double[]>... curvePoints) throws IOException {
		File file = new File(path);

		Preconditions.checkArgument(
			isOverwrite || !file.exists(),
			"File: %s is exists.", path
		);

		Preconditions.checkNotNull(curvePoints, "Points should not be null!");
		XYSeriesCollection seriesCollection = new XYSeriesCollection();
		for (int i = 0; i < curvePoints.length; i++) {
			XYSeries series = new XYSeries(keys[i]);
			for (int j = 0; j < curvePoints[i].f0.length; j++) {
				series.add(curvePoints[i].f0[j], curvePoints[i].f1[j]);
			}
			seriesCollection.addSeries(series);
		}

		JFreeChart chart = ChartFactory.createXYLineChart(title,
			xLabel,
			yLabel,
			seriesCollection,
			PlotOrientation.VERTICAL,
			keys.length > 1,
			true,
			false);
		chart.setBackgroundPaint(Color.white);
		XYPlot p = chart.getXYPlot();
		p.setBackgroundPaint(ChartColor.WHITE);
		p.setRangeGridlinePaint(ChartColor.BLACK);
		p.setDomainGridlinePaint(ChartColor.BLACK);
		p.setOutlinePaint(null);
		p.getRenderer().setSeriesPaint(0, ChartColor.BLACK);
		for (int i = 0; i < keys.length; i++) {
			p.getRenderer().setSeriesStroke(i, new BasicStroke(2.0f));
		}

		if (null != subTitle) {
			TextTitle source = new TextTitle(subTitle.f0 + ":" + FeatureBinsUtil.keepGivenDecimal(subTitle.f1, 3));
			source.setFont(new Font("SansSerif", Font.PLAIN, 15));
			source.setPosition(RectangleEdge.TOP);
			source.setHorizontalAlignment(HorizontalAlignment.LEFT);
			chart.addSubtitle(source);
		}

		String format = TreeModelViz.getFormat(path);
		BufferedImage image = chart.createBufferedImage(500, 400,
			BufferedImage.TYPE_INT_RGB, null);
		ImageIO.write(image, format, new FileImageOutputStream(file));
		//ChartUtilities.saveChartAsJPEG(file, chart, 500, 400);
	}

	public void saveRocCurveAsImage(String path, boolean isOverwrite) throws IOException {
		saveAsImage(path,
			isOverwrite,
			"ROC Curve",
			"FPR",
			"TPR",
			new String[] {"ROC"},
			Tuple2.of("AUC", getAuc()),
			getRocCurve());
	}

	public void saveKSAsImage(String path, boolean isOverwrite) throws IOException {
		double[] thresholdArray = getThresholdArray();
		double[] tprArray = getRocCurve().f1;
		double[] fprArray = getRocCurve().f0;
		ArrayUtils.reverse(thresholdArray);
		ArrayUtils.reverse(tprArray);
		ArrayUtils.reverse(fprArray);
		saveAsImage(path,
			isOverwrite,
			"K-S Curve",
			"Thresholds",
			"Rate",
			new String[] {"TPR", "FPR"},
			Tuple2.of("KS", getKs()),
			Tuple2.of(thresholdArray, tprArray),
			Tuple2.of(thresholdArray, fprArray));
	}

	public void saveLiftChartAsImage(String path, boolean isOverwrite) throws IOException {
		saveAsImage(path,
			isOverwrite,
			"LiftChart",
			"Positive Rate",
			"True Positive",
			new String[] {"LiftChart"},
			null,
			getLiftChart());
	}

	public void saveRecallPrecisionCurveAsImage(String path, boolean isOverwrite) throws IOException {
		saveAsImage(path,
			isOverwrite,
			"RecallPrecisionCurve",
			"Recall",
			"Precision",
			new String[] {"RecallPrecision"},
			Tuple2.of("PRC", getPrc()),
			getRecallPrecisionCurve());
	}

	public void saveLorenzCurveAsImage(String path, boolean isOverwrite) throws IOException {
		saveAsImage(path,
			isOverwrite,
			"LorenzCurve",
			"Positive Rate",
			"TPR",
			new String[] {"LorenzCurve"},
			Tuple2.of("GINI", getGini()),
			getLorenzeCurve());
	}
}
