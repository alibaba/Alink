package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.StandardCategorySeriesLabelGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.labels.StandardXYSeriesLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.util.SortOrder;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class JFreeChartUtil {

	public static boolean isNumberType(TypeInformation<?> type) {
		return type.equals(Types.INT) || type.equals(Types.LONG) || type.equals(Types.DOUBLE) || type.equals(Types.FLOAT) || type.equals(Types.SHORT);
	}

	public static void generateChart(Insight insight, String filename) {
		switch (insight.type) {
			case Correlation:
				correlation(insight, filename);
				break;
			case CrossMeasureCorrelation:
				crossMeasure(insight, filename);
				break;
			case Clustering2D:
				cluster2D(insight, filename);
				break;
			case OutstandingNo1:
			case OutstandingLast:
				outstandingNo1(insight, filename);
				break;
			case Attribution:
			case OutstandingTop2:
				attribution(insight, filename);
				break;
			case Evenness:
				evenness(insight, filename);
				break;
			case ChangePoint:
			case Outlier:
			case Trend:
			case Seasonality:
				timeSeries(insight, filename);
				break;
			case BasicStat:
			case Distribution:
				break;
			default:
				throw new AkIllegalOperatorParameterException("Insight type not support yet!" + insight.type.name());
		}
	}

	private static void timeSeries(Insight insight, String filename) {
		LayoutData data = insight.layout;
		TimeSeriesCollection timeseriescollection = new TimeSeriesCollection();
		TimeSeries timeseries = new TimeSeries(data.xAxis);
		for (Row row : data.data.getRows()) {
			if (null == row.getField(0) || null == row.getField(1)) {
				continue;
			}
			Timestamp time = (Timestamp) row.getField(0);
			timeseries.add(new Second(new Date(time.getTime())), (Number) row.getField(1));
		}
		timeseriescollection.addSeries(timeseries);
		JFreeChart chart = ChartFactory.createTimeSeriesChart(
			data.title, data.xAxis, data.yAxis, timeseriescollection, true, true, false
		);
		XYPlot xyplot = (XYPlot) chart.getPlot();
		XYItemRenderer xyitemrenderer = xyplot.getRenderer();
		if (xyitemrenderer instanceof XYLineAndShapeRenderer) {
			XYLineAndShapeRenderer xylineandshaperenderer = (XYLineAndShapeRenderer) xyitemrenderer;
			xylineandshaperenderer.setBaseShapesVisible(true);
			xylineandshaperenderer.setBaseShapesFilled(true);
		}
		saveChart(chart, filename, 500, 500);
	}

	private static void evenness(Insight insight, String filename) {
		LayoutData data = insight.layout;
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		for (Row row : data.data.getRows()) {
			if (null == row.getField(0) || null == row.getField(1)) {
				continue;
			}
			dataset.addValue((Number) row.getField(1), data.xAxis, (Comparable) row.getField(0));
		}
		JFreeChart chart = ChartFactory.createLineChart(
			data.title, data.xAxis, data.yAxis, dataset, PlotOrientation.VERTICAL, true, true, false
		);
		CategoryPlot xyplot = (CategoryPlot) chart.getPlot();
		LineAndShapeRenderer renderer = new LineAndShapeRenderer();
		renderer.setLegendItemLabelGenerator(new StandardCategorySeriesLabelGenerator());
		renderer.setSeriesPaint(0, Color.RED);
		renderer.setBaseShapesFilled(true);
		renderer.setBaseShapesVisible(true);
		xyplot.setRenderer(renderer);
		saveChart(chart, filename, 500, 500);
	}

	private static void attribution(Insight insight, String filename) {
		LayoutData data = insight.layout;
		DefaultPieDataset dataset = new DefaultPieDataset();
		for (Row row : data.data.getRows()) {
			dataset.setValue((Comparable) row.getField(0), (Number) row.getField(1));
		}
		dataset.sortByValues(SortOrder.DESCENDING);

		JFreeChart chart = ChartFactory.createPieChart(
			data.title, dataset, true, true, false
		);
		PiePlot plot = (PiePlot) chart.getPlot();
		plot.setLabelGenerator(new StandardPieSectionLabelGenerator("{0} {2}"));
		saveChart(chart, filename, 500, 500);
	}

	private static void outstandingNo1(Insight insight, String filename) {
		LayoutData data = insight.layout;
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		List <Tuple2 <Comparable, Number>> list = new ArrayList<>();
		for (Row row : data.data.getRows()) {
			if (null == row.getField(0) || null == row.getField(1)) {
				continue;
			}
			list.add(Tuple2.of((Comparable) row.getField(0), (Number) row.getField(1)));
		}
		list.sort(new Comparator <Tuple2 <Comparable, Number>>() {
			@Override
			public int compare(Tuple2 <Comparable, Number> t1, Tuple2 <Comparable, Number> t2) {
				return new BigDecimal(t2.f1.toString()).compareTo(new BigDecimal(t1.f1.toString()));
			}
		});
		for (int i = 0; i < list.size(); i++) {
			dataset.setValue(list.get(i).f1, data.xAxis, list.get(i).f0);
		}
		JFreeChart chart = ChartFactory.createBarChart(
			data.title, data.xAxis, data.yAxis, dataset, PlotOrientation.VERTICAL, true, true, false
		);
		saveChart(chart, filename, 500, 500);
	}

	private static void cluster2D(Insight insight, String filename) {
		LayoutData data = insight.layout;
		Map <Integer, XYSeries> seriesMap = new HashMap <>();
		for (Row row : data.data.getRows()) {
			if (null == row.getField(0) || null == row.getField(1)
				|| null == row.getField(2) || null == row.getField(3)) {
				continue;
			}
			Integer clusterid = (Integer) row.getField(3);
			if (!seriesMap.containsKey(clusterid)) {
				XYSeries series = new XYSeries(clusterid.toString());
				seriesMap.put(clusterid, series);
			}
			seriesMap.get(clusterid).add((Number) row.getField(1), (Number) row.getField(2));
		}

		XYSeriesCollection dataset = new XYSeriesCollection();
		for (Entry <Integer, XYSeries> entry : seriesMap.entrySet()) {
			dataset.addSeries(entry.getValue());
		}
		JFreeChart chart = ChartFactory.createScatterPlot(
			data.title, data.xAxis, data.yAxis, dataset, PlotOrientation.HORIZONTAL, true, true, false
		);
		saveChart(chart, filename, 500, 500);
	}

	private static void crossMeasure(Insight insight, String filename) {
		LayoutData data = insight.layout;
		XYSeries series = new XYSeries(data.title);
		for (Row row : data.data.getRows()) {
			if (null == row.getField(0) || null == row.getField(1)) {
				continue;
			}
			series.add((Number) row.getField(1), (Number) row.getField(2));
		}
		XYSeriesCollection dataset = new XYSeriesCollection();
		dataset.addSeries(series);
		JFreeChart chart = ChartFactory.createScatterPlot(
			data.title, data.xAxis, data.yAxis, dataset, PlotOrientation.HORIZONTAL, true, true, false
		);
		saveChart(chart, filename, 500, 500);
 	}

	private static void correlation(Insight insight, String filename) {
		LayoutData data = insight.layout;
		TypeInformation <?> keyType = data.data.getColTypes()[0];
		JFreeChart chart = null;
		double maxA = Double.MIN_VALUE;
		double maxB = Double.MIN_VALUE;
		double minA = Double.MAX_VALUE;
		double minB = Double.MAX_VALUE;

		if (isNumberType(keyType)) {
			XYSeries seriesA = new XYSeries(data.lineA);
			XYSeries seriesB = new XYSeries(data.lineB);
			for (Row row : data.data.getRows()) {
				if (null == row.getField(0) || null == row.getField(1) || null == row.getField(2)) {
					continue;
				}
				seriesA.add((Number) row.getField(0), (Number) row.getField(1));
				seriesB.add((Number) row.getField(0), (Number) row.getField(2));
				double a = Double.valueOf(String.valueOf(row.getField(1)));
				double b = Double.valueOf(String.valueOf(row.getField(2)));
				maxA = Math.max(a, maxA);
				minA = Math.min(a, minA);
				maxB = Math.max(b, maxB);
				minB = Math.min(b, minB);
			}
			XYSeriesCollection datasetA = new XYSeriesCollection();
			XYSeriesCollection datasetB = new XYSeriesCollection();
			datasetA.addSeries(seriesA);
			datasetB.addSeries(seriesB);
			chart = ChartFactory.createXYLineChart(
				data.title, data.xAxis, data.yAxis, null, PlotOrientation.HORIZONTAL, true, true, false
			);
			XYPlot xyplot = (XYPlot) chart.getPlot();
			xyplot.setDataset(0, datasetA);
			xyplot.setDataset(1, datasetB);
			XYLineAndShapeRenderer renderer1 = new XYLineAndShapeRenderer();
			renderer1.setLegendItemLabelGenerator(new StandardXYSeriesLabelGenerator());
			renderer1.setSeriesPaint(0, Color.RED);
			renderer1.setBaseShapesFilled(true);
			renderer1.setBaseShapesVisible(true);

			XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer();
			renderer2.setLegendItemLabelGenerator(new StandardXYSeriesLabelGenerator());
			renderer2.setSeriesPaint(0, Color.BLUE);
			renderer2.setBaseShapesFilled(true);
			renderer2.setBaseShapesVisible(true);
			xyplot.setRenderer(0, renderer1);
			xyplot.setRenderer(1, renderer2);
			NumberAxis axisA = new NumberAxis();
			axisA.setRange(minA * 0.9, maxA * 1.1);
			NumberAxis axisB = new NumberAxis();
			axisB.setRange(minB * 0.9, maxB * 1.1);
			xyplot.setRangeAxis(0, axisA);
			xyplot.setRangeAxis(1, axisB);
			//xyplot.mapDatasetToRangeAxis(0, 0);
			xyplot.mapDatasetToRangeAxis(1, 1);
		} else {
			DefaultCategoryDataset datasetA = new DefaultCategoryDataset();
			DefaultCategoryDataset datasetB = new DefaultCategoryDataset();
			ArrayList<Tuple3 <String, Number, Number>> values = new ArrayList <>();
			for (Row row : data.data.getRows()) {
				if (null == row.getField(0) || null == row.getField(1) || null == row.getField(2)) {
					continue;
				}
				values.add(Tuple3.of(String.valueOf(row.getField(0)),(Number) row.getField(1),(Number) row.getField(2)));
				double a = Double.parseDouble(String.valueOf(row.getField(1)));
				double b = Double.parseDouble(String.valueOf(row.getField(2)));
				maxA = Math.max(a, maxA);
				minA = Math.min(a, minA);
				maxB = Math.max(b, maxB);
				minB = Math.min(b, minB);
			}
			values.sort(new Comparator <Tuple3 <String, Number, Number>>() {
				@Override
				//public int compare(Tuple3 <String, Number, Number> o1, Tuple3 <String, Number, Number> o2) {
				//	return o1.f0.compareTo(o2.f0);
				//}
				public int compare(Tuple3 <String, Number, Number> t1, Tuple3 <String, Number, Number> t2) {
					return new BigDecimal(t2.f1.toString()).compareTo(new BigDecimal(t1.f1.toString()));
				}
			});
			for (Tuple3 <String, Number, Number> t : values) {
				datasetA.addValue(t.f1, data.lineA, t.f0);
				datasetB.addValue(t.f2, data.lineB, t.f0);
			}
			chart = ChartFactory.createLineChart(
				data.title, data.xAxis, data.yAxis, null, PlotOrientation.VERTICAL, true, true, false
			);
			CategoryPlot xyplot = chart.getCategoryPlot();
			xyplot.setDataset(0, datasetA);
			xyplot.setDataset(1, datasetB);
			LineAndShapeRenderer renderer1 = new LineAndShapeRenderer();
			renderer1.setLegendItemLabelGenerator(new StandardCategorySeriesLabelGenerator());
			renderer1.setSeriesPaint(0, Color.RED);
			renderer1.setBaseShapesFilled(true);
			renderer1.setBaseShapesVisible(true);

			LineAndShapeRenderer renderer2 = new LineAndShapeRenderer();
			renderer2.setLegendItemLabelGenerator(new StandardCategorySeriesLabelGenerator());
			renderer2.setSeriesPaint(0, Color.BLUE);
			renderer2.setBaseShapesFilled(true);
			renderer2.setBaseShapesVisible(true);
			xyplot.setRenderer(0, renderer1);
			xyplot.setRenderer(1, renderer2);
			NumberAxis axisA = new NumberAxis();
			axisA.setRange(minA * 0.9, maxA * 1.1);
			NumberAxis axisB = new NumberAxis();
			axisB.setRange(minB * 0.9, maxB * 1.1);
			xyplot.setRangeAxis(0, axisA);
			xyplot.setRangeAxis(1, axisB);
			//xyplot.mapDatasetToRangeAxis(0, 0);
			xyplot.mapDatasetToRangeAxis(1, 1);
		}

		saveChart(chart, filename, 500, 500);

	}

	private static void saveChart(JFreeChart chart, String filename, int width, int height) {
		try {
			ChartUtilities.saveChartAsJPEG(new File(filename), chart, width, height);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
