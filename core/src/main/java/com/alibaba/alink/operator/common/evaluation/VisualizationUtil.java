package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;
import org.jfree.chart.ChartColor;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.HorizontalAlignment;
import org.jfree.ui.RectangleEdge;

import java.awt.*;
import java.io.File;
import java.io.IOException;

/**
 * Some Functions for Visualization.
 */
public class VisualizationUtil {
    static void saveAsImage(String path,
                            boolean isOverwrite,
                            String title,
                            String xLabel,
                            String yLabel,
                            String[] keys,
                            Tuple2<String, Double> subTitle,
                            Tuple2<double[], double[]>... curvePoints) throws IOException {
        File file = new File(path);

        Preconditions.checkArgument(
            isOverwrite || !file.exists(),
            "File: %s is exists.", path
        );

        Preconditions.checkNotNull(curvePoints, "Points should not be null!");
        XYSeriesCollection seriesCollection = new XYSeriesCollection();
        for(int i = 0; i < curvePoints.length; i++){
            XYSeries series = new XYSeries(keys[i]);
            for(int j = 0; j < curvePoints[i].f0.length; j++) {
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
        for(int i = 0; i < keys.length; i++) {
            p.getRenderer().setSeriesStroke(i, new BasicStroke(2.0f));
        }

        if(null != subTitle) {
            TextTitle source = new TextTitle(subTitle.f0 + ":" + FeatureBinsUtil.keepGivenDecimal(subTitle.f1, 3));
            source.setFont(new Font("SansSerif", Font.PLAIN, 15));
            source.setPosition(RectangleEdge.TOP);
            source.setHorizontalAlignment(HorizontalAlignment.LEFT);
            chart.addSubtitle(source);
        }
        ChartUtilities.saveChartAsJPEG(file, chart, 500, 400);
    }
}
