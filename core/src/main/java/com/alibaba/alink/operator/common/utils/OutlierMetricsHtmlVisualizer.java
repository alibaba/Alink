package com.alibaba.alink.operator.common.utils;

import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class OutlierMetricsHtmlVisualizer {
	public static final String RESOURCE_PATH = "/classification_eval_template.html";

	public static final String template = HtmlVisualizerUtils.readTemplate(RESOURCE_PATH);

	private static final OutlierMetricsHtmlVisualizer INSTANCE = new OutlierMetricsHtmlVisualizer();

	public static OutlierMetricsHtmlVisualizer getInstance() {
		return INSTANCE;
	}

	private String generateHtml(OutlierMetrics outlierMetrics) {
		String content = outlierMetrics.getParams().toJson();
		String b64Content = Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
		final String toBeReplaced = "{b64_data}";
		return template.replace(toBeReplaced, b64Content);
	}

	public void visualize(OutlierMetrics outlierMetrics) {
		String html = generateHtml(outlierMetrics);
		HtmlVisualizerUtils.saveOpenHtml("outlier_eval_report_", html);
	}
}
