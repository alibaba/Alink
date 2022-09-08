package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DiveVisualizer {

	private static final DiveVisualizer INSTANCE = new DiveVisualizer();

	private final String TEMPLATE;

	private final String IFRAME_TEMPLATE;

	private DiveVisualizer() {
		final String toBeReplaced
			= "<link rel=\"import\" href=\"https://raw.githubusercontent.com/PAIR-code/facets/master/facets-dist/facets-jupyter.html\">";
		final ClassLoader cl = getClass().getClassLoader();
		final String replaceContent = HtmlVizUtils.readResourceContent("/facets-jupyter.html");
		TEMPLATE = HtmlVizUtils.readResourceContent("/facets_dive_template.html")
			.replace(toBeReplaced, replaceContent);
		IFRAME_TEMPLATE = HtmlVizUtils.readResourceContent("/iframe_facets_dive_template.html")
			.replace(toBeReplaced, replaceContent);
	}

	public static DiveVisualizer getInstance() {
		return INSTANCE;
	}

	public String generateHtml(String jsonStr) {
		return TEMPLATE.replace("{jsonstr}", jsonStr);
	}

	public void visualize(String jsonStr) {
		HtmlVizUtils.saveOpenHtml(generateHtml(jsonStr));
	}

	/**
	 * Generate iframe version of HTML for Jupyter Notebook usage.
	 */
	public String generateIframeHtml(String jsonStr) {
		return IFRAME_TEMPLATE.replace("{jsonstr}", jsonStr);
	}

	public static class DiveVisualizerConsumer implements Consumer <List <Row>> {
		private final String[] colNames;

		public DiveVisualizerConsumer(String[] colNames) {
			this.colNames = colNames;
		}

		@Override
		public void accept(List <Row> rows) {
			List <Map <String, Object>> jsonObj = new ArrayList <>();

			for (Row row : rows) {
				Map <String, Object> rowJsonObj = new HashMap <>();

				for (int i = 0; i < row.getArity(); ++i) {
					rowJsonObj.put(colNames[i], row.getField(i));
				}

				jsonObj.add(rowJsonObj);
			}

			DiveVisualizer.getInstance().visualize(JsonConverter.toJson(jsonObj));
		}
	}
}
