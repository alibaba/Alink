package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DiveVisualizer {

	private static final DiveVisualizer INSTANCE = new DiveVisualizer();

	private final String template;

	private DiveVisualizer() {
		try (InputStream templateStream = DiveVisualizer.class.getResourceAsStream("/facets_dive_template.html")) {
			assert templateStream != null;
			template = IOUtils.toString(templateStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static DiveVisualizer getInstance() {
		return INSTANCE;
	}

	public void visualize(String jsonStr) {
		String html = template.replace("{jsonstr}", jsonStr);
		String path;
		try {
			File file = File.createTempFile("alink_statis_", ".html");
			FileUtils.writeStringToFile(file, html, StandardCharsets.UTF_8);
			path = file.getAbsolutePath();
		} catch (IOException e) {
			throw new RuntimeException("Failed to write html to file.", e);
		}
		try {
			Desktop desktop = Desktop.getDesktop();
			desktop.open(new File(path));
		} catch (IOException e) {
			throw new RuntimeException("Open webpage failed.", e);
		}
	}

	public static class DiveVisualizerConsumer implements Consumer<List<Row>> {
		private String[] colNames;

		public DiveVisualizerConsumer(String[] colNames) {
			this.colNames = colNames;
		}

		@Override
		public void accept(List <Row> rows) {
			List<Map <String, Object>> jsonObj = new ArrayList <>();

			for (Row row : rows) {
				Map<String, Object> rowJsonObj = new HashMap <>();

				for (int i = 0; i < row.getArity(); ++i) {
					rowJsonObj.put(colNames[i], row.getField(i));
				}

				jsonObj.add(rowJsonObj);
			}

			DiveVisualizer.getInstance().visualize(JsonConverter.toJson(jsonObj));
		}
	}

}
