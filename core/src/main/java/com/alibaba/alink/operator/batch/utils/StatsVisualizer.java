package com.alibaba.alink.operator.batch.utils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatisticsList;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class StatsVisualizer {
	private static final Logger LOG = LoggerFactory.getLogger(StatsVisualizer.class);

	private static final StatsVisualizer INSTANCE = new StatsVisualizer();

	private final String template;

	private StatsVisualizer() {
		try (InputStream templateStream = StatsVisualizer.class.getResourceAsStream("/facets_template.html")) {
			assert templateStream != null;
			template = IOUtils.toString(templateStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static StatsVisualizer getInstance() {
		return INSTANCE;
	}

	public void visualize(DatasetFeatureStatisticsList datasetFeatureStatisticsList) {
		String protoStr = Base64.getEncoder().encodeToString(datasetFeatureStatisticsList.toByteArray());
		String html = template
			.replace("{title}", datasetFeatureStatisticsList.getDatasets(0).getName() + "'s Stats")
			.replace("{protostr}", protoStr);
		String path;
		try {
			File file = File.createTempFile("alink_stats_", ".html");
			FileUtils.writeStringToFile(file, html, StandardCharsets.UTF_8);
			path = file.getAbsolutePath();
		} catch (IOException e) {
			throw new RuntimeException("Failed to write html to file.", e);
		}
		try {
			Desktop desktop = Desktop.getDesktop();
			desktop.open(new File(path));
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("Opening webpage: " + path);
			}
		} catch (Exception e) {
			LOG.info("Open webpage {} failed.", path, e);
		}
	}
}
