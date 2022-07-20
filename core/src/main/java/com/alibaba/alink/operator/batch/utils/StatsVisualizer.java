package com.alibaba.alink.operator.batch.utils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.metadata.def.shaded.com.google.protobuf.InvalidProtocolBufferException;
import com.alibaba.alink.metadata.def.shaded.com.google.protobuf.util.JsonFormat;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatistics;
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
import java.util.stream.Collectors;

public class StatsVisualizer {
	private static final Logger LOG = LoggerFactory.getLogger(StatsVisualizer.class);

	private static final StatsVisualizer INSTANCE = new StatsVisualizer();

	private final String template;
	private final String newTemplate;

	private StatsVisualizer() {
		try (InputStream templateStream = StatsVisualizer.class.getResourceAsStream("/facets_template.html");
			 InputStream newTemplateStream = StatsVisualizer.class.getResourceAsStream("/mtvis.html")) {
			assert templateStream != null;
			assert newTemplateStream != null;
			template = IOUtils.toString(templateStream);
			newTemplate = IOUtils.toString(newTemplateStream);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("Failed to read resource /facets_template.html", e);
		}
	}

	public static StatsVisualizer getInstance() {
		return INSTANCE;
	}

	private static void saveOpenHtml(String html) {
		String path;
		try {
			File file = File.createTempFile("alink_stats_", ".html");
			FileUtils.writeStringToFile(file, html, StandardCharsets.UTF_8);
			path = file.getAbsolutePath();
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("Failed to write html to file.", e);
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

	private String generateHtmlTitle(DatasetFeatureStatisticsList datasetFeatureStatisticsList,
									 String[] newTableNames) {
		String title;
		if (null != newTableNames && newTableNames.length > 0) {
			AkPreconditions.checkArgument(
				datasetFeatureStatisticsList.getDatasetsCount() == newTableNames.length,
				new AkIllegalDataException("The number of new table names must be equal to the number of datasets."));
			title = String.join(", ", newTableNames) + "'s Stats";
		} else {
			title = datasetFeatureStatisticsList.getDatasetsList()
				.stream().map(DatasetFeatureStatistics::getName)
				.collect(Collectors.joining(", ")) + "'s Stats";
		}
		return title;
	}

	private String generateHtml(DatasetFeatureStatisticsList datasetFeatureStatisticsList,
								String[] newTableNames) {
		String protoStr = Base64.getEncoder().encodeToString(datasetFeatureStatisticsList.toByteArray());
		String title = generateHtmlTitle(datasetFeatureStatisticsList, newTableNames);
		return template.replace("{title}", title).replace("{protostr}", protoStr);
	}

	public void visualize(DatasetFeatureStatisticsList datasetFeatureStatisticsList, String[] newTableNames) {
		String html = generateHtml(datasetFeatureStatisticsList, newTableNames);
		saveOpenHtml(html);
	}

	@Internal
	private String generateHtmlNew(DatasetFeatureStatisticsList datasetFeatureStatisticsList,
								   String[] newTableNames) {
		String protoJson;
		try {
			protoJson = JsonFormat.printer().includingDefaultValueFields().print(datasetFeatureStatisticsList);
		} catch (InvalidProtocolBufferException e) {
			throw new AkUnclassifiedErrorException(
				"Failed to convert the DatasetFeatureStatisticsList instance to a JSON string.", e);
		}
		String b64ProtoJson = Base64.getEncoder().encodeToString(protoJson.getBytes(StandardCharsets.UTF_8));
		String title = generateHtmlTitle(datasetFeatureStatisticsList, newTableNames);
		return newTemplate.replace("{title}", title).replace("{b64_proto_json}", b64ProtoJson);
	}

	@Internal
	public void visualizeNew(DatasetFeatureStatisticsList datasetFeatureStatisticsList,
							 String[] newTableNames) {
		String html = generateHtmlNew(datasetFeatureStatisticsList, newTableNames);
		saveOpenHtml(html);
	}
}
