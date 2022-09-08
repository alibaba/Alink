package com.alibaba.alink.operator.batch.utils;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.metadata.def.shaded.com.google.protobuf.InvalidProtocolBufferException;
import com.alibaba.alink.metadata.def.shaded.com.google.protobuf.util.JsonFormat;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatistics;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatisticsList;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Collectors;

public class StatsVisualizer {
	private static final StatsVisualizer INSTANCE = new StatsVisualizer();

	private final String TEMPLATE;

	private final String IFRAME_TEMPLATE;
	private final String NEW_TEMPLATE;

	private StatsVisualizer() {
		final String toBeReplaced
			= "<link rel=\"import\" href=\"https://raw.githubusercontent.com/PAIR-code/facets/master/facets-dist/facets-jupyter.html\">";
		final ClassLoader cl = getClass().getClassLoader();
		final String replaceContent = HtmlVizUtils.readResourceContent("/facets-jupyter.html");
		TEMPLATE = HtmlVizUtils.readResourceContent("/facets_template.html")
			.replace(toBeReplaced, replaceContent);
		IFRAME_TEMPLATE = HtmlVizUtils.readResourceContent("/iframe_facets_template.html")
			.replace(toBeReplaced, replaceContent);
		NEW_TEMPLATE = HtmlVizUtils.readResourceContent("/mtvis.html");
	}

	public static StatsVisualizer getInstance() {
		return INSTANCE;
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

	public String generateHtml(DatasetFeatureStatisticsList datasetFeatureStatisticsList, String[] newTableNames) {
		String protoStr = Base64.getEncoder().encodeToString(datasetFeatureStatisticsList.toByteArray());
		String title = generateHtmlTitle(datasetFeatureStatisticsList, newTableNames);
		return TEMPLATE.replace("{title}", title).replace("{protostr}", protoStr);
	}

	public void visualize(DatasetFeatureStatisticsList datasetFeatureStatisticsList, String[] newTableNames) {
		String html = generateHtml(datasetFeatureStatisticsList, newTableNames);
		HtmlVizUtils.saveOpenHtml(html);
	}

	/**
	 * Generate iframe version of HTML for Jupyter Notebook usage.
	 */
	@SuppressWarnings("unused")
	public String generateIframeHtml(DatasetFeatureStatisticsList datasetFeatureStatisticsList) {
		String protoStr = Base64.getEncoder().encodeToString(datasetFeatureStatisticsList.toByteArray());
		return IFRAME_TEMPLATE.replace("{protostr}", protoStr);
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
		return NEW_TEMPLATE.replace("{title}", title).replace("{b64_proto_json}", b64ProtoJson);
	}

	@Internal
	public void visualizeNew(DatasetFeatureStatisticsList datasetFeatureStatisticsList,
							 String[] newTableNames) {
		String html = generateHtmlNew(datasetFeatureStatisticsList, newTableNames);
		HtmlVizUtils.saveOpenHtml(html);
	}
}
