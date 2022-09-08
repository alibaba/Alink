package com.alibaba.alink.operator.batch.utils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.utils.AlinkSerializable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class HtmlVizUtils implements AlinkSerializable {

	private static final Logger LOG = LoggerFactory.getLogger(HtmlVizUtils.class);

	private static final String RESOURCE_ROOT = "/html_viz";

	static String readResourceContent(String relativePath) {
		String path = RESOURCE_ROOT + relativePath;
		try (InputStream is = HtmlVizUtils.class.getResourceAsStream(path)) {
			AkPreconditions.checkNotNull(is);
			return IOUtils.toString(is, StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException(String.format("Failed to read resource %s", path), e);
		}
	}

	static void saveOpenHtml(String html) {
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
}
