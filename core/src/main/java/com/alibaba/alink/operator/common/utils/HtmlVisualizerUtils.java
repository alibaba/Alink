package com.alibaba.alink.operator.common.utils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class HtmlVisualizerUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HtmlVisualizerUtils.class);

	public static String readTemplate(String resourcePath) {
		try (InputStream templateStream = OutlierMetrics.class.getResourceAsStream(resourcePath)) {
			assert templateStream != null;
			return IOUtils.toString(templateStream);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException("Failed to read resource " + resourcePath, e);
		}
	}

	public static void saveOpenHtml(String filenamePrefix, String html) {
		String path;
		try {
			File file = File.createTempFile(filenamePrefix, ".html");
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
