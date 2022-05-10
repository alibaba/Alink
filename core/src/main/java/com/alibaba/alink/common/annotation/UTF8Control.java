package com.alibaba.alink.common.annotation;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;

/**
 * Make resource bundle able to load UTF-8 encoding property files. See
 * <a
 * href="https://stackoverflow.com/questions/4659929/how-to-use-utf-8-in-resource-properties-with-resourcebundle">this
 * page</a>.
 */
public class UTF8Control extends Control {
	@Override
	public ResourceBundle newBundle
		(String baseName, Locale locale, String format, ClassLoader loader, boolean reload) throws IOException {
		// The below is a copy of the default implementation.
		String bundleName = toBundleName(baseName, locale);
		String resourceName = toResourceName(bundleName, "properties");
		ResourceBundle bundle = null;
		InputStream stream = null;
		if (reload) {
			URL url = loader.getResource(resourceName);
			if (url != null) {
				URLConnection connection = url.openConnection();
				if (connection != null) {
					connection.setUseCaches(false);
					stream = connection.getInputStream();
				}
			}
		} else {
			stream = loader.getResourceAsStream(resourceName);
		}
		if (stream != null) {
			try {
				// Only this line is changed to make it to read properties files as UTF-8.
				bundle = new PropertyResourceBundle(new InputStreamReader(stream, StandardCharsets.UTF_8));
			} finally {
				stream.close();
			}
		}
		return bundle;
	}
}
