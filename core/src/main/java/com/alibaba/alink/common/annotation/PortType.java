package com.alibaba.alink.common.annotation;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Describe data type represented by the port.
 * <p>
 * When adding a new type or changing an existing type, update the resource bundle files accordingly
 * (src/main/resources/i18n/port_types.properties, src/main/resources/i18n/port_types_zh_CN.properties).
 */
public enum PortType implements Internationalizable {
	MODEL,
	DATA,
	MODEL_STREAM,
	EVAL_METRICS,
	ANY;

	public static final ResourceBundle PORT_TYPE_CN_BUNDLE = ResourceBundle.getBundle(
		"i18n/port_types", new Locale("zh", "CN"), new UTF8Control());
	public static final ResourceBundle PORT_TYPE_EN_BUNDLE = ResourceBundle.getBundle(
		"i18n/port_types", new Locale("en", "US"), new UTF8Control());

	@Override
	public String getCn() {
		return PORT_TYPE_CN_BUNDLE.getString(name());
	}

	@Override
	public String getEn() {
		return PORT_TYPE_EN_BUNDLE.getString(name());
	}
}
