package com.alibaba.alink.common.annotation;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Description key of the port. An enum value is used as a key, the actual descriptions are put in the resource bundle.
 * <p>
 * When adding a new key or changing an existing key, update the resource bundle files accordingly
 * (src/main/resources/i18n/port_desc.properties, src/main/resources/i18n/port_desc_zh_CN.properties).
 */
public enum PortDesc implements Internationalizable {
	EMPTY,
	OUTPUT_RESULT,
	OUTLIER_DETECTION_RESULT,
	DL_BC_DATA,
	MODEL_INFO,
	MODEL_WEIGHT,
	USER_FACTOR,
	ITEM_FACTOR,
	APPEND_USER_FACTOR,
	APPEND_ITEM_FACTOR,
	FEATURE_IMPORTANCE,
	PREDICT_INPUT_DATA,
	PREDICT_INPUT_MODEL,
	PREDICT_INPUT_MODEL_STREAM,
	CORPUS,
	NODE_TYPE_MAPPING,
	INIT_MODEL,
	GRAPH,
	SAMPLED_DATA,
	CROSSED_FEATURES,
	KMEANS_MODEL,
	PREDICT_DATA,
	PREDICT_RESULT,
	DBSCAN_MODEL,
	NODE_TYPE,
	ASSOCIATION_PATTERNS,
	ASSOCIATION_RULES,
	GRPAH_EDGES,
	GRAPH_VERTICES;

	public static final ResourceBundle PORT_DESC_CN_BUNDLE = ResourceBundle.getBundle(
		"i18n/port_desc", new Locale("zh", "CN"), new UTF8Control());
	public static final ResourceBundle PORT_DESC_EN_BUNDLE = ResourceBundle.getBundle(
		"i18n/port_desc", new Locale("en", "US"), new UTF8Control());

	@Override
	public String getCn() {
		return PORT_DESC_CN_BUNDLE.getString(name());
	}

	@Override
	public String getEn() {
		return PORT_DESC_EN_BUNDLE.getString(name());
	}
}
