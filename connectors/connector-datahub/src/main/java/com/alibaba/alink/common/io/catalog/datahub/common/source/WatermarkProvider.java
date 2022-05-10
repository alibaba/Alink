package com.alibaba.alink.common.io.catalog.datahub.common.source;

/**
 * WatermarkProvider.
 */
public interface WatermarkProvider {
	/**
	 * Gets current partition watermark.
	 * @return
	 */
	long getWatermark();
}
