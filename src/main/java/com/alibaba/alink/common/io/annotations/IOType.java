package com.alibaba.alink.common.io.annotations;

/**
 * Type of source&&sink operator.
 */
public enum IOType {
	/**
	 * Batch source operator.
	 */
	SourceBatch,

	/**
	 * Batch sink operator.
	 */
	SinkBatch,

	/**
	 * Stream source operator.
	 */
	SourceStream,

	/**
	 * Stream sink operator.
	 */
	SinkStream,
}
