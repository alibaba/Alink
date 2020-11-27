package com.alibaba.alink.common.mapper;

import org.apache.flink.types.Row;

/**
 * Interface for mappers with iterable model loader, which loads models from Iterable
 * without caching the whole list in memory.
 */
public interface IterableModelLoader {
	/**
	 * Load model from the iterable data.
	 *
	 * @param modelRows the iterable of Row type data
	 */
	void loadIterableModel(Iterable <Row> modelRows);
}
