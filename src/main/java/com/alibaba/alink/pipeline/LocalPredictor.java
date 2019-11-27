package com.alibaba.alink.pipeline;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * A LocalPredictor which is generated from {@link LocalPredictable}
 * predict an instance to one or more instances using map or flatMap accordingly.
 * <p>
 * The most important feature of LocalPredictor is that it can run at local and
 * thus we can deploy the predictor to another system.
 */
public class LocalPredictor {
	private ArrayList <Mapper> mappers = new ArrayList <>();

	public LocalPredictor(Mapper... mappers) {
		if (null == mappers || 0 == mappers.length) {
			throw new RuntimeException("The input mappers can not be empty.");
		}

		this.mappers.addAll(Arrays.asList(mappers));
	}

	public void merge(LocalPredictor otherPredictor) {
		this.mappers.addAll(otherPredictor.mappers);
	}

	public TableSchema getOutputSchema() {
		if (mappers.size() > 0) {
			return mappers.get(mappers.size() - 1).getOutputSchema();
		} else {
			return null;
		}
	}

	/**
	 * map operation method that maps a row to a new row.
	 *
	 * @param row the input Row type data
	 * @return one Row type data
	 * @throws Exception This method may throw exceptions. Throwing
	 * an exception will cause the operation to fail.
	 */
	public Row map(Row row) throws Exception {
		Row r = row;
		for (Mapper mapper : mappers) {
			r = mapper.map(r);
		}
		return r;
	}

}
