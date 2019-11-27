package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperAdapter;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.function.BiFunction;

/**
 * class for a flat map {@link BatchOperator}.
 *
 * @param <T> class type of the {@link MapBatchOp} implementation itself.
 */
public class MapBatchOp<T extends MapBatchOp<T>> extends BatchOperator<T> {

	private final BiFunction<TableSchema, Params, Mapper> mapperBuilder;

	public MapBatchOp(BiFunction<TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}


	@Override
	public T linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);

		try {
			Mapper mapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
			DataSet<Row> resultRows = in.getDataSet().map(new MapperAdapter(mapper));
			TableSchema resultSchema = mapper.getOutputSchema();
			this.setOutput(resultRows, resultSchema);
			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}