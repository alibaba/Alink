package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperAdapter;
import com.alibaba.alink.common.mapper.MapperAdapterMT;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.mapper.MapperParams;

import java.util.function.BiFunction;

/**
 * class for a flat map {@link StreamOperator}.
 *
 * @param <T> class type of the {@link MapStreamOp} implementation itself.
 */
public class MapStreamOp<T extends MapStreamOp <T>> extends StreamOperator <T> {

	private static final long serialVersionUID = -1335939787657447127L;
	private final BiFunction <TableSchema, Params, Mapper> mapperBuilder;

	public MapStreamOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		try {
			Mapper mapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());

			DataStream <Row> resultRows;

			if (getParams().get(MapperParams.NUM_THREADS) <= 1) {
				resultRows = in.getDataStream().map(new MapperAdapter(mapper));
			} else {
				resultRows = in.getDataStream().flatMap(
					new MapperAdapterMT(mapper, getParams().get(MapperParams.NUM_THREADS))
				);
			}

			this.setOutput(resultRows, mapper.getOutputSchema());

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
