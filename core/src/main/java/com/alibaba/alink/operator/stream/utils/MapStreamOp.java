package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithFirstInputSpec;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
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
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithFirstInputSpec
@Internal
public class MapStreamOp<T extends MapStreamOp <T>> extends StreamOperator <T> {

	private static final long serialVersionUID = -1335939787657447127L;
	protected final BiFunction <TableSchema, Params, Mapper> mapperBuilder;

	public MapStreamOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		try {
			Mapper mapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());

			DataStream <Row> resultRows = calcResultRows(in, mapper, getParams());

			this.setOutput(resultRows, mapper.getOutputSchema());

			return (T) this;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException(ex.getMessage(),ex);
		}
	}

	public static DataStream <Row> calcResultRows(StreamOperator <?> in, Mapper mapper, Params params) {
		if (params.get(MapperParams.NUM_THREADS) <= 1) {
			return in.getDataStream().map(new MapperAdapter(mapper));
		} else {
			return in.getDataStream().flatMap(
				new MapperAdapterMT(mapper, params.get(MapperParams.NUM_THREADS))
			);
		}
	}
}
