package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithFirstInputSpec;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperAdapter;
import com.alibaba.alink.common.mapper.MapperAdapterMT;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.mapper.MapperParams;

import java.util.function.BiFunction;

/**
 * class for a flat map {@link BatchOperator}.
 *
 * @param <T> class type of the {@link MapBatchOp} implementation itself.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithFirstInputSpec
@Internal
public class MapBatchOp<T extends MapBatchOp <T>> extends BatchOperator <T> {

	private static final long serialVersionUID = 7422972179266979419L;
	protected final BiFunction <TableSchema, Params, Mapper> mapperBuilder;

	public MapBatchOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		try {
			Mapper mapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());

			DataSet <Row> resultRows = calcResultRows(in, mapper, getParams());

			this.setOutput(resultRows, mapper.getOutputSchema());

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static DataSet <Row> calcResultRows(BatchOperator <?> in, Mapper mapper, Params params) {
		if (params.get(MapperParams.NUM_THREADS) <= 1) {
			return in.getDataSet().map(new MapperAdapter(mapper));
		} else {
			return in.getDataSet().flatMap(
				new MapperAdapterMT(mapper, params.get(MapperParams.NUM_THREADS))
			);
		}
	}

}