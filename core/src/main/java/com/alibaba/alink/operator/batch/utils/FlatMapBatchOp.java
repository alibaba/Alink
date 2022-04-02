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
import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.mapper.FlatMapperAdapter;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.function.BiFunction;

/**
 * class for a flat map {@link BatchOperator}.
 *
 * @param <T> class type of the {@link FlatMapBatchOp} implementation itself.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithFirstInputSpec
@Internal
public class FlatMapBatchOp<T extends FlatMapBatchOp <T>> extends BatchOperator <T> {

	private static final long serialVersionUID = 7763733109595053461L;
	private final BiFunction <TableSchema, Params, FlatMapper> mapperBuilder;

	public FlatMapBatchOp(BiFunction <TableSchema, Params, FlatMapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		try {
			FlatMapper flatmapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
			DataSet <Row> resultRows = in.getDataSet().flatMap(new FlatMapperAdapter(flatmapper));
			TableSchema resultSchema = flatmapper.getOutputSchema();
			this.setOutput(resultRows, resultSchema);
			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}
