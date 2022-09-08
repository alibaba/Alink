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
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.mapper.FlatMapperAdapter;
import com.alibaba.alink.common.mapper.FlatMapperAdapterMT;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.mapper.MapperParams;

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
			FlatMapper flatMapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
			DataSet <Row> resultRows = calcResultRows(in, flatMapper, getParams());
			TableSchema resultSchema = flatMapper.getOutputSchema();
			this.setOutput(resultRows, resultSchema);
			//noinspection unchecked
			return (T) this;
		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}
	}

	public static DataSet <Row> calcResultRows(BatchOperator <?> in, FlatMapper flatMapper, Params params) {
		if (params.get(MapperParams.NUM_THREADS) <= 1) {
			return in.getDataSet().flatMap(new FlatMapperAdapter(flatMapper));
		} else {
			return in.getDataSet().flatMap(
				new FlatMapperAdapterMT(flatMapper, params.get(MapperParams.NUM_THREADS))
			);
		}
	}

}
