package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperAdapter;
import com.alibaba.alink.common.mapper.MapperAdapterMT;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.shared.HasNumThreads;

import java.util.function.BiFunction;

/**
 * class for a flat map {@link BatchOperator}.
 *
 * @param <T> class type of the {@link MapBatchOp} implementation itself.
 */
public class MapBatchOp<T extends MapBatchOp <T>> extends BatchOperator <T> {

	private static final long serialVersionUID = 7422972179266979419L;
	private final BiFunction <TableSchema, Params, Mapper> mapperBuilder;

	public MapBatchOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		try {
			Mapper mapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());

			DataSet <Row> resultRows;

			final boolean isBatchPredictMultiThread = AlinkGlobalConfiguration.isBatchPredictMultiThread();

			if (!isBatchPredictMultiThread
				|| !getParams().contains(HasNumThreads.NUM_THREADS)
				|| getParams().get(HasNumThreads.NUM_THREADS) <= 1) {

				resultRows = in.getDataSet().map(new MapperAdapter(mapper));
			} else {
				resultRows = in.getDataSet().flatMap(
					new MapperAdapterMT(mapper, getParams().get(HasNumThreads.NUM_THREADS))
				);
			}

			this.setOutput(resultRows, mapper.getOutputSchema());

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}