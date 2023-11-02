package com.alibaba.alink.operator.local.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithFirstInputSpec;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.shared.HasNumThreads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * class for a flat map {@link LocalOperator}.
 *
 * @param <T> class type of the {@link FlatMapLocalOp} implementation itself.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithFirstInputSpec
@Internal
public class FlatMapLocalOp<T extends FlatMapLocalOp <T>> extends LocalOperator <T> {

	private static final Logger LOG = LoggerFactory.getLogger(FlatMapLocalOp.class);

	protected final BiFunction <TableSchema, Params, FlatMapper> mapperBuilder;

	public FlatMapLocalOp(BiFunction <TableSchema, Params, FlatMapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		try {
			FlatMapper mapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
			mapper.open();

			List <Row> output = execFlatMapper(in, mapper, getParams());

			this.setOutputTable(new MTable(output, mapper.getOutputSchema()));
			mapper.close();
			return (T) this;
		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}
	}

	protected static List <Row> execFlatMapper(final LocalOperator <?> in, final FlatMapper mapper,
											   final Params params) {
		int numThreads = LocalOperator.getParallelism();

		if (params.contains(HasNumThreads.NUM_THREADS)) {
			numThreads *= params.get(HasNumThreads.NUM_THREADS);
		}

		final TaskRunner taskRunner = new TaskRunner();

		final List <Row>[] buffer = new List[numThreads];

		MTable mt = in.getOutputTable();
		final int numRows = mt.getNumRow();

		for (int i = 0; i < numThreads; ++i) {
			final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, numThreads, numRows);
			final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, numThreads, numRows);
			final int curThread = i;

			if (cnt <= 0) {continue;}

			taskRunner.submit(() -> {
					RowCollector rowCollector = new RowCollector();
					for (int j = start; j < start + cnt; j++) {
						try {
							mapper.flatMap(mt.getRow(j), rowCollector);
						} catch (Exception e) {
							LOG.error("Execute mapper error.", e);
							throw new AkIllegalDataException("FlatMap error on the data : " + mt.getRow(j).toString());
						}
					}
					buffer[curThread] = rowCollector.getRows();
				}
			);
		}

		taskRunner.join();

		ArrayList <Row> output = new ArrayList <>();
		for (int i = 0; i < numThreads; ++i) {
			if (buffer[i] != null) {
				output.addAll(buffer[i]);
			}
		}
		return output;
	}
}