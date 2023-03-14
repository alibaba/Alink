package com.alibaba.alink.operator.local.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
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
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.shared.HasNumThreads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

/**
 * class for a flat map {@link LocalOperator}.
 *
 * @param <T> class type of the {@link MapLocalOp} implementation itself.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithFirstInputSpec
@Internal
public class MapLocalOp<T extends MapLocalOp <T>> extends LocalOperator <T> {

	private static final Logger LOG = LoggerFactory.getLogger(MapLocalOp.class);

	protected final BiFunction <TableSchema, Params, Mapper> mapperBuilder;

	public MapLocalOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		try {
			Mapper mapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
			mapper.open();

			List <Row> output = execMapper(in, mapper, getParams());

			this.setOutputTable(new MTable(output, mapper.getOutputSchema()));
			mapper.close();
			return (T) this;
		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}
	}

	protected static List <Row> execMapper(final LocalOperator <?> in, final Mapper mapper, final Params params) {
		int numThreads = LocalOperator.getDefaultNumThreads();

		if (params.contains(HasNumThreads.NUM_THREADS)) {
			numThreads = params.get(HasNumThreads.NUM_THREADS);
		}

		final TaskRunner taskRunner = new TaskRunner();

		final List <Row> input = in.getOutputTable().getRows();
		final int numRows = input.size();
		final List <Row> output = Arrays.asList(new Row[numRows]);

		for (int i = 0; i < numThreads; ++i) {
			final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, numThreads, numRows);
			final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, numThreads, numRows);

			if (cnt <= 0) {continue;}

			taskRunner.submit(() -> {
					for (int j = start; j < start + cnt; j++) {
						try {
							output.set(j, mapper.map(input.get(j)));
						} catch (Exception e) {
							LOG.error("Execute mapper error.", e);
							throw new AkIllegalDataException("Map error on the data : " + output.get(j).toString());
						}
					}
				}
			);
		}

		taskRunner.join();

		return output;
	}
}