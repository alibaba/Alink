package com.alibaba.alink.operator.local.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.io.csv.GenericCsvInputFormat;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.IOTaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.shared.HasNumThreads;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The base class of all data sources.
 *
 * @param <T>
 */

@InputPorts()
@OutputPorts(values = {@PortSpec(PortType.ANY)})
public abstract class BaseSourceLocalOp<T extends BaseSourceLocalOp <T>> extends LocalOperator <T>
	implements HasNumThreads <T> {

	protected BaseSourceLocalOp(Params params) {
		super(params);
	}

	@Override
	public final T linkFrom(LocalOperator <?>... inputs) {
		throw new AkUnsupportedOperationException("Source operator does not support linkFrom()");
	}

	@Override
	public MTable getOutputTable() {
		if (isNullOutputTable()) {
			super.setOutputTable(initializeDataSource());
		}
		return super.getOutputTable();
	}

	/**
	 * Initialize the table.
	 */
	protected abstract MTable initializeDataSource();

	public static <T extends InputSplit> List <Row> createInput(InputFormat <Row, T> inputFormat, Params params) {
		if (inputFormat == null) {
			throw new IllegalArgumentException("InputFormat must not be null.");
		}
		return createInput(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat), params);
	}

	public static <T extends InputSplit> List <Row> createInput(InputFormat <Row, T> inputFormat,
																TypeInformation <Row> producedType,
																Params params) {
		if (inputFormat == null) {
			throw new IllegalArgumentException("InputFormat must not be null.");
		}

		if (producedType == null) {
			throw new IllegalArgumentException("Produced type information must not be null.");
		}

		int numThreads = LocalOperator.getDefaultNumThreads();

		if (params.contains(HasNumThreads.NUM_THREADS)) {
			numThreads = params.get(HasNumThreads.NUM_THREADS);
		}

		final int numFields = producedType.getTotalFields();

		final IOTaskRunner ioTaskRunner = new IOTaskRunner();
		final List <Row>[] buffer = new List[numThreads];
		T[] inputSplits;

		try {
			inputSplits = inputFormat.createInputSplits(numThreads);
		} catch (IOException e) {
			throw new AkIllegalDataException("Error in data input.", e);
		}

		final byte[] serialized = SerializationUtils.serialize(inputFormat);

		for (int i = 0; i < numThreads; ++i) {
			final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, numThreads, inputSplits.length);
			final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, numThreads, inputSplits.length);
			final int curThread = i;

			if (cnt <= 0) {continue;}

			ioTaskRunner.submit(() -> {

					ArrayList <Row> cache = new ArrayList <>();

					InputFormat <Row, T> localInputFormat = SerializationUtils.deserialize(serialized);

					for (int j = start; j < start + cnt; ++j) {
						try {

							if (localInputFormat instanceof RichInputFormat) {
								((RichInputFormat <Row, T>) localInputFormat).openInputFormat();
							}

							try {
								localInputFormat.open(inputSplits[j]);

								while (!localInputFormat.reachedEnd()) {
									Row returned;
									if ((returned = localInputFormat.nextRecord(new Row(numFields))) != null) {
										cache.add(returned);
									}
								}
							} catch (IOException e) {
								e.printStackTrace();
							} finally {
								try {
									localInputFormat.close();
								} catch (Exception ex) {
									ex.printStackTrace();
								}
							}
						} catch (IOException e) {
							e.printStackTrace();
						} finally {
							if (localInputFormat instanceof RichInputFormat) {
								try {
									((RichInputFormat <Row, T>) localInputFormat).closeInputFormat();
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
					}

					buffer[curThread] = cache;
				}
			);
		}

		ioTaskRunner.join();

		ArrayList <Row> output = new ArrayList <>();
		for (int i = 0; i < numThreads; ++i) {
			if (buffer[i] != null) {
				output.addAll(buffer[i]);
			}
		}
		return output;
	}

}
