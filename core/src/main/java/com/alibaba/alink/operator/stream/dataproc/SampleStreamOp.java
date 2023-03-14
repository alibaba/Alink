package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dataproc.SampleParams;

import java.util.Random;

/**
 * Sample with given ratio with or without replacement.
 */
@InputPorts(values = {@PortSpec(value = PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@SuppressWarnings("uncheck")
@NameCn("随机采样")
@NameEn("Sample")
public class SampleStreamOp extends StreamOperator <SampleStreamOp> implements SampleParams <SampleStreamOp> {

	private static final long serialVersionUID = 2165833879105000066L;

	public SampleStreamOp() {
		this(new Params());
	}

	public SampleStreamOp(double ratio) {
		this(new Params().set(RATIO, ratio));
	}

	public SampleStreamOp(Params params) {
		super(params);
	}

	@Override
	public SampleStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		final double ratio = getRatio();
		AkPreconditions.checkArgument(ratio >= 0. && ratio <= 1.,
			new AkIllegalArgumentException("The ratio should be in [0,1]"));

		DataStream <Row> rows = in.getDataStream()
			.flatMap(new RichFlatMapFunction <Row, Row>() {
				private static final long serialVersionUID = 2076455302934723779L;
				transient Random random;

				@Override
				public void open(Configuration parameters) throws Exception {
					random = new Random();
				}

				@Override
				public void flatMap(Row value, Collector <Row> out) throws Exception {
					if (random.nextDouble() <= ratio) {
						out.collect(value);
					}
				}
			});

		this.setOutput(rows, in.getSchema());
		return this;
	}
}
