package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.onlinelearning.SpeedControlParams;

import static java.lang.Thread.sleep;

@InputPorts(values = {@PortSpec(PortType.ANY)})
@OutputPorts()
@NameCn("流速控制")
public class SpeedControlStreamOp extends StreamOperator <SpeedControlStreamOp>
	implements SpeedControlParams <SpeedControlStreamOp> {

	private static final long serialVersionUID = 7943599253882942917L;

	public SpeedControlStreamOp() {
		super(new Params());
	}

	public SpeedControlStreamOp(Params param) {
		super(param);
	}

	@Override
	public SpeedControlStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		final double timeInterval = getTimeInterval();

		DataStream <Row> ret = in.getDataStream().map(new RichMapFunction <Row, Row>() {
			private static final long serialVersionUID = 8928343941684014599L;
			private long sleepTime;
			private long numSamplesPass;
			long cnt = 0;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
				if (timeInterval > 0.001) {
					sleepTime = parallelism * Double.valueOf(Math.ceil(timeInterval * 1000)).longValue();
					numSamplesPass = 1;
				} else {
					sleepTime = parallelism;
					numSamplesPass = Double.valueOf(Math.ceil(1 / (timeInterval * 1000))).longValue();
				}
			}

			@Override
			public Row map(Row value) throws Exception {
				if (cnt % numSamplesPass == 0) {
					sleep(sleepTime);
				}
				cnt++;
				return value;
			}
		});
		this.setOutput(ret, in.getSchema());
		return this;
	}
}
