package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dataproc.AppendIdStreamParams;
import org.apache.commons.lang3.ArrayUtils;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@NameCn("流式增加ID列")
@NameEn("Append ID")
public class AppendIdStreamOp extends StreamOperator <AppendIdStreamOp>
	implements AppendIdStreamParams <AppendIdStreamOp> {

	private static final long serialVersionUID = -6309808493226982591L;

	public AppendIdStreamOp() {
		this(new Params());
	}

	public AppendIdStreamOp(Params params) {
		super(params);
	}

	@Override
	public AppendIdStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		String idColName = getIdCol();

		DataStream <Row> input = in.getDataStream();
		DataStream <Row> output = input
			.map(new RichMapFunction <Row, Row>() {
				private static final long serialVersionUID = 8195432332635374903L;
				transient long currIdx;
				transient int numTasks;
				transient int taskId;
				transient Row reused;

				@Override
				public void open(Configuration parameters) throws Exception {
					this.numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
					this.taskId = getRuntimeContext().getIndexOfThisSubtask();
					this.currIdx = this.taskId;
				}

				@Override
				public Row map(Row value) throws Exception {
					if (reused == null || reused.getArity() != value.getArity() + 1) {
						reused = new Row(value.getArity() + 1);
					}
					for (int i = 0; i < value.getArity(); i++) {
						reused.setField(i, value.getField(i));
					}
					reused.setField(value.getArity(), currIdx);
					currIdx += numTasks;
					return reused;
				}
			})
			.name("append_id");

		setOutput(output, ArrayUtils.add(in.getColNames(), idColName),
			ArrayUtils.add(in.getColTypes(), Types.LONG));
		return this;
	}
}
