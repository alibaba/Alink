package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.MTableSerializeStreamOp;
import com.alibaba.alink.operator.stream.utils.TensorSerializeStreamOp;
import com.alibaba.alink.operator.stream.utils.VectorSerializeStreamOp;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasIoType;

/**
 * Base class for all sinks.
 *
 * @param <T>
 */
@InputPorts(values = {@PortSpec(PortType.ANY)})
@OutputPorts()
@NameCn("")
public abstract class BaseSinkStreamOp<T extends BaseSinkStreamOp <T>> extends StreamOperator <T> {

	static final IOType IO_TYPE = IOType.SinkStream;
	private static final long serialVersionUID = -7359864593021986768L;

	protected BaseSinkStreamOp(String nameSrcSnk, Params params) {
		super(params);
		getParams().set(HasIoType.IO_TYPE, IO_TYPE)
			.set(HasIoName.IO_NAME, nameSrcSnk);
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		return sinkFrom(in
			.link(new VectorSerializeStreamOp().setMLEnvironmentId(getMLEnvironmentId()))
			.link(new MTableSerializeStreamOp().setMLEnvironmentId(getMLEnvironmentId()))
			.link(new TensorSerializeStreamOp().setMLEnvironmentId(getMLEnvironmentId()))
		);
	}

	protected abstract T sinkFrom(StreamOperator <?> in);

	@Override
	public final Table getOutputTable() {
		throw new AkIllegalOperationException("Sink Operator has no output data.");
	}

	@Override
	public final StreamOperator <?> getSideOutput(int idx) {
		throw new AkIllegalOperationException("Sink Operator has no side-output data.");
	}

	@Override
	public final int getSideOutputCount() {
		return 0;
	}

	/**
	 * Converts the given Table into a DataStream<Row> of add and retract messages.
	 *
	 * @param table the Table to convert.
	 * @return the converted DataStream.
	 */
	public DataStream <Row> toRetractStream(Table table) {
		return MLEnvironmentFactory
			.get(getMLEnvironmentId())
			.getStreamTableEnvironment()
			.toRetractStream(table, Row.class)
			.flatMap(new FlatMapFunction <Tuple2 <Boolean, Row>, Row>() {
				private static final long serialVersionUID = -335704052194502150L;

				@Override
				public void flatMap(Tuple2 <Boolean, Row> tuple2, Collector <Row> collector) {
					if (tuple2.f0) {
						collector.collect(tuple2.f1);
					}
				}
			});
	}

	/**
	 * Judge whether the table only have insert (append) changes.
	 *
	 * @param table the Table to convert.
	 * @return if the Table only have insert (append) changes, return true. If the Table also modifies by update or
	 * delete changes, returns false.
	 */
	public Boolean isAppendStream(Table table) {
		try {
			MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamTableEnvironment()
				.toAppendStream(table, Row.class);
			return true;
		} catch (TableException ex) {
			return false;
		}
	}
}
