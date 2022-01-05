package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.MTableSerializeStreamOp;
import com.alibaba.alink.operator.stream.utils.TensorSerializeStreamOp;
import com.alibaba.alink.operator.stream.utils.VectorSerializeStreamOp;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Internal
@IoOpAnnotation(name = "collect_stream_sink", ioType = IOType.SinkStream)
public final class CollectSinkStreamOp extends BaseSinkStreamOp <CollectSinkStreamOp> {

	private final int sessionId;

	public CollectSinkStreamOp() {
		this(new Params());
	}

	public CollectSinkStreamOp(Params parameter) {
		super(AnnotationUtils.annotatedName(CollectSinkStreamOp.class), parameter);
		sessionId = CollectSinkObjKeeper.getNewSessionId();
	}

	@Override
	public CollectSinkStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		return sinkFrom(in);
	}

	@Override
	public CollectSinkStreamOp sinkFrom(StreamOperator <?> in) {
		in.getDataStream()
			.addSink(new CollectSink(sessionId))
			.name("CollectSink");
		return this;
	}

	public List <Row> getAndRemoveValues() {
		return CollectSinkObjKeeper.getAndRemoveValues(sessionId);
	}

	private static class CollectSink extends RichSinkFunction <Row> {

		private final int sessionId;

		public CollectSink(int sessionId) {
			this.sessionId = sessionId;
		}

		@Override
		public void invoke(Row value, Context context) throws Exception {
			CollectSinkObjKeeper.add(sessionId, value);
		}
	}

	protected static class CollectSinkObjKeeper {
		//todo: if user not call the function 'getAndRemove', results exist in values and cause memory leak.
		public static ConcurrentHashMap <Integer, List <Row>> values = new ConcurrentHashMap <Integer, List <Row>>();
		private static int globalSessionId = 0;

		static int getNewSessionId() {
			return globalSessionId++;
		}

		static List <Row> getAndRemoveValues(int sessionId) {
			List <Row> results = values.remove(sessionId);
			return null == results ? new ArrayList <>() : results;
		}

		static void add(int sessionId, Row value) {
			values.compute(sessionId, (k, v) -> {
				v = null == v ? new ArrayList <>() : v;
				v.add(value);
				return v;
			});
		}

	}

}
