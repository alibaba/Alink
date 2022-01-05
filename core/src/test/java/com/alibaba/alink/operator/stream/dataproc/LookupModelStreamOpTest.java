package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;

public class LookupModelStreamOpTest extends AlinkTestBase {

	public static class FakeStreamOperator extends StreamOperator <FakeStreamOperator> {
		public FakeStreamOperator(Params params) {
			super(params);

			DataStream <Row> leftStream = MLEnvironmentFactory.get(getMLEnvironmentId())
				.getStreamExecutionEnvironment()
				.fromElements(1)
				.flatMap(new RichFlatMapFunction <Integer, Row>() {
					private long time = 0;
					@Override
					public void flatMap(Integer value, Collector <Row> out) throws Exception {
						while (true) {
							out.collect(Row.of("ppk1", "res1", "v1"));
							out.collect(Row.of("ppk2", "res1", "v1"));
							out.collect(Row.of("ppk3", "res1", "v1"));
							out.collect(Row.of("ppk4", "res1", "v1"));
							out.collect(Row.of("ppk5", "rsk555", "v1"));
							Thread.sleep(1000 * 1L);
							time += 1000L;
							if (time > 5000L) {
								break;
							}
						}
					}
				});

			this.setOutputTable(
				DataStreamConversionUtil.toTable(getMLEnvironmentId(), leftStream, new String[] {"a", "b", "c"},
					new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING}));
		}

		@Override
		public FakeStreamOperator linkFrom(StreamOperator <?>... inputs) {
			return this;
		}
	}

	@Test
	public void testLookUpModelStream() throws Exception {
		Row[] modelRows = new Row[] {
			Row.of(true, "ppk1", "res1", "rv1"),
			Row.of(true, "ppk9", "res1", "rv11"),
			Row.of(true, "ppk1", "res1", "rv111"),
			Row.of(true, "ppk2", "rsk2", "rv2"),
			Row.of(true, "ppk2", "rsk2", "rv22"),
			Row.of(true, "ppk2", "rsk222", "rv222"),
			Row.of(true, "ppk3", "rsk3", "rv3"),
			Row.of(true, "ppk5", "rsk5", "rv5"),
			Row.of(true, "ppk5", "rsk555", "rv555"),
		};

		Row[] streamRows = new Row[] {
			Row.of(new Timestamp(0L), 3L, true, "ppk1", "res1", "rv1"),
			Row.of(new Timestamp(0L), 3L, true, "ppk2", "res1", "rv11"),
			Row.of(new Timestamp(0L), 3L, true, "ppk4", "res1", "rv2222"),
			Row.of(new Timestamp(1L), 3L, true, "ppk4", "rsk2", "rv2"),
			Row.of(new Timestamp(1L), 3L, true, "ppk2", "rsk2", "rv22"),
			Row.of(new Timestamp(1L), 3L, true, "ppk2", "rsk222", "rv222"),
			Row.of(new Timestamp(2L), 3L, true, "ppk3", "rsk3", "rv3"),
			Row.of(new Timestamp(2L), 3L, true, "ppk5", "rsk5", "rv5"),
			Row.of(new Timestamp(2L), 3L, true, "ppk5", "rsk555", "rv555"),
		};

		BatchOperator <?> model = new MemSourceBatchOp(modelRows, new String[] {"_model_update_type_", "d", "e", "f"});
		StreamOperator <?> left = new MemSourceStreamOp(streamRows,
			new String[] {"alinkmodelstreamtimestamp", "alinkmodelstreamcount", "_model_update_type_", "d", "e", "f"});
		StreamOperator <?> data = new FakeStreamOperator(null);
		StreamOperator <?> out = new LookupStreamOp(model)
			.setSelectedCols(new String[] {"a", "b"})
			.setMapKeyCols(new String[] {"d", "e"})
			.setMapValueCols("f")
			.linkFrom(data, left);

		CollectSinkStreamOp sop = out.link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List <Row> res = sop.getAndRemoveValues();
		Assert.assertEquals(30, res.size());
	}
}