package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamCond;
import com.alibaba.alink.common.annotation.ParamCond.CondType;
import com.alibaba.alink.common.annotation.ParamMutexRule;
import com.alibaba.alink.common.annotation.ParamMutexRule.ActionType;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.viz.AlinkViz;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.utils.TimeUtil;
import com.alibaba.alink.common.viz.VizData;
import com.alibaba.alink.common.viz.VizDataWriterInterface;
import com.alibaba.alink.common.viz.VizOpChartData;
import com.alibaba.alink.common.viz.VizOpDataInfo;
import com.alibaba.alink.common.viz.VizOpMeta;
import com.alibaba.alink.operator.common.dataproc.Format;
import com.alibaba.alink.operator.common.dataproc.counter.AbstractCounter;
import com.alibaba.alink.operator.common.dataproc.counter.AdaptiveCounter;
import com.alibaba.alink.operator.common.dataproc.counter.HyperLogLogCounter;
import com.alibaba.alink.operator.common.dataproc.counter.HyperLogLogPlusCounter;
import com.alibaba.alink.operator.common.dataproc.counter.LinearCounter;
import com.alibaba.alink.operator.common.dataproc.counter.LogLogCounter;
import com.alibaba.alink.operator.common.dataproc.counter.PvCounter;
import com.alibaba.alink.operator.common.dataproc.counter.StochasticCounter;
import com.alibaba.alink.operator.common.dataproc.counter.UvCounter;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@ParamSelectColumnSpec(name = "selectedCol")
@ParamSelectColumnSpec(name = "keyCol")
@ParamMutexRule(
	name = "bit",
	type = ActionType.SHOW,
	cond = @ParamCond(
		name = "index",
		type = CondType.WHEN_IN_VALUES,
		values = {
			"CARDINALITY_ESTIMATE_STOCHASTIC",
			"CARDINALITY_ESTIMATE_LINEAR",
			"CARDINALITY_ESTIMATE_LOGLOG",
			"CARDINALITY_ESTIMATE_ADAPTIVE",
			"CARDINALITY_ESTIMATE_HYPERLOGLOG",
			"CARDINALITY_ESTIMATE_HYPERLOGLOGPLUS"
		}
	)
)
@ParamMutexRule(
	name = "format",
	type = ActionType.SHOW,
	cond = @ParamCond(
		name = "index",
		type = CondType.WHEN_IN_VALUES,
		values = {
			"CARDINALITY_ESTIMATE_HYPERLOGLOGPLUS"
		}
	)
)
@NameCn("网页流量指标生成")
@NameEn("Web Traffic Index")
public final class WebTrafficIndexStreamOp extends StreamOperator <WebTrafficIndexStreamOp>
	implements WebTrafficIndexParams <WebTrafficIndexStreamOp>, AlinkViz <WebTrafficIndexStreamOp> {

	private static final long serialVersionUID = 7658064770323906063L;

	List <StreamOperator <?>> inputList = new ArrayList <>();

	public WebTrafficIndexStreamOp() {
		super(null);
	}

	public WebTrafficIndexStreamOp(Params params) {
		super(params);
	}

	@Override
	public WebTrafficIndexStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		String selectedColName = this.getSelectedCol();
		double timeInterval = this.getTimeInterval();
		Index indexStr = this.getIndex();

		inputList.add(in);
		int selectedColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), selectedColName);

		DataStream <AbstractCounter> output = in.getDataStream().map(new Select(selectedColIdx))
			.windowAll(TumblingProcessingTimeWindows.of(TimeUtil.convertTime(timeInterval)))
			.apply(new GroupIndex(this.getParams()));

		AllStatistics allStatistics = new AllStatistics();
		DataStream <AbstractCounter> totalStatistics = output.map(allStatistics).setParallelism(1);

		DataStream <Row> windowOutput = output.map(new Output(this.getVizDataWriter(), "window"));
		DataStream <Row> allOutput = totalStatistics.map(new Output(this.getVizDataWriter(), "all"));

		DataStream <Row> union = windowOutput.union(allOutput);
		setOutput(union, new String[] {"functionName", indexStr.name()},
			new TypeInformation[] {Types.STRING, Types.LONG});

		//write meta
		VizOpMeta meta = new VizOpMeta();
		meta.dataInfos = new VizOpDataInfo[2];
		meta.dataInfos[0] = new VizOpDataInfo(0, indexStr.name());
		meta.dataInfos[1] = new VizOpDataInfo(1, indexStr.name());

		meta.cascades = new HashMap <>();
		meta.cascades.put(gson.toJson(new String[] {selectedColName, "WindowStat", indexStr.name()}),
			new VizOpChartData(0, indexStr.name()));
		meta.cascades.put(gson.toJson(new String[] {selectedColName, "AllStat", indexStr.name()}),
			new VizOpChartData(1, indexStr.name()));

		meta.setSchema(in.getSchema());
		meta.params = this.getParams();
		meta.isOutput = false;
		meta.opName = "WebTrafficIndexStreamOp";

		this.getVizDataWriter().writeStreamMeta(meta);
		return this;
	}

	public static class Output implements MapFunction <AbstractCounter, Row> {
		private static final long serialVersionUID = -4637838734238510208L;
		private VizDataWriterInterface node;
		private String functionName;

		public Output(VizDataWriterInterface node, String functionName) {
			this.node = node;
			this.functionName = functionName;
		}

		@Override
		public Row map(AbstractCounter counter) throws Exception {
			Row ret = new Row(2);
			long total = counter.count();
			long timeStamp = System.currentTimeMillis();
			ret.setField(0, functionName);
			ret.setField(1, total);

			//write for viz
			if (this.node != null) {
				List <VizData> vizData = new ArrayList <>();
				if ("window".equals(functionName)) {
					vizData.add(new VizData(0, String.valueOf(total), timeStamp));
				} else {
					vizData.add(new VizData(1, String.valueOf(total), timeStamp));
				}
				node.writeStreamData(vizData);
			}

			return ret;
		}
	}

	public static class Select implements MapFunction <Row, Object> {
		private static final long serialVersionUID = -7778339970540156093L;
		private int selectedIdx;

		public Select(int selectedIdx) {
			this.selectedIdx = selectedIdx;
		}

		@Override
		public Object map(Row value) throws Exception {
			return value.getField(selectedIdx);
		}
	}

	public static class GroupIndex implements AllWindowFunction <Object, AbstractCounter, TimeWindow> {
		private static final long serialVersionUID = 7509708371547389269L;
		private Index indexStr;
		private int bit;
		private Format format;

		public GroupIndex(Params params) {
			indexStr = params.get(WebTrafficIndexParams.INDEX);
			bit = params.get(WebTrafficIndexParams.BIT);
			format = params.get(WebTrafficIndexParams.FORMAT);
		}

		@Override
		public void apply(TimeWindow window, Iterable <Object> values, Collector <AbstractCounter> out)
			throws Exception {
			AbstractCounter counter;

			switch (indexStr) {
				case PV: {
					counter = new PvCounter();
					break;
				}
				case UV:
				case UIP: {
					counter = new UvCounter();
					break;
				}
				case CARDINALITY_ESTIMATE_STOCHASTIC: {
					counter = new StochasticCounter(bit);
					break;
				}
				case CARDINALITY_ESTIMATE_LINEAR: {
					counter = new LinearCounter(bit);
					break;
				}
				case CARDINALITY_ESTIMATE_LOGLOG: {
					counter = new LogLogCounter(bit);
					break;
				}
				case CARDINALITY_ESTIMATE_ADAPTIVE: {
					counter = new AdaptiveCounter(bit);
					break;
				}
				case CARDINALITY_ESTIMATE_HYPERLOGLOG: {
					counter = new HyperLogLogCounter(bit);
					break;
				}
				case CARDINALITY_ESTIMATE_HYPERLOGLOGPLUS: {
					switch (format) {
						case NORMAL: {
							counter = new HyperLogLogPlusCounter(bit);
							break;
						}
						case SPARSE: {
							counter = new HyperLogLogPlusCounter("sparse", bit, 25);
							break;
						}
						default: {
							throw new RuntimeException("Not support this type!");
						}
					}
					break;
				}
				default: {
					throw new RuntimeException("Unsupport index type. indexType: " + indexStr);
				}
			}
			for (Object val : values) {
				counter.visit(val);
			}
			out.collect(counter);
		}
	}

	private class AllStatistics implements MapFunction <AbstractCounter, AbstractCounter> {
		private static final long serialVersionUID = 9010637128573806249L;
		private AbstractCounter statistics;

		@Override
		public AbstractCounter map(AbstractCounter value) {
			try {
				if (null == this.statistics) {
					this.statistics = value;
				} else {
					this.statistics = this.statistics.merge(value);
				}

			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return this.statistics;
		}
	}
}
