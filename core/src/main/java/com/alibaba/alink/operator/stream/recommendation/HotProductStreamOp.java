package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.HopTimeWindowStreamOp;
import com.alibaba.alink.operator.stream.feature.OverTimeWindowStreamOp;
import com.alibaba.alink.operator.stream.feature.TumbleTimeWindowStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.params.recommendation.HotProductParams;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * hot product StreamOp
 */
@NameCn("热点推荐")
@NameEn("Hot product")
public class HotProductStreamOp extends StreamOperator <HotProductStreamOp>
	implements HotProductParams <HotProductStreamOp> {

	public HotProductStreamOp() {
		this(null);
	}

	public HotProductStreamOp(Params params) {
		super(params);
	}

	public static class FromUnixTimestamp extends ScalarFunction {

		public java.sql.Timestamp eval(Long ts) {
			return new java.sql.Timestamp(ts * 1000);
		}

	}

	@Override
	public HotProductStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);

		final String TIME_COL = getTimeCol();
		final String PRODUCT_COL = getProductCol();
		final String WINDOW_TIME = getWindowTime();
		final String HOP_TIME = getHopTime();
		final int TOP_N = getTopN();

		StreamOperator <?> tumble = in
			.select(PRODUCT_COL + ", " + TIME_COL + ", 1 AS cnt")
			.link(
				new TumbleTimeWindowStreamOp()
					.setWindowTime(HOP_TIME)
					.setTimeCol(TIME_COL)
					.setGroupCols(PRODUCT_COL)
					.setClause(
						String.format("TUMBLE_START() AS %s,TUMBLE_END() AS %s, SUM(cnt) AS cnt", TIME_COL, "timeEnd"))
			);

		StreamOperator <?> tumbleAdd1 = addTriggerData(tumble, TIME_COL, "cnt",
			OverTimeWindowStreamOp.getIntervalBySecond(getHopTime()));

		StreamOperator <?> hop = tumbleAdd1.link(
			new HopTimeWindowStreamOp()
				.setWindowTime(WINDOW_TIME)
				.setHopTime(HOP_TIME)
				.setTimeCol(TIME_COL)
				.setGroupCols(PRODUCT_COL)
				.setClause(String.format("HOP_START() AS %s, HOP_END() AS %s, SUM(cnt) AS cnt",
					"startTime", TIME_COL))
		).udf(TIME_COL, TIME_COL, new AdjustTimeStamp(OverTimeWindowStreamOp.getIntervalBySecond(HOP_TIME)));

		StreamOperator <?> hopAdd1 = addTriggerData(hop, TIME_COL, "cnt", OverTimeWindowStreamOp.getIntervalBySecond(getHopTime()));

		StreamOperator <?> tumble2 = hopAdd1.link(
			new TumbleTimeWindowStreamOp()
				.setWindowTime(HOP_TIME)
				.setTimeCol(TIME_COL)
				.setClause(String.format("TUMBLE_END() AS %s, MTABLE_AGG(%s, cnt) AS mt",
					TIME_COL, PRODUCT_COL))
		);

		Table resultTable = tumble2
			.udf("mt", "mt", new TopNHotList(TOP_N, "cnt"))
			.getOutputTable();

		this.setOutputTable(resultTable);
		return this;
	}

	private static StreamOperator <?> addTriggerData(StreamOperator <?> in, String timeCol, String valCol,
													 double hopTime) {
		final int tumbleTimeColIdx = TableUtil.findColIndex(in.getColNames(), timeCol);
		final int cntColIdx = TableUtil.findColIndex(in.getColNames(), valCol);

		return new TableSourceStreamOp(DataStreamConversionUtil.toTable(
			in.getMLEnvironmentId(),
			in.getDataStream()
				.flatMap(new FlatMapFunction <Row, Row>() {
					@Override
					public void flatMap(Row value, Collector <Row> out) throws Exception {
						Timestamp ts = (Timestamp) value.getField(tumbleTimeColIdx);
						out.collect(value);
						Row outRow = Row.copy(value);
						outRow.setField(tumbleTimeColIdx, new Timestamp(ts.getTime() + 1000 * (long) hopTime));
						outRow.setField(cntColIdx, 0);
						out.collect(outRow);
					}
				}),
			in.getSchema()));
	}

	public static class AdjustTimeStamp extends ScalarFunction {
		final long offset_time;

		public AdjustTimeStamp(double offset_time) {
			this.offset_time = (long) (1000 * offset_time);
		}

		public java.sql.Timestamp eval(java.sql.Timestamp ts) {
			return new Timestamp(ts.getTime() - this.offset_time);
		}
	}

	public static class TopNHotList extends ScalarFunction {
		final int n;
		final String countCol;

		public TopNHotList(int n, String countCol) {
			this.n = n;
			this.countCol = countCol;
		}

		public MTable eval(MTable mt) {
			mt.orderBy(new String[] {countCol}, new boolean[] {false});
			if (mt.getNumRow() <= n) {
				return mt;
			} else {
				ArrayList <Row> rows = new ArrayList <>();
				for (int i = 0; i < n; i++) {rows.add(mt.getRow(i));}
				return new MTable(rows, mt.getSchema());
			}
		}

	}

}
