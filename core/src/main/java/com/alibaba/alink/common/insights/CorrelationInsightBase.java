package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.LocalOperator;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public abstract class CorrelationInsightBase {
	protected static int MIN_SAMPLE_NUM = 10;
	public static final String MEASURE_NAME_PREFIX = "measure_";
	protected Insight insight;
	protected boolean needGroup = false;
	protected boolean needFilter = false;
	public static final double MIN_CORRELATION_THRESHOLD = 0.5;
	protected Number range = 0;
	public static final Number MAX_SCALAR_THRESHOLD = 10;

	private int threadNum = LocalOperator.getParallelism();

	public CorrelationInsightBase(Insight insight) {
		this.insight = insight;
		this.insight.layout = new LayoutData();
	}

	protected CorrelationInsightBase() {}

	public CorrelationInsightBase setNeedGroup(boolean needGroup) {
		this.needGroup = needGroup;
		return this;
	}

	public CorrelationInsightBase setNeedFilter(boolean needFilter) {
		this.needFilter = needFilter;
		return this;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}

	public List <LocalOperator <?>> groupData(LocalOperator <?> source, Subject subject) {
		return AggregationQuery.query(source, subject.breakdown, subject.measures, this.threadNum);
	}

	public MTable mergeData(List <Tuple3 <Number, Number, Object>> points, TableSchema schema, TableSchema schema2) {
		TableSchema outSchema = new TableSchema(new String[]{
			schema.getFieldName(0).get(),
			MEASURE_NAME_PREFIX + "0",
			MEASURE_NAME_PREFIX + "1"
		}, new TypeInformation[]{
			schema.getFieldType(0).get(),
			schema.getFieldType(1).get(),
			schema2.getFieldType(1).get()
		});

		List <Row> rows = new ArrayList <>();
		for (Tuple3 <Number, Number, Object> point : points) {
			Row newRow = new Row(3);
			newRow.setField(0, point.f2);
			newRow.setField(1, point.f0);
			newRow.setField(2, point.f1);
			rows.add(newRow);
		}
		return new MTable(rows, outSchema);
	}

	public HashMap <Object, Number> initData(LocalOperator <?> source) {
		HashMap <Object, Number> meaValues = new HashMap <>();
		for (Row row : source.getOutputTable().getRows()) {
			if (null == row.getField(0) || null == row.getField(1)) {
				continue;
			}
			Object breakdownValue = row.getField(0);
			Number meaValue = (Number) row.getField(1);
			meaValues.put(breakdownValue, meaValue);
		}
		return meaValues;
	}

	public abstract Insight processData(LocalOperator <?>... sources);

	public abstract void fillLayout();

	@Override
	public String toString() {
		return insight.toString();
	}

	/*
		https://www.microsoft.com/en-us/research/uploads/prod/2016/12/Insight-Types-Specification.pdf Significance of
		Correlation
	 */
	public double computeScore(LocalOperator <?>... sources) {
		//String[] columns = new String[] {insight.subject.breakdown.colName, MEASURE_NAME_PREFIX + "0"};
		HashMap <Object, Number> meaValues1 = initData(sources[0]);
		HashMap <Object, Number> meaValues2 = initData(sources[1]);
		List <Tuple3 <Number, Number, Object>> points = new ArrayList <>();
		for (Entry <Object, Number> entry : meaValues1.entrySet()) {
			if (!meaValues2.containsKey(entry.getKey())) {
				continue;
			}
			points.add(Tuple3.of(entry.getValue(), meaValues2.get(entry.getKey()), entry.getKey()));
		}
		if (points.size() < MIN_SAMPLE_NUM) {
			return 0;
		}
		double[] xArray = new double[points.size()];
		double[] yArray = new double[points.size()];
		double maxX = 0;
		double maxY = 0;
		for (int i = 0; i < points.size(); i++) {
			xArray[i] = points.get(i).f0.doubleValue();
			yArray[i] = points.get(i).f1.doubleValue();
			maxX = Math.max(maxX, Math.abs(xArray[i]));
			maxY = Math.max(maxY, Math.abs(yArray[i]));
		}
		if (maxX == 0 || maxY == 0) {
			return 0;
		}
		if (maxX > maxY) {
			range = Math.round(maxX / maxY);
		} else {
			range = Math.round(maxY / maxX);
		}

		//PearsonsCorrelation pc = new PearsonsCorrelation();
		SpearmansCorrelation sc = new SpearmansCorrelation();
		double score = Math.abs(sc.correlation(xArray, yArray));
		if (score >= MIN_CORRELATION_THRESHOLD) {
			MTable mtable = mergeData(points, sources[0].getSchema(), sources[1].getSchema());
			insight.layout.data = mtable;
		} else {
			score = 0;
		}
		return score;
	}
}
