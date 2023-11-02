package com.alibaba.alink.common.insights;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.LocalOperator;

import java.util.HashMap;
import java.util.List;

public abstract class CorrelationInsightBase {

	public static final String MEASURE_NAME_PREFIX = "measure_";
	protected Insight insight;
	protected boolean needGroup = false;
	protected boolean needFilter = false;

	public CorrelationInsightBase(Subject subject, InsightType type) {
		insight = new Insight();
		insight.subject = subject;
		insight.type = type;
	}

	public CorrelationInsightBase setSubject(Subject subject) {
		insight.subject = subject;
		return this;
	}

	public CorrelationInsightBase setNeedGroup(boolean needGroup) {
		this.needGroup = needGroup;
		return this;
	}

	public CorrelationInsightBase setNeedFilter(boolean needFilter) {
		this.needFilter = needFilter;
		return this;
	}

	public List <LocalOperator <?>> groupData(LocalOperator <?> source, Subject subject) {
		return AggregationQuery.query(source, subject.breakdown, subject.measures);
	}

	public LocalOperator <?> filterData(LocalOperator <?> source, Subject subject) {
		if (subject.subspaces.size() > 0) {
			StringBuilder sbd = new StringBuilder();
			sbd.append(createFilterClause(subject.subspaces.get(0)));
			for (int i = 1; i < subject.subspaces.size(); i++) {
				sbd.append(" AND ").append(createFilterClause(subject.subspaces.get(i)));
			}
			source = source.filter(sbd.toString());
		}

		return source;
	}

	public String createFilterClause(Subspace subspace) {
		if (subspace.value instanceof String) {
			return subspace.colName + "='" + subspace.value + "'";
		} else {
			return subspace.colName + "=" + subspace.value;
		}
	}

	public HashMap <Object, Double> initData(LocalOperator <?> source) {
		HashMap<Object, Double> meaValues = new HashMap <>();
		for (Row row : source.getOutputTable().getRows()) {
			Object breakdownValue = row.getField(0);
			Double meaValue = ((Number) row.getField(1)).doubleValue();
			meaValues.put(breakdownValue, meaValue);
		}
		return meaValues;
	}

	public abstract Insight processData(LocalOperator <?>... sources);

	public abstract double computeScore(LocalOperator <?>... source);

	@Override
	public String toString() {
		return insight.toString();
	}
}
