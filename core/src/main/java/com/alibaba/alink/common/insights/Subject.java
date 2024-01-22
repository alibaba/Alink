package com.alibaba.alink.common.insights;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Subject implements Serializable {

	public List <Subspace> subspaces;
	public Breakdown breakdown;
	public List <Measure> measures;

	public Subject() {
		subspaces = new ArrayList <>();
		measures = new ArrayList <>();
	}

	public Subject addSubspace(Subspace subspace) {
		this.subspaces.add(subspace);
		return this;
	}

	public Subject setSubspaces(List <Subspace> subspaces) {
		this.subspaces = subspaces;
		return this;
	}

	public Subject setMeasures(List <Measure> measures) {
		this.measures = measures;
		return this;
	}

	public Subject setBreakdown(Breakdown breakdown) {
		this.breakdown = breakdown;
		return this;
	}

	public Subject addMeasure(Measure measure) {
		this.measures.add(measure);
		return this;
	}

	public List <Subspace> getSubspaces() {
		return subspaces;
	}

	public Breakdown getBreakdown() {
		return breakdown;
	}

	public List <Measure> getMeasures() {
		return measures;
	}
}
