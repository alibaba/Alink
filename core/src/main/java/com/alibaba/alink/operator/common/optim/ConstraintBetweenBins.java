package com.alibaba.alink.operator.common.optim;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * bins constraint of one feature. constraints only occur on the bins of one feature.
 */
public class ConstraintBetweenBins implements Serializable {
	private static final long serialVersionUID = -7770851276936359039L;
	public String name = "initial";
	public int dim = 0;

	//Upper bound of weight constraint of variable.
	//number type: integer double.
	//eg, 1, 1.0 then bin_1 < 1.0.
	@JsonProperty("UP")
	public List <Number[]> lessThan = new ArrayList <>();

	//lower bound of weight constraint for variable.
	//number type: integer double.
	//eg, 1, 1.0 then bin_1 > 1.0.
	@JsonProperty("LO")
	public List <Number[]> largerThan = new ArrayList <>();

	//The weight of the variable is equal to a fixed value.
	//number type: integer double.
	//eg, 1, 1.0 then bin_1 = 1.0.
	@JsonProperty("=")
	public List <Number[]> equal = new ArrayList <>();

	//The weights of variables are proportional to each other.
	//number type: integer integer double.
	//eg, 1,2,1.5 then bin_1/bin_2=1.5.
	@JsonProperty("%")
	public List <Number[]> scale = new ArrayList <>();

	//The weights of variables satisfy the ascending order constraint in order.
	//number type: integer, integer, integer ....
	//eg, 1,3,6,4 then bin_1 < bin_3 < bin_6 < bin_4.
	@JsonProperty("<")
	public List <Number[]> lessThanBin = new ArrayList <>();

	//The weights of variables satisfy the decending order constraint in order.
	//number type: integer, integer, integer ....
	//eg, 1,3,6,4 then bin_1 > bin_3 > bin_6 > bin_4.
	@JsonProperty(">")
	public List <Number[]> largerThanBin = new ArrayList <>();

	public ConstraintBetweenBins() {}

	/**
	 * @param name feature colName.
	 * @param dim  bins number of feature.
	 */
	public ConstraintBetweenBins(String name, int dim) {
		this.name = name;
		this.dim = dim;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setDim(int dim) {
		this.dim = dim;
	}

	//Upper bound of weight constraint of variable.
	//number type: integer double.
	//eg, 1, 1.0 then bin_1 < 1.0.
	public void addLessThan(Number[] item) {
		lessThan.add(item);
	}

	//lower bound of weight constraint for variable.
	//number type: integer double.
	//eg, 1, 1.0 then bin_1 > 1.0.
	public void addLargerThan(Number[] item) {
		largerThan.add(item);
	}

	//The weight of the variable is equal to a fixed value.
	//number type: integer double.
	//eg, 1, 1.0 then bin_1 = 1.0.
	public void addEqual(Number[] item) {
		equal.add(item);
	}

	//The weights of variables are proportional to each other.
	//number type: integer integer double.
	//eg, 1,2,1.5 then bin_1/bin_2=1.5.
	public void addScale(Number[] item) {
		scale.add(item);
	}

	//The weights of variables satisfy the ascending order constraint in order.
	//number type: integer, integer, integer ....
	//eg, 1,3,6,4 then bin_1 < bin_3 < bin_6 < bin_4.
	public void addLessThanBin(Number[] item) {
		lessThanBin.add(item);
	}

	//The weights of variables satisfy the decending order constraint in order.
	//number type: integer, integer, integer ....
	//eg, 1,3,6,4 then bin_1 > bin_3 > bin_6 > bin_4.
	public void addLargerThanBin(Number[] item) {
		largerThanBin.add(item);
	}

	public int getInequalSize() {
		return lessThan.size() + largerThan.size() + lessThanBin.size() + largerThanBin.size();
	}

	public int getEqualSize() {
		return equal.size() + scale.size();
	}

	public String toString() {
		return JsonConverter.toJson(this);
	}

	//Divide the continuous unequal（< or >) into pairwise.
	public static ConstraintBetweenBins fromJson(String constraintJson) {
		if (constraintJson == null || constraintJson.equals("")) {
			return new ConstraintBetweenBins();
		}
		ConstraintBetweenBins constraint = JsonConverter.fromJson(constraintJson, ConstraintBetweenBins.class);
		List <Number[]> lessThanBin = new ArrayList <>();
		for (Number[] item : constraint.lessThanBin) {
			int size = item.length;
			if (size == 2) {
				lessThanBin.add(item);
			} else {
				for (int i = 0; i < size - 1; i++) {
					lessThanBin.add(new Number[] {item[i], item[i + 1]});
				}
			}
		}

		List <Number[]> largerThanBin = new ArrayList <>();
		for (Number[] item : constraint.largerThanBin) {
			int size = item.length;
			if (size == 2) {
				largerThanBin.add(item);
			} else {
				for (int i = 0; i < size - 1; i++) {
					largerThanBin.add(new Number[] {item[i], item[i + 1]});
				}
			}
		}
		constraint.lessThanBin = lessThanBin;
		constraint.largerThanBin = largerThanBin;
		return constraint;
	}

	public Tuple4 <double[][], double[], double[][], double[]> getConstraints(int dim) {
		int inequalSize = lessThan.size() + largerThan.size() + lessThanBin.size() + largerThanBin.size();
		int equalSize = equal.size() + scale.size();
		double[][] equalityConstraint = new double[equalSize][dim];
		double[][] inequalityConstraint = new double[inequalSize][dim];
		double[] equalityItem = new double[equalSize];
		double[] inequalityItem = new double[inequalSize];
		//in sqp, it needs to write in the form of ≥.
		int index = 0;
		for (int i = 0; i < lessThan.size(); i++) {
			inequalityConstraint[index][(int) lessThan.get(i)[0]] = -1;
			inequalityItem[index] = (double) lessThan.get(i)[1] * -1;
			index++;
		}
		for (int i = 0; i < largerThan.size(); i++) {
			inequalityConstraint[index][(int) largerThan.get(i)[0]] = 1;
			inequalityItem[index] = (double) largerThan.get(i)[1];
			index++;
		}
		for (int i = 0; i < lessThanBin.size(); i++) {
			inequalityConstraint[index][(int) lessThanBin.get(i)[0]] = -1;
			inequalityConstraint[index][(int) lessThanBin.get(i)[1]] = 1;
			inequalityItem[index] = 0;
			index++;
		}
		for (int i = 0; i < largerThanBin.size(); i++) {
			inequalityConstraint[index][(int) largerThanBin.get(i)[0]] = 1;
			inequalityConstraint[index][(int) largerThanBin.get(i)[1]] = -1;
			inequalityItem[index] = 0;
			index++;
		}

		index = 0;
		for (int i = 0; i < equal.size(); i++) {
			equalityConstraint[index][(int) equal.get(i)[0]] = 1;
			equalityItem[index] = (double) equal.get(i)[1];
			index++;
		}
		for (int i = 0; i < scale.size(); i++) {
			equalityConstraint[index][(int) scale.get(i)[0]] = 1;
			equalityConstraint[index][(int) scale.get(i)[1]] = -1 * ((double) scale.get(i)[2]);
			equalityItem[index] = 0;
			index++;
		}
		return Tuple4.of(inequalityConstraint, inequalityItem, equalityConstraint, equalityItem);
	}

}
