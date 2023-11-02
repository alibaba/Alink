package com.alibaba.alink.operator.common.optim;

import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureConstraint implements Serializable {

	private static final long serialVersionUID = -8834177894594296080L;
	// bins constraint of one feature.
	private List <ConstraintBetweenBins> featureConstraint = new ArrayList <>();
	//the constraints between features. If no binConstraint, it can also support.
	private ConstraintBetweenFeatures constraintBetweenFeatures;
	//countZero is the number of each feature in the dataset.
	private DenseVector countZero = null;
	private List <Integer> elseNullSave = null;

	public void setCountZero(DenseVector countZero) {
		if (countZero != null) {
			this.countZero = countZero;
		}
	}

	public boolean fromScorecard() {
		return null != elseNullSave;
	}

	public FeatureConstraint() {
	}

	//if the length won't be changed, then replace list as array.
	public void addBinConstraint(ConstraintBetweenBins... featureConstraint) {
		if (null == this.featureConstraint) {
			this.featureConstraint = new ArrayList <>(Arrays.asList(featureConstraint));
		} else {
			this.featureConstraint.addAll(Arrays.asList(featureConstraint));
		}

	}

	public HashMap <String, Integer> getDim() {
		int size = getBinConstraintSize();
		HashMap <String, Integer> res = new HashMap <>(size);
		for (int i = 0; i < size; i++) {
			ConstraintBetweenBins cons = this.featureConstraint.get(i);
			res.put(cons.name, i);
		}
		return res;
	}

	public void addConstraintBetweenFeature(ConstraintBetweenFeatures constraint) {
		constraintBetweenFeatures = constraint;
	}

	public int getBinConstraintSize() {
		return featureConstraint.size();
	}

	public String toString() {
		return JsonConverter.toJson(this);
	}

	public static FeatureConstraint fromJson(String constraintJson) {
		if (constraintJson == null || constraintJson.equals("")) {
			return new FeatureConstraint();
		}
		FeatureConstraint constraints = JsonConverter.fromJson(constraintJson, FeatureConstraint.class);
		return constraints;
	}

	//here the bin constraint must not be null.
	//<featureIndex(featureName, featureStartBinIndex), inequalSize, equalSize, dim(all bin size)>
	public Tuple4 <HashMap <String, Integer>, Integer, Integer, Integer> getParamsWithBinAndFeature() {
		int size = featureConstraint.size();
		int inequalSize = 0;
		int equalSize = 0;
		int dim = 0;
		HashMap <String, Integer> featureIndex = new HashMap <>(size);
		for (int i = 0; i < size; i++) {
			if (i != 0) {
				dim += featureConstraint.get(i - 1).dim;
			}
			featureIndex.put(featureConstraint.get(i).name, dim);
			int tempInequal = featureConstraint.get(i).getInequalSize();
			int tempEqual = featureConstraint.get(i).getEqualSize();
			if (tempEqual + tempInequal > 0) {
				//add default constraint
				equalSize += 1;
			}
			inequalSize += tempInequal;
			equalSize += tempEqual;
		}
		dim += featureConstraint.get(size - 1).dim;
		return Tuple4.of(featureIndex, inequalSize, equalSize, dim);
	}

	//for feature with no bins. It's suitable for table. Judge table or vector in the algo.
	public Tuple4 <double[][], double[], double[][], double[]> getConstraintsForFeatures(
		HashMap <String, Integer> featureIndex) {
		int dim = featureIndex.size();
		int inequalSize = 0;
		int equalSize = 0;
		if (constraintBetweenFeatures != null) {
			inequalSize = constraintBetweenFeatures.getInequalSize();
			equalSize = constraintBetweenFeatures.getEqualSize();
		}
		double[][] equalityConstraint = new double[equalSize][dim];
		double[][] inequalityConstraint = new double[inequalSize][dim];
		double[] equalityItem = new double[equalSize];
		double[] inequalityItem = new double[inequalSize];
		int equalStartIndex = 0;
		int inequalStartIndex = 0;
		addConstraintBetweenBinAndFeatures(constraintBetweenFeatures, featureIndex, equalStartIndex, inequalStartIndex,
			equalityConstraint, inequalityConstraint, equalityItem, inequalityItem);
		return Tuple4.of(inequalityConstraint, inequalityItem, equalityConstraint, equalityItem);
	}

	//for feature with no bins. It's suitable for vector. Judge table or vector in the algo.
	public Tuple4 <double[][], double[], double[][], double[]> getConstraintsForFeatures(int dim) {
		int inequalSize = 0;
		int equalSize = 0;
		if (constraintBetweenFeatures != null) {
			inequalSize = constraintBetweenFeatures.getInequalSize();
			equalSize = constraintBetweenFeatures.getEqualSize();
		}
		double[][] equalityConstraint = new double[equalSize][dim];
		double[][] inequalityConstraint = new double[inequalSize][dim];
		double[] equalityItem = new double[equalSize];
		double[] inequalityItem = new double[inequalSize];
		int equalStartIndex = 0;
		int inequalStartIndex = 0;
		int binStartIndex = 0;
		addConstraintBetweenFeatures(constraintBetweenFeatures, equalStartIndex, inequalStartIndex, binStartIndex,
			equalityConstraint, inequalityConstraint, equalityItem, inequalityItem);
		return Tuple4.of(inequalityConstraint, inequalityItem, equalityConstraint, equalityItem);
	}

	//this is for bins in one feature.
	//this is only for bin!! it add the default constraint.
	public Tuple4 <double[][], double[], double[][], double[]> getConstraintsForFeatureWithBin() {
		//<featureIndex(featureName, consStartIndex), inequalSize, equalSize, dim>
		Tuple4 <HashMap <String, Integer>, Integer, Integer, Integer> binFeatureParams = getParamsWithBinAndFeature();
		HashMap <String, Integer> featureIndex = binFeatureParams.f0;
		int inequalSize = binFeatureParams.f1;
		int equalSize = binFeatureParams.f2;
		int dim = binFeatureParams.f3;
		if (constraintBetweenFeatures != null) {
			inequalSize += constraintBetweenFeatures.getInequalSize();
			equalSize += constraintBetweenFeatures.getEqualSize();
		}
		double[][] equalityConstraint = new double[equalSize][dim];
		double[][] inequalityConstraint = new double[inequalSize][dim];
		double[] equalityItem = new double[equalSize];
		double[] inequalityItem = new double[inequalSize];
		int equalStartIndex = 0;
		int inequalStartIndex = 0;
		int binStartIndex = 0;
		int elseNullIndex = 0;
		for (ConstraintBetweenBins constraintBetweenBins : featureConstraint) {
			boolean hasConstraint = addConstraintBetweenBins(constraintBetweenBins,
				equalStartIndex, inequalStartIndex, binStartIndex,
				equalityConstraint, inequalityConstraint, equalityItem, inequalityItem);
			equalStartIndex += constraintBetweenBins.getEqualSize();
			inequalStartIndex += constraintBetweenBins.getInequalSize();
			//here add default constraint, consider the constraint on null and else
			//the index of null is -1, and if have else, the index is -2.
			//if there is constraint on null or else, default constraint should include them,
			// even the constraint is zero. but in fact, if the constraint is zero, the default
			//if there is not constraint on null or else, and there are not samples under null or else, default
			// constraint
			//should include them.
			//else do not add default constraint.
			if (hasConstraint) {
				//if it's null, it is not from scorecard.
				if (this.elseNullSave == null) {
					addDefaultBinConstraint(constraintBetweenBins, equalStartIndex, binStartIndex,
						equalityConstraint, equalityItem, null, null);
				} else {
					addDefaultBinConstraint(constraintBetweenBins, equalStartIndex, binStartIndex,
						equalityConstraint, equalityItem, this.elseNullSave.get(elseNullIndex), countZero);
				}
				equalStartIndex += 1;
			}
			binStartIndex += constraintBetweenBins.dim;
			elseNullIndex++;
		}
		addConstraintBetweenBinAndFeatures(constraintBetweenFeatures, featureIndex, equalStartIndex, inequalStartIndex,
			equalityConstraint, inequalityConstraint, equalityItem, inequalityItem);
		return Tuple4.of(inequalityConstraint, inequalityItem, equalityConstraint, equalityItem);
	}

	private static void addDefaultBinConstraint(ConstraintBetweenBins constraint, int equalStartIndex,
												int binStartIndex,
												double[][] equalityConstraint, double[] equalityItem,
												Integer elseNullIndex, DenseVector countZero) {
		int dim = constraint.dim;
		for (int i = binStartIndex; i < binStartIndex + dim; i++) {
			equalityConstraint[equalStartIndex][i] = 1;
		}
		//if no data on else/null, no default constraint, even if have constraint;
		//if have data on else/null, add default constraint.
		if (null == elseNullIndex) {
		} else if (elseNullIndex == 1) {
			if (countZero.get(binStartIndex + dim - 1) == 0) {
				equalityConstraint[equalStartIndex][binStartIndex + dim - 1] = 0;
			}
		} else if (elseNullIndex == 2) {
			if (countZero.get(binStartIndex + dim - 1) == 0) {
				equalityConstraint[equalStartIndex][binStartIndex + dim - 1] = 0;
			}
			equalityConstraint[equalStartIndex][binStartIndex + dim - 1] = 0;
		} else if (elseNullIndex == 0) {
			if (countZero.get(binStartIndex + dim - 1) == 0) {
				equalityConstraint[equalStartIndex][binStartIndex + dim - 1] = 0;
			}
			if (countZero.get(binStartIndex + dim - 2) == 0) {
				equalityConstraint[equalStartIndex][binStartIndex + dim - 2] = 0;
			}
		}
		equalityItem[equalStartIndex] = 0;
	}

	//only for constraints between bins in one feature.
	private boolean addConstraintBetweenBins(ConstraintBetweenBins constraint,
											 int equalStartIndex, int inequalStartIndex, int binStartIndex,
											 double[][] equalityConstraint, double[][] inequalityConstraint,
											 double[] equalityItem, double[] inequalityItem) {
		if (constraint == null) {
			return false;
		}
		boolean hasConstraint = false;
		for (int i = 0; i < constraint.lessThan.size(); i++) {
			inequalityConstraint[inequalStartIndex][(int) constraint.lessThan.get(i)[0] + binStartIndex] = -1;
			inequalityItem[inequalStartIndex] = constraint.lessThan.get(i)[1].doubleValue() * -1;
			inequalStartIndex++;
			hasConstraint = true;
		}
		for (int i = 0; i < constraint.largerThan.size(); i++) {
			inequalityConstraint[inequalStartIndex][(int) constraint.largerThan.get(i)[0] + binStartIndex] = 1;
			inequalityItem[inequalStartIndex] = constraint.largerThan.get(i)[1].doubleValue();
			inequalStartIndex++;
			hasConstraint = true;
		}
		for (int i = 0; i < constraint.lessThanBin.size(); i++) {
			inequalityConstraint[inequalStartIndex][(int) constraint.lessThanBin.get(i)[0] + binStartIndex] = -1;
			inequalityConstraint[inequalStartIndex][(int) constraint.lessThanBin.get(i)[1] + binStartIndex] = 1;
			inequalityItem[inequalStartIndex] = 0;
			inequalStartIndex++;
			hasConstraint = true;
		}
		for (int i = 0; i < constraint.largerThanBin.size(); i++) {
			inequalityConstraint[inequalStartIndex][(int) constraint.largerThanBin.get(i)[0] + binStartIndex] = 1;
			inequalityConstraint[inequalStartIndex][(int) constraint.largerThanBin.get(i)[1] + binStartIndex] = -1;
			inequalityItem[inequalStartIndex] = 0;
			inequalStartIndex++;
			hasConstraint = true;
		}

		for (int i = 0; i < constraint.equal.size(); i++) {
			equalityConstraint[equalStartIndex][(int) constraint.equal.get(i)[0] + binStartIndex] = 1;
			equalityItem[equalStartIndex] = constraint.equal.get(i)[1].doubleValue();
			equalStartIndex++;
			hasConstraint = true;
		}
		for (int i = 0; i < constraint.scale.size(); i++) {
			equalityConstraint[equalStartIndex][(int) constraint.scale.get(i)[0] + binStartIndex] = 1;
			equalityConstraint[equalStartIndex][(int) constraint.scale.get(i)[1] + binStartIndex] =
				-1 * constraint.scale.get(i)[2].doubleValue();
			equalityItem[equalStartIndex] = 0;
			equalStartIndex++;
			hasConstraint = true;
		}
		return hasConstraint;
	}

	//this is for features in the form of vector.
	private static void addConstraintBetweenFeatures(ConstraintBetweenFeatures constraint,
													 int equalStartIndex, int inequalStartIndex, int binStartIndex,
													 double[][] equalityConstraint, double[][] inequalityConstraint,
													 double[] equalityItem, double[] inequalityItem) {
		if (constraint == null) {
			return;
		}
		int index = inequalStartIndex;
		for (int i = 0; i < constraint.lessThan.size(); i++) {
			inequalityConstraint[index][(int) constraint.lessThan.get(i)[0] + binStartIndex] = -1;
			inequalityItem[index] = ((Number) constraint.lessThan.get(i)[1]).doubleValue() * -1;
			index++;
		}
		for (int i = 0; i < constraint.largerThan.size(); i++) {
			inequalityConstraint[index][(int) constraint.largerThan.get(i)[0] + binStartIndex] = 1;
			inequalityItem[index] = ((Number) constraint.largerThan.get(i)[1]).doubleValue();
			index++;
		}
		for (int i = 0; i < constraint.lessThanFeature.size(); i++) {
			inequalityConstraint[index][(int) constraint.lessThanFeature.get(i)[0] + binStartIndex] = -1;
			inequalityConstraint[index][(int) constraint.lessThanFeature.get(i)[1] + binStartIndex] = 1;
			inequalityItem[index] = 0;
			index++;
		}
		for (int i = 0; i < constraint.largerThanFeature.size(); i++) {
			inequalityConstraint[index][(int) constraint.largerThanFeature.get(i)[0] + binStartIndex] = 1;
			inequalityConstraint[index][(int) constraint.largerThanFeature.get(i)[1] + binStartIndex] = -1;
			inequalityItem[index] = 0;
			index++;
		}

		index = equalStartIndex;
		for (int i = 0; i < constraint.equal.size(); i++) {
			equalityConstraint[index][(int) constraint.equal.get(i)[0] + binStartIndex] = 1;
			equalityItem[index] = ((Number) constraint.equal.get(i)[1]).doubleValue();
			index++;
		}
		for (int i = 0; i < constraint.scale.size(); i++) {
			equalityConstraint[index][(int) constraint.scale.get(i)[0] + binStartIndex] = 1;
			equalityConstraint[index][(int) constraint.scale.get(i)[1] + binStartIndex] =
				-1 * ((Number) constraint.scale.get(i)[2]).doubleValue();
			equalityItem[index] = 0;
			index++;
		}
	}

	//for features in the form of table.
	private static void addConstraintBetweenBinAndFeatures(ConstraintBetweenFeatures constraint,
														   HashMap <String, Integer> featureIndex,
														   int equalStartIndex, int inequalStartIndex,
														   double[][] equalityConstraint,
														   double[][] inequalityConstraint,
														   double[] equalityItem, double[] inequalityItem) {
		if (constraint == null) {
			return;
		}
		int index = inequalStartIndex;
		for (int i = 0; i < constraint.lessThan.size(); i++) {
			int first = featureIndex.get(constraint.lessThan.get(i)[0]) + (int) constraint.lessThan.get(i)[1];
			inequalityConstraint[index][first] = 1;
			inequalityItem[index] = ((Number) constraint.lessThan.get(i)[2]).doubleValue();
			index++;
		}
		for (int i = 0; i < constraint.largerThan.size(); i++) {
			int first = featureIndex.get(constraint.largerThan.get(i)[0]) + (int) constraint.largerThan.get(i)[1];
			inequalityConstraint[index][first] = 1;
			inequalityItem[index] = ((Number) constraint.largerThan.get(i)[2]).doubleValue();
			index++;
		}
		for (int i = 0; i < constraint.lessThanFeature.size(); i++) {
			int first = featureIndex.get(constraint.lessThanFeature.get(i)[0]) + (int) constraint.lessThanFeature.get(
				i)[1];
			int second = featureIndex.get(constraint.lessThanFeature.get(i)[2]) + (int) constraint.lessThanFeature.get(
				i)[3];
			inequalityConstraint[index][first] = -1;
			inequalityConstraint[index][second] = 1;
			inequalityItem[index] = 0;
			index++;
		}
		for (int i = 0; i < constraint.largerThanFeature.size(); i++) {
			int first = featureIndex.get(constraint.largerThanFeature.get(i)[0]) + (int) constraint.largerThanFeature
				.get(i)[1];
			int second = featureIndex.get(constraint.largerThanFeature.get(i)[2]) + (int) constraint.largerThanFeature
				.get(i)[3];
			inequalityConstraint[index][first] = 1;
			inequalityConstraint[index][second] = -1;
			inequalityItem[index] = 0;
			index++;
		}

		index = equalStartIndex;
		for (int i = 0; i < constraint.equal.size(); i++) {
			int first = featureIndex.get(constraint.equal.get(i)[0]) + (int) constraint.equal.get(i)[1];
			equalityConstraint[index][first] = 1;
			equalityItem[index] = ((Number) constraint.equal.get(i)[2]).doubleValue();
			index++;
		}
		for (int i = 0; i < constraint.scale.size(); i++) {
			int first = featureIndex.get(constraint.scale.get(i)[0]) + (int) constraint.scale.get(i)[1];
			int second = featureIndex.get(constraint.scale.get(i)[2]) + (int) constraint.scale.get(i)[3];
			equalityConstraint[index][first] = 1;
			equalityConstraint[index][second] =
				-1 * ((Number) constraint.scale.get(i)[4]).doubleValue();
			equalityItem[index] = 0;
			index++;
		}
	}

	//ano: bin, all is here.
	public void addDim(FeatureConstraint anotherConstraint) {
		HashMap <String, Integer> featureDim = this.getDim();
		int length = anotherConstraint.featureConstraint.size();
		//ConstraintBetweenBins constraintBetweenBins : anotherConstraint.featureConstraint
		for (int i = 0; i < length; ++i) {
			ConstraintBetweenBins constraintBetweenBins = anotherConstraint.featureConstraint.get(i);
			if (featureDim.containsKey(constraintBetweenBins.name)) {
				int dim = constraintBetweenBins.dim;
				constraintBetweenBins = this.featureConstraint.get(featureDim.get(constraintBetweenBins.name));
				constraintBetweenBins.dim = dim;
				anotherConstraint.featureConstraint.set(i, constraintBetweenBins);
			}
		}
		this.featureConstraint = anotherConstraint.featureConstraint;
	}

	public void modify(Map <String, Boolean> hasElse) {
		this.elseNullSave = new ArrayList <>(this.featureConstraint.size());
		for (ConstraintBetweenBins constraint : this.featureConstraint) {
			boolean withElse = false;
			boolean withNull = false;
			HashMap <Integer, Integer> replace = new HashMap <>(2);
			boolean hasName = hasElse.get(constraint.name);
			int dim = constraint.dim;
			if (hasName) {
				replace.put(-1, dim - 2);
				replace.put(-2, dim - 1);
			} else {
				replace.put(-1, dim - 1);
			}
			for (Number[] equal : constraint.equal) {
				int number = (int) equal[0];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					equal[0] = replace.get(number);
				}
			}
			for (Number[] scale : constraint.scale) {
				int number = (int) scale[0];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					scale[0] = replace.get(number);
				}
				number = (int) scale[1];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					scale[1] = replace.get(number);
				}
			}
			for (Number[] lessThanBin : constraint.lessThanBin) {
				int number = (int) lessThanBin[0];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					lessThanBin[0] = replace.get(number);
				}
				number = (int) lessThanBin[1];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					lessThanBin[1] = replace.get(number);
				}
			}
			for (Number[] largerThanBin : constraint.largerThanBin) {
				int number = (int) largerThanBin[0];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					largerThanBin[0] = replace.get(number);
				}
				number = (int) largerThanBin[1];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					largerThanBin[1] = replace.get(number);
				}
			}
			for (Number[] largerThan : constraint.largerThan) {
				int number = (int) largerThan[0];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					largerThan[0] = replace.get(number);
				}
			}
			for (Number[] lessThan : constraint.lessThan) {
				int number = (int) lessThan[0];
				if (number == -1) {
					withElse = true;
				}
				if (number == -2) {
					withNull = true;
				}
				if (number == -1 || number == -2) {
					lessThan[0] = replace.get(number);
				}
			}
			if (!withElse && !withNull) {
				this.elseNullSave.add(0);
			}
			if (withElse && !withNull) {
				this.elseNullSave.add(1);
			}
			if (!withElse && withNull) {
				this.elseNullSave.add(2);
			}
			if (withElse && withNull) {
				this.elseNullSave.add(3);
			}
		}
	}

	public String extractConstraint(int[] indices) {
		FeatureConstraint constraint = new FeatureConstraint();
		if (this.constraintBetweenFeatures != null) {
			constraint.constraintBetweenFeatures =
				this.constraintBetweenFeatures.extractConstraint(indices);
		}
		return constraint.toString();
	}

}
