package com.alibaba.alink.operator.common.optim;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * ConstraintBetweenFeatures support six types constraint, and support 2 types of features.
 * In scorecard, one feature may be divided into several partition, so this class supports
 * constraint between features, or between partitions.
 */
public class ConstraintBetweenFeatures implements Serializable {
	private static final long serialVersionUID = 1830363299295476243L;
	private final String name = "constraintBetweenFeatures";
	@JsonProperty("UP")
	public List <Object[]> lessThan = new ArrayList <>();
	@JsonProperty("LO")
	public List <Object[]> largerThan = new ArrayList <>();
	@JsonProperty("=")
	public List <Object[]> equal = new ArrayList <>();
	@JsonProperty("%")
	public List <Object[]> scale = new ArrayList <>();
	@JsonProperty("<")
	public List <Object[]> lessThanFeature = new ArrayList <>();
	@JsonProperty(">")
	public List <Object[]> largerThanFeature = new ArrayList <>();

	//for table.
	public void addLessThan(String f1, int i1, double value) {
		if (lessThan.size() == 0 || lessThan.get(0).length == 3) {
			Object[] temp = new Object[] {f1, i1, value};
			lessThan.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addLargerThan(String f1, int i1, double value) {
		if (largerThan.size() == 0 || largerThan.get(0).length == 3) {
			Object[] temp = new Object[] {f1, i1, value};
			largerThan.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addEqual(String f1, int i1, double value) {
		if (equal.size() == 0 || equal.get(0).length == 3) {
			Object[] temp = new Object[] {f1, i1, value};
			equal.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addScale(String f1, int i1, String f2, int i2, double time) {
		if (scale.size() == 0 || scale.get(0).length == 5) {
			Object[] temp = new Object[] {f1, i1, f2, i2, time};
			scale.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addLessThanFeature(String f1, int i1, String f2, int i2) {
		if (lessThanFeature.size() == 0 || lessThanFeature.get(0).length == 4) {
			Object[] temp = new Object[] {f1, i1, f2, i2};
			lessThanFeature.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addLargerThanFeature(String f1, int i1, String f2, int i2) {
		if (largerThanFeature.size() == 0 || largerThanFeature.get(0).length == 4) {
			Object[] temp = new Object[] {f1, i1, f2, i2};
			largerThanFeature.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	//for vector.
	public void addLessThan(int i1, double value) {
		if (lessThan.size() == 0 || lessThan.get(0).length == 2) {
			Object[] temp = new Object[] {i1, value};
			lessThan.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addLargerThan(int i1, double value) {
		if (largerThan.size() == 0 || largerThan.get(0).length == 2) {
			Object[] temp = new Object[] {i1, value};
			largerThan.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addEqual(int i1, double value) {
		if (equal.size() == 0 || equal.get(0).length == 2) {
			Object[] temp = new Object[] {i1, value};
			equal.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addScale(int i1, int i2, double time) {
		if (scale.size() == 0 || scale.get(0).length == 3) {
			Object[] temp = new Object[] {i1, i2, time};
			scale.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addLessThanFeature(int i1, int i2) {
		if (lessThanFeature.size() == 0 || lessThanFeature.get(0).length == 2) {
			Object[] temp = new Object[] {i1, i2};
			lessThanFeature.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public void addLargerThanFeature(int i1, int i2) {
		if (largerThanFeature.size() == 0 || largerThanFeature.get(0).length == 2) {
			Object[] temp = new Object[] {i1, i2};
			largerThanFeature.add(temp);
		} else {
			throw new RuntimeException();
		}
	}

	public String toString() {
		return JsonConverter.toJson(this);
	}

	public static ConstraintBetweenFeatures fromJson(String constraintJson) {
		if (constraintJson == null || constraintJson.equals("")) {
			return new ConstraintBetweenFeatures();
		}
		ConstraintBetweenFeatures constraint = JsonConverter.fromJson(constraintJson, ConstraintBetweenFeatures.class);

		if (constraint.lessThanFeature.size() + constraint.largerThanFeature.size() == 0) {
			return constraint;
		}
		List <Object[]> lessThanFeature = new ArrayList <>();
		List <Object[]> largerThanFeature = new ArrayList <>();
		if (constraint.lessThanFeature.size() != 0 && constraint.lessThanFeature.get(0)[0] instanceof String ||
			constraint.largerThanFeature.size() != 0 && constraint.largerThanFeature.get(0)[0] instanceof String) {
			for (Object[] item : constraint.lessThanFeature) {
				int size = item.length / 2;
				if (size == 2) {
					lessThanFeature.add(item);
				} else {
					for (int i = 0; i < size - 1; i++) {
						lessThanFeature.add(
							new Object[] {item[i * 2], item[i * 2 + 1], item[i * 2 + 2], item[i * 2 + 3]});
					}
				}
			}

			for (Object[] item : constraint.largerThanFeature) {
				int size = item.length / 2;
				if (size == 2) {
					largerThanFeature.add(item);
				} else {
					for (int i = 0; i < size - 1; i++) {
						largerThanFeature.add(
							new Object[] {item[i * 2], item[i * 2 + 1], item[i * 2 + 2], item[i * 2 + 3]});
					}
				}
			}
		} else {
			for (Object[] item : constraint.lessThanFeature) {
				int size = item.length;
				if (size == 2) {
					lessThanFeature.add(item);
				} else {
					for (int i = 0; i < size - 1; i++) {
						lessThanFeature.add(new Object[] {item[i], item[i + 1]});
					}
				}
			}

			for (Object[] item : constraint.largerThanFeature) {
				int size = item.length;
				if (size == 2) {
					largerThanFeature.add(item);
				} else {
					for (int i = 0; i < size - 1; i++) {
						largerThanFeature.add(new Object[] {item[i], item[i + 1]});
					}
				}
			}
		}
		constraint.lessThanFeature = lessThanFeature;
		constraint.largerThanFeature = largerThanFeature;
		return constraint;
	}

	/**
	 * extract constraints which are in the indices from the all constraint.
	 */
	public ConstraintBetweenFeatures extractConstraint(String[] indices) {
		HashSet <String> indicesSet = new HashSet <>(indices.length);
		for (String index : indices) {
			indicesSet.add(index);
		}
		ConstraintBetweenFeatures constraint = new ConstraintBetweenFeatures();
		for (Object[] objects : this.lessThan) {
			if (indicesSet.contains(objects[0])) {
				constraint.addLessThan((String) objects[0], (int) objects[1], (double) objects[2]);
			}
		}
		for (Object[] objects : this.largerThan) {
			if (indicesSet.contains(objects[0])) {
				constraint.addLargerThan((String) objects[0], (int) objects[1], (double) objects[2]);
			}
		}
		for (Object[] objects : this.equal) {
			if (indicesSet.contains(objects[0])) {
				constraint.addEqual((String) objects[0], (int) objects[1], (double) objects[2]);
			}
		}
		for (Object[] objects : this.scale) {
			if (indicesSet.contains(objects[0]) || indicesSet.contains(objects[2])) {
				constraint.addScale((String) objects[0], (int) objects[1], (String) objects[2], (int) objects[3],
					(double) objects[4]);
			}
		}
		for (Object[] objects : this.largerThanFeature) {
			if (indicesSet.contains(objects[0]) || indicesSet.contains(objects[2])) {
				constraint.addLargerThanFeature((String) objects[0], (int) objects[1], (String) objects[2],
					(int) objects[3]);
			}
		}
		for (Object[] objects : this.lessThanFeature) {
			if (indicesSet.contains(objects[0]) || indicesSet.contains(objects[2])) {
				constraint.addLessThanFeature((String) objects[0], (int) objects[1], (String) objects[2],
					(int) objects[3]);
			}
		}
		return constraint;
	}

	public ConstraintBetweenFeatures extractConstraint(int[] indices) {
		ConstraintBetweenFeatures constraint = new ConstraintBetweenFeatures();
		for (Object[] objects : this.lessThan) {
			int idx = findIdx(indices, (int) objects[0]);
			if (idx >= 0) {
				constraint.addLessThan(idx, (double) objects[1]);
			}
		}
		for (Object[] objects : this.largerThan) {
			int idx = findIdx(indices, (int) objects[0]);
			if (idx >= 0) {
				constraint.addLargerThan(idx, (double) objects[1]);
			}
		}
		for (Object[] objects : this.equal) {
			int idx = findIdx(indices, (int) objects[0]);
			if (idx >= 0) {
				constraint.addEqual(idx, (double) objects[1]);
			}
		}
		for (Object[] objects : this.scale) {
			int idx0 = findIdx(indices, (int) objects[0]);
			int idx1 = findIdx(indices, (int) objects[1]);
			if (idx0 >= 0 && idx1 >= 0) {
				constraint.addScale(idx0, idx1, (double) objects[2]);
			}
		}
		for (Object[] objects : this.lessThanFeature) {
			int idx0 = findIdx(indices, (int) objects[0]);
			int idx1 = findIdx(indices, (int) objects[1]);
			if (idx0 >= 0 && idx1 >= 0) {
				constraint.addLessThanFeature(idx0, idx1);
			}
		}
		for (Object[] objects : this.largerThanFeature) {
			int idx0 = findIdx(indices, (int) objects[0]);
			int idx1 = findIdx(indices, (int) objects[1]);
			if (idx0 >= 0 && idx1 >= 0) {
				constraint.addLargerThanFeature(idx0, idx1);
			}
		}
		return constraint;
	}

	public int getInequalSize() {
		return lessThan.size() + largerThan.size() + lessThanFeature.size() + largerThanFeature.size();
	}

	public int getEqualSize() {
		return equal.size() + scale.size();
	}

	private int findIdx(int[] indices, int idx) {
		for (int i = 0; i < indices.length; i++) {
			if (idx == indices[i]) {
				return i;
			}
		}
		return -1;
	}
}
