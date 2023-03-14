/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.operator.common.statistics.interval;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;

import java.math.BigDecimal;

/**
 * @author zhihan.gao
 */
class Interval implements Cloneable {

	public double start;
	public double end;
	public double step;
	public int numIntervals;
	public int numStartToEnd;
	public boolean bEqualAtRight;
	public boolean leftResult;
	public boolean rightResult;

	public Interval(double start, int size, double step, boolean bEqualAtRight, boolean leftResult, boolean
		rightResult) {
		if (step <= 0) {
			throw new AkIllegalStateException("step must be positive!");
		}
		//    System.out.println("Ok");
		this.start = start;
		BigDecimal startBD = new BigDecimal(String.valueOf(start));
		BigDecimal sizeBD = new BigDecimal(size);
		BigDecimal stepBD = new BigDecimal(String.valueOf(step));
		this.end = (startBD.add(stepBD.multiply(sizeBD))).doubleValue();
		this.step = step;
		this.bEqualAtRight = bEqualAtRight;
		this.leftResult = leftResult;
		this.rightResult = rightResult;
		numStartToEnd = size;
		if (leftResult) {
			size++;
		}
		if (rightResult) {
			size++;
		}
		numIntervals = size;
	}

	public static Interval findInterval(double min, double max, int N) {
		if (min > max) {
			return null;
		}
		if (max - min < Double.MIN_VALUE) {
			BigDecimal stepBD = new BigDecimal(1);
			int start = (int) (Math.floor(min / stepBD.doubleValue()));
			BigDecimal startBD = new BigDecimal(start);
			Object[] r = new Object[3];
			r[0] = startBD;
			r[1] = stepBD;
			r[2] = 1;
			return new Interval(((BigDecimal) r[0]).doubleValue(), (Integer) (r[2]), ((BigDecimal) r[1]).doubleValue(),
				false, false, false);
		}
		BigDecimal stepBD = new BigDecimal(100000000);
		BigDecimal scaleBD = new BigDecimal(10);
		int i = 0;
		long k = 0;
		long start = 0;
		long end = 0;
		for (; i < 17; i++) {
			start = (int) (Math.floor(min / stepBD.doubleValue()));
			end = (int) (Math.ceil(max / stepBD.doubleValue()));
			if ((max / stepBD.doubleValue() - end) < Double.MIN_VALUE) {
				end++;
			}
			k = end - start;
			if (k > N && k <= N * 10) {
				break;
			}
			stepBD = stepBD.divide(scaleBD);
		}
		if (i == 17) {
			double step = Math.pow(10.0, Math.ceil(Math.log10((max - min) / N)));
			return new Interval(Math.floor(min / step) * step, (int) (Math.ceil(max / step) - Math.floor(min / step)),
				step, false, false, false);
		}
		BigDecimal startBD = new BigDecimal(start).multiply(stepBD);
		Object[] r = new Object[3];
		r[0] = startBD;
		r[1] = stepBD;
		r[2] = k;
		return new Interval(((BigDecimal) r[0]).doubleValue(), Integer.parseInt(String.valueOf(r[2])),
			((BigDecimal) r[1]).doubleValue(), false, false, false);
	}

	public double getStart() {
		return start;
	}

	public double getEnd() {
		return end;
	}

	public double getStep() {
		return step;
	}

	public boolean getBEqualAtRight() {
		return bEqualAtRight;
	}

	public boolean getLeftResult() {
		return leftResult;
	}

	public boolean getRightResult() {
		return rightResult;
	}

	@Override
	public Object clone() {
		Interval o = null;
		try {
			o = (Interval) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return o;
	}

	public int Position(double valueID) {
		int num = -1;
		if (bEqualAtRight) {
			if (valueID <= start) {
				if (leftResult) {
					num = 0;
				} else {
					num = -1;
				}
			} else if (valueID > end) {
				if (rightResult) {
					num = numIntervals - 1;
				} else {
					num = -1;
				}
			} else {
				double temp = (valueID - start) / step;
				int tempInt = (int) (temp);
				if ((temp - tempInt) < Double.MIN_VALUE) {
					tempInt--;
				}
				num = tempInt;
				if (leftResult) {
					num++;
				}
			}
		} else {
			if (valueID < start) {
				if (leftResult) {
					num = 0;
				} else {
					num = -1;
				}
			} else if (valueID >= end) {
				if (rightResult) {
					num = numIntervals - 1;
				} else {
					num = -1;
				}
			} else {
				double temp = (valueID - start) / step;
				int tempInt = (int) (temp);
				num = tempInt;
				if (leftResult) {
					num++;
				}
			}
		}
		if (num < 0) {
			num = 0;
		}

		return num;
	}

	public int NumIntervals() {
		return numIntervals;
	}

	public String toString(int pos) {
		if (pos < 0 || pos >= numIntervals) {
			throw new AkIllegalStateException("Out of bound!");
		}
		if (pos == numIntervals - 1 && rightResult) {
			if (bEqualAtRight) {
				return "(" + end + ", " + "infinity)";
			} else {
				return "[" + end + ", " + "infinity)";
			}
		}
		BigDecimal posBD = new BigDecimal(pos);
		BigDecimal stepBD = new BigDecimal(String.valueOf(step));
		BigDecimal startBD = new BigDecimal(String.valueOf(start));

		double left;
		double right;
		if (leftResult) {
			if (pos == 0) {
				if (bEqualAtRight) {
					return "(-infinity, " + start + "]";
				} else {
					return "(-infinity, " + start + ")";
				}
			}
			pos--;
			if (pos == numStartToEnd - 1) {
				left = (startBD.add(posBD.multiply(stepBD))).doubleValue();
				right = end;
			} else {
				left = (startBD.add(posBD.multiply(stepBD))).doubleValue();
				right = (startBD.add(posBD.multiply(stepBD)).add(stepBD)).doubleValue();
			}
		} else {

			left = (startBD.add(posBD.multiply(stepBD))).doubleValue();
			right = (startBD.add(posBD.multiply(stepBD)).add(stepBD)).doubleValue();
		}
		if (bEqualAtRight) {
			return "(" + left + ", " + right + "]";
		} else {
			return "[" + left + ", " + right + ")";
		}
	}
}
