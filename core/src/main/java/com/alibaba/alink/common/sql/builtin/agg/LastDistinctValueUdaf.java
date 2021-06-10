package com.alibaba.alink.common.sql.builtin.agg;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.sql.builtin.agg.LastDistinctValueUdaf.LastDistinctSaveFirst;

import java.sql.Timestamp;


public class LastDistinctValueUdaf extends BaseUdaf<Object, LastDistinctSaveFirst> {
	private double timeInterval = -1;
	private final boolean considerNull;

	public LastDistinctValueUdaf() {
		this.considerNull = false;
	}

	public LastDistinctValueUdaf(boolean considerNull) {
		this.considerNull = considerNull;
	}

	public LastDistinctValueUdaf(double timeInterval) {
		this.timeInterval = timeInterval;
		this.considerNull = false;
	}


	@Override
	public Object getValue(LastDistinctSaveFirst accumulator) {
		return accumulator.query();
	}

	@Override
	public LastDistinctSaveFirst createAccumulator() {
		if (timeInterval == -1) {
			return new LastDistinctSaveFirst(-0.001, considerNull);//make sure inside data is -1.
		}
		return new LastDistinctSaveFirst(timeInterval, considerNull);
	}

	@Override
	public void accumulate(LastDistinctSaveFirst acc, Object... values) {
		if (values.length != 4) {
			throw new RuntimeException();
		}
		Object key = values[0];
		Object value = values[1];
		Object eventTime = values[2];
		acc.setTimeInterval(Double.parseDouble(values[3].toString()));
		acc.addOne(key, value, eventTime);
		acc.setLastKey(key);
		acc.setLastTime(eventTime);
	}

	@Override
	public void retract(LastDistinctSaveFirst acc, Object... values) {
	}

	@Override
	public void resetAccumulator(LastDistinctSaveFirst acc) {
		acc.save = null;
		acc.setLastTime(null);
		acc.setLastKey(null);
	}

	@Override
	public void merge(LastDistinctSaveFirst acc, Iterable <LastDistinctSaveFirst> it) {
	}

	public static class LastDistinctSaveFirst extends LastDistinct {
		Tuple3 <Object, Object, Object> save = null;
		private Object lastKey;
		private Object lastTime;
		private final boolean considerNull;

		public void setLastKey(Object lastKey) {
			this.lastKey = lastKey;

		}

		public void setLastTime(Object lastTime) {
			this.lastTime = lastTime;
		}

		public LastDistinctSaveFirst(double timeInterval, boolean considerNull) {
			super(timeInterval * 1000);
			this.considerNull = considerNull;
		}

		public void setTimeInterval(double timeInterval) {
			if (timeInterval == -1) {
				this.timeInterval = -1;
			} else {
				this.timeInterval = timeInterval * 1000;
			}
		}

		@Override
		public void addOne(Object key, Object value, Object eventTime) {
			//when query, if current key is null, it can query; besides, it can ignore null value.
			if (key == null && !this.considerNull) {
				return;
			}
			if (save != null) {
				super.addOne(save.f0, save.f1, save.f2);
			}
			save = Tuple3.of(key, value, eventTime);
		}

		public Object query() {
			if (lastTime instanceof Timestamp) {
				return query(lastKey, ((Timestamp) lastTime).getTime());
			} else {
				return query(lastKey, (long) lastTime);
			}
		}
	}

	public static class LastDistinct {
		//key, value, time
		Tuple3 <Object, Object, Double> firstObj;
		Tuple3 <Object, Object, Double> secondObj;
		public double timeInterval;//the metric is ms.

		public LastDistinct(double timeInterval) {
			this.timeInterval = timeInterval;
		}

		void addOne(Object key, Object value, double currentTime) {
			Tuple3<Object, Object, Double> currentNode = Tuple3.of(key, value, currentTime);
			if (firstObj == null) {
				firstObj = currentNode;
			} else if (secondObj == null) {
				secondObj = currentNode;
			} else {
				if (secondObj.f0 != key || firstObj.f0 == key) {
					firstObj = secondObj;
				}
				secondObj = currentNode;
			}
		}

		Object query(Object key, double currentTime) {
			if (secondObj != null) {
				if (secondObj.f0 == key) {
					if (!(firstObj.f0 == key) &&
						(timeInterval == -1 || currentTime - firstObj.f2 <= timeInterval)) {
						return firstObj.f1;
					} else {
						return null;
					}
				} else {
					if (timeInterval == -1 || currentTime - secondObj.f2 <= timeInterval) {
						return secondObj.f1;
					} else {
						return null;
					}
				}
			} else {
				if (firstObj != null &&
					!(firstObj.f0 == key) &&
					(timeInterval == -1 || currentTime - firstObj.f2 <= timeInterval)) {
					return firstObj.f1;
				} else {
					return null;
				}
			}
		}


		public void addOne(Object key, Object value, Object currentTime) {
			if (currentTime instanceof Timestamp) {
				addOne(key, value, ((Timestamp) currentTime).getTime());
			} else {
				addOne(key, value, (long) currentTime);
			}
		}

		public Object query(Object key, Object currentTime) {
			if (currentTime instanceof Timestamp) {
				return query(key, ((Timestamp) currentTime).getTime());
			} else {
				return query(key, (long) currentTime);
			}

		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof LastDistinct)) {
				return false;
			}
			Tuple3 <Object, Object, Double> otherFirst = ((LastDistinct) o).firstObj;
			Tuple3 <Object, Object, Double> otherSecond = ((LastDistinct) o).secondObj;
			return AggUtil.judgeNull(firstObj, otherFirst, LastDistinct::judgeTuple) &&
				AggUtil.judgeNull(secondObj, otherSecond, LastDistinct::judgeTuple);
		}

		private static boolean judgeTuple(Object a, Object b) {
			Tuple3 <Object, Object, Double> ta = (Tuple3 <Object, Object, Double>) a;
			Tuple3 <Object, Object, Double> tb = (Tuple3 <Object, Object, Double>) b;
			return AggUtil.judgeNull(ta.f0, tb.f0, AggUtil::simpleJudgeEqual) &&
				AggUtil.judgeNull(ta.f1, tb.f1, AggUtil::simpleJudgeEqual) &&
				AggUtil.judgeNull(ta.f2, tb.f2, AggUtil::simpleJudgeEqual);
		}
	}
}
