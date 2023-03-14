package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.exceptions.AkIllegalStateException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class StatisticsIteratorFactory {

	public static BaseMeasureIterator getMeasureIterator(TypeInformation <?> type) {
		return getMeasureIterator(type.getTypeClass());
	}

	public static BaseMeasureIterator getMeasureIterator(Class cls) {
		if (Number.class.isAssignableFrom(cls)) {
			if (Double.class == cls) {
				return new NumberMeasureIterator <Double>();
			} else if (Long.class == cls) {
				return new NumberMeasureIterator <Long>();
			} else if (Byte.class == cls) {
				return new NumberMeasureIterator <Byte>();
			} else if (Integer.class == cls) {
				return new NumberMeasureIterator <Integer>();
			} else if (Float.class == cls) {
				return new NumberMeasureIterator <Float>();
			} else if (Short.class == cls) {
				return new NumberMeasureIterator <Short>();
			} else if (BigDecimal.class == cls) {
				return new NumberMeasureIterator <BigDecimal>();
			} else if (BigInteger.class == cls) {
				return new NumberMeasureIterator <BigInteger>();
			}
		} else if (java.util.Date.class.isAssignableFrom(cls)) {
			if (java.sql.Timestamp.class == cls) {
				return new DateMeasureIterator <Timestamp>();
			} else if (java.sql.Date.class == cls) {
				return new DateMeasureIterator <Date>();
			} else if (java.sql.Time.class == cls) {
				return new DateMeasureIterator <Time>();
			}
		} else if (String.class == cls) {
			return new StringMeasureIterator();
		} else if (Boolean.class == cls) {
			return new BooleanMeasureIterator();
		} else if (byte[].class == cls) {
			return new VarbinaryMeasureIterator();
		}

		return new ObjectMeasureIterator <Object>();
	}

	public static TopKIterator getTopKIterator(Class cls, int smallK, int largeK) {
		if (Number.class.isAssignableFrom(cls)) {
			if (Double.class == cls) {
				return new TopKIterator <Double>(smallK, largeK);
			} else if (Long.class == cls) {
				return new TopKIterator <Long>(smallK, largeK);
			} else if (Byte.class == cls) {
				return new TopKIterator <Byte>(smallK, largeK);
			} else if (Integer.class == cls) {
				return new TopKIterator <Integer>(smallK, largeK);
			} else if (Float.class == cls) {
				return new TopKIterator <Float>(smallK, largeK);
			} else if (Short.class == cls) {
				return new TopKIterator <Short>(smallK, largeK);
			} else if (BigDecimal.class == cls) {
				return new TopKIterator <BigDecimal>(smallK, largeK);
			} else if (BigInteger.class == cls) {
				return new TopKIterator <BigInteger>(smallK, largeK);
			}
			throw new AkIllegalStateException(String.format("type [%s] not support.", cls.getSimpleName()));
		} else if (java.util.Date.class.isAssignableFrom(cls)) {
			return new TopKIterator <java.util.Date>(smallK, largeK);
		} else if (Boolean.class == cls) {
			return new TopKIterator <Boolean>(smallK, largeK);
		} else if (String.class == cls) {
			return new TopKIterator <String>(smallK, largeK);
		} else {
			return null;
		}
	}

	public static FrequencyIterator getFrequencyIterator(Class cls, int freqSize) {
		return (byte[].class == cls) ? null : new FrequencyIterator <Object>(freqSize);
	}

	public static DistinctValueIterator getDistinctValueIterator(Class cls) {
		if (Number.class.isAssignableFrom(cls)) {
			if (Double.class == cls) {
				return new DistinctValueIterator <Double>();
			} else if (Long.class == cls) {
				return new DistinctValueIterator <Long>();
			} else if (Byte.class == cls) {
				return new DistinctValueIterator <Byte>();
			} else if (Integer.class == cls) {
				return new DistinctValueIterator <Integer>();
			} else if (Float.class == cls) {
				return new DistinctValueIterator <Float>();
			} else if (Short.class == cls) {
				return new DistinctValueIterator <Short>();
			} else if (BigDecimal.class == cls) {
				return new DistinctValueIterator <BigDecimal>();
			} else if (BigInteger.class == cls) {
				return new DistinctValueIterator <BigInteger>();
			}
		} else if (java.util.Date.class.isAssignableFrom(cls)) {
			if (java.sql.Timestamp.class == cls) {
				return new DistinctValueIterator <Timestamp>();
			} else if (java.sql.Date.class == cls) {
				return new DistinctValueIterator <Date>();
			} else if (java.sql.Time.class == cls) {
				return new DistinctValueIterator <Time>();
			}
		} else if (Boolean.class == cls) {
			return new DistinctValueIterator <Boolean>();
		} else if (String.class == cls) {
			return new DistinctValueIterator <String>();
		}
		return null;
		//throw new AkIllegalStateException(String.format("type [%s] not support.", cls.getSimpleName()));
	}

}
