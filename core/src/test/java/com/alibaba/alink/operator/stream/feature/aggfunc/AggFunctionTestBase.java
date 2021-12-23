package com.alibaba.alink.operator.stream.feature.aggfunc;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.sql.builtin.agg.RankData;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Base class for aggregate function test.
 *
 * @param <I>   the type for the aggregation input. If there are multiple input, I should be array.
 * @param <R>   the type for the aggregation result
 * @param <ACC> accumulate type
 */
public abstract class AggFunctionTestBase<I, R, ACC> {

	protected abstract List <List <I>> getInputValueSets();

	protected abstract List <R> getExpectedResults();

	protected abstract AggregateFunction <R, ACC> getAggregator();

	protected abstract Class <?> getAccClass();

	protected boolean useRetract = true;

	protected boolean multiInput = false;

	protected boolean resetAcc = false;

	//todo 以下两个方法需要重写，如果输入是array的话。，需要把最后的Object.class改成Object[].class
	protected Method getAccumulateFunc() throws NoSuchMethodException {
		AggregateFunction <R, ACC> agg = getAggregator();
		if (ifMethodExistInFunction("accumulate", agg)) {
			Class <?> methodInput;
			if (multiInput) {
				methodInput = Object[].class;
			} else {
				methodInput = Object.class;
			}
			return agg.getClass().getMethod("accumulate", getAccClass(), methodInput);
		}
		return null;
	}

	protected Method getRetractFunc() throws NoSuchMethodException {
		AggregateFunction <R, ACC> agg = getAggregator();
		if (ifMethodExistInFunction("retract", agg)) {
			Class <?> methodInput;
			if (multiInput) {
				methodInput = Object[].class;
			} else {
				methodInput = Object.class;
			}
			return agg.getClass().getMethod("retract", getAccClass(), methodInput);
		}
		return null;
	}

	@Test
	// test aggregate and retract functions without partial merge
	public void testAccumulateAndRetractWithoutMerge()
		throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		// iterate over input sets
		List <List <I>> inputValueSets = getInputValueSets();
		List <R> expectedResults = getExpectedResults();
		Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
		AggregateFunction <R, ACC> aggregator = getAggregator();
		int size = inputValueSets.size();
		// iterate over input sets
		for (int i = 0; i < size; ++i) {
			List <I> inputValues = inputValueSets.get(i);
			R expected = expectedResults.get(i);
			ACC acc = accumulateValues(inputValues);
			R result = aggregator.getValue(acc);
			validateResult(expected, result);

			if (useRetract && ifMethodExistInFunction("retract", aggregator)) {
				retractValues(acc, inputValues);
				ACC expectedAcc = aggregator.createAccumulator();
				// The two accumulators should be exactly same
				//validateResult(expectedAcc, acc);
			}
		}
	}

	protected static boolean ifMethodExistInFunction(String method, AggregateFunction function) {
		Method[] methods = function.getClass().getMethods();
		for (Method funcMethod : methods) {
			if (method.equals(funcMethod.getName())) {
				return true;
			}
		}
		return false;
	}

	@Test
	public void testResetAccumulator()
		throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction <R, ACC> aggregator = getAggregator();
		if (resetAcc && ifMethodExistInFunction("resetAccumulator", aggregator)) {
			Method resetAccFunc =
				aggregator.getClass().getMethod("resetAccumulator", getAccClass());

			List <List <I>> inputValueSets = getInputValueSets();
			List <R> expectedResults = getExpectedResults();
			Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
			int size = inputValueSets.size();
			// iterate over input sets
			for (int i = 0; i < size; ++i) {
				List <I> inputValues = inputValueSets.get(i);
				ACC acc = accumulateValues(inputValues);
				resetAccFunc.invoke(aggregator, acc);
				ACC expectedAcc = aggregator.createAccumulator();
				// The accumulator after reset should be exactly same as the new accumulator
				validateResult(expectedAcc, acc);
			}
		}
	}

	protected <E> void validateResult(E expected, E result) {
		if (expected instanceof Number) {
			assertEquals(((Number) expected).doubleValue(),
				((Number) result).doubleValue(), 0.0001);
		} else {
			if (!(expected instanceof RankData)) {
				assertEquals(expected, result);
			}
		}
	}

	protected ACC accumulateValues(List <I> values)
		throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction <R, ACC> aggregator = getAggregator();
		ACC accumulator = getAggregator().createAccumulator();
		Method accumulateFunc = getAccumulateFunc();
		for (I value : values) {
			if (accumulateFunc.getParameterCount() == 1) {
				accumulateFunc.invoke(aggregator, accumulator);
			} else if (accumulateFunc.getParameterCount() == 2) {
				accumulateFunc.invoke(aggregator, accumulator, value);
			} else {
				//accumulateFunc.invoke(aggregator, accumulator, value);
				throw new TableException("Unsupported now");
			}
		}
		return accumulator;
	}

	protected void retractValues(ACC accumulator, List <I> values)
		throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction <R, ACC> aggregator = getAggregator();
		Method retractFunc = getRetractFunc();
		for (I value : values) {
			if (retractFunc.getParameterCount() == 1) {
				retractFunc.invoke(aggregator, accumulator);
			} else if (retractFunc.getParameterCount() == 2) {
				retractFunc.invoke(aggregator, accumulator, value);
			} else {
				//retractFunc.invoke(aggregator, accumulator, value);
				throw new TableException("Unsupported now");
			}
		}
	}

	//protected Tuple2 <List <I>, List <I>> splitValues(List <I> values) {
	//	return splitValues(values, values.size() / 2);
	//}
	//
	//protected Tuple2 <List <I>, List <I>> splitValues(List <I> values, int index) {
	//	List <I> firstValues = new ArrayList <>();
	//	List <I> secondValues = new ArrayList <>();
	//	int i;
	//	for (i = 0; i < values.size(); ++i) {
	//		if (i < index) {
	//			firstValues.add(values.get(i));
	//		} else {
	//			break;
	//		}
	//	}
	//	if (i < values.size()) {
	//		secondValues.addAll(values.subList(i, values.size()));
	//	}
	//	return new Tuple2 <>(firstValues, secondValues);
	//}
}