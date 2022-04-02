package com.alibaba.alink.common.pyrunner.fn;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_TURN_ON_LOGGING_KEY;

public class PyTableFn extends TableFunction <Row> implements Serializable {

	protected final String name;
	protected final String fnSpecJson;
	protected final SerializableBiFunction <String, String, String> runConfigGetter;
	protected PyTableFnRunner runner;
	protected String[] resultTypeStrs;
	protected TypeInformation <?>[] resultTypes;

	public PyTableFn(String name, String fnSpecJson, String[] resultTypeStrs) {
		this(name, fnSpecJson, resultTypeStrs, Collections. <String, String>emptyMap()::getOrDefault);

	}

	public PyTableFn(String name, String fnSpecJson, String[] resultTypeStrs,
					 SerializableBiFunction <String, String, String> runConfigGetter) {
		this.name = name;
		this.fnSpecJson = fnSpecJson;
		this.resultTypeStrs = resultTypeStrs;
		this.runConfigGetter = runConfigGetter;
	}



	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);

		SerializableBiFunction <String, String, String> newRunConfigGetter = (key, defaultValue) -> {
			if (PY_TURN_ON_LOGGING_KEY.equals(key)) {
				return String.valueOf(AlinkGlobalConfiguration.isPrintProcessInfo());
			} else {
				return runConfigGetter.apply(key, context.getJobParameter(key, defaultValue));
			}
		};
		runner = new PyTableFnRunner(new PyCollector(), fnSpecJson, resultTypeStrs, newRunConfigGetter);
		runner.open();
	}

	@Override
	public void close() throws Exception {
		runner.close();
		super.close();
	}

	public void eval(Object... args) {
		runner.calc(args);
	}

	@Override
	public TypeInformation <Row> getResultType() {
		resultTypes = Arrays.stream(resultTypeStrs)
			.map(AlinkTypes::getTypeInformation)
			.toArray(TypeInformation[]::new);
		return Types.ROW(resultTypes);
	}

	class PyCollector implements Collector <List <Object>> {
		@Override
		public void collect(List <Object> record) {
			Preconditions.checkArgument(record.size() == resultTypes.length,
				"Python UDTF returns wrong number of elements.");
			Object[] values = new Object[resultTypes.length];
			for (int i = 0; i < resultTypes.length; i += 1) {
				values[i] = DataConversionUtils.pyToJava(record.get(i), resultTypes[i]);
			}
			PyTableFn.this.collect(Row.of(values));
		}

		@Override
		public void close() {
		}
	}
}
