package com.alibaba.alink.common.pyrunner.fn;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge.PY_TURN_ON_LOGGING_KEY;

public class PyTableFn extends TableFunction <Row> implements Serializable {

	protected final String name;
	protected final String fnSpecJson;
	protected final Map <String, String> runConfig;
	protected PyTableFnRunner runner;
	protected String[] resultTypeStrs;
	protected TypeInformation <?>[] resultTypes;

	public PyTableFn(String name, String fnSpecJson, String[] resultTypeStrs) {
		this(name, fnSpecJson, resultTypeStrs, Collections. <String, String>emptyMap());

	}

	public PyTableFn(String name, String fnSpecJson, String[] resultTypeStrs,
					 Map <String, String> runConfig) {
		this.name = name;
		this.fnSpecJson = fnSpecJson;
		this.resultTypeStrs = resultTypeStrs;
		this.runConfig = runConfig;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		Tuple2 <String, Map <String, String>> updated = PyFnUtils.updateFnSpecRunConfigWithPlugin(fnSpecJson,
			runConfig);
		String updatedFnSpecJson = updated.f0;
		Map <String, String> updatedRunConfig = updated.f1;
		SerializableBiFunction <String, String, String> newRunConfigGetter = (key, defaultValue) -> {
			if (PY_TURN_ON_LOGGING_KEY.equals(key)) {
				return String.valueOf(AlinkGlobalConfiguration.isPrintProcessInfo());
			} else {
				return updatedRunConfig.getOrDefault(key, context.getJobParameter(key, defaultValue));
			}
		};
		PyCollector pyCollector = new PyCollector(this::collect, resultTypes);
		runner = new PyTableFnRunner(pyCollector, updatedFnSpecJson, resultTypeStrs, newRunConfigGetter);
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

	public static class PyCollector implements Collector <List <Object>> {

		private Consumer <Row> collectFn;
		private final TypeInformation <?>[] resultTypes;

		public PyCollector(Consumer <Row> collectFn, TypeInformation <?>[] resultTypes) {
			this.collectFn = collectFn;
			this.resultTypes = resultTypes;
		}

		public void setCollectFn(Consumer <Row> collectFn) {
			this.collectFn = collectFn;
		}

		@Override
		public void collect(List <Object> record) {
			AkPreconditions.checkArgument(record.size() == resultTypes.length,
				new AkIllegalOperationException("Python UDTF returns wrong number of elements."));
			Object[] values = new Object[resultTypes.length];
			for (int i = 0; i < resultTypes.length; i += 1) {
				values[i] = DataConversionUtils.pyToJava(record.get(i), resultTypes[i]);
			}
			collectFn.accept(Row.of(values));
		}

		@Override
		public void close() {
		}
	}
}
