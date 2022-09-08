package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CalciteFunctionCompiler {

	private static final Logger LOG = LoggerFactory.getLogger(CalciteFunctionCompiler.class);

	private static final String TEMPLATE = ""
		+ "public class ${calciteClassName} extends com.alibaba.alink.operator.local.sql.FlinkTableFunctionCalciteWrapper <${className}> {\n"
		+ "\n"
		+ "\tprivate final ${className} instance = new ${className}();\n"
		+ "\n"
		+ "\t@Override\n"
		+ "\tpublic ${className} getInstance() {\n"
		+ "\t\treturn instance;\n"
		+ "\t}\n"
		+ "\n"
		+ "\t@Override\n"
		+ "\tpublic int getNumParameters() {\n"
		+ "\t\treturn ${numParameters};\n"
		+ "\t}\n"
		+ "\n"
		+ "\t@Override\n"
		+ "\tpublic org.apache.calcite.schema.ScannableTable eval(Object... values) {\n"
		+ "\t\treturn super.eval(values);\n"
		+ "\t}\n"
		+ "}";

	private final SimpleCompiler compiler;

	private ClassLoader cl;

	public CalciteFunctionCompiler(ClassLoader cl) {
		compiler = new SimpleCompiler();
		setClassLoader(cl);
	}

	/**
	 * Every time `doCompile` is called, a new classloader is generated. `setParentClassLoader` must be called with this
	 * new classloader to from a chain.
	 *
	 * @param cl classloader.
	 */
	private void setClassLoader(ClassLoader cl) {
		this.cl = cl;
		compiler.setParentClassLoader(cl);
	}

	public ClassLoader getClassLoader() {
		return cl;
	}

	public <T extends TableFunction <Row>> Class <org.apache.calcite.schema.TableFunction> compileVarargsTableFunction(
		String name, int numParameters, Class <T> clazz) {
		Map <String, String> valueMap = new HashMap <>();
		valueMap.put("calciteClassName", name);
		valueMap.put("className", clazz.getCanonicalName());
		valueMap.put("numParameters", Integer.toString(numParameters));
		StrSubstitutor strSubstitutor = new StrSubstitutor(valueMap);
		String code = strSubstitutor.replace(TEMPLATE);
		return doCompile(name, code);
	}

	private <T> Class <T> doCompile(String name, String code) {
		LOG.debug("Compiling: {} \n\n Code:\n{}", name, code);
		try {
			compiler.cook(code);
		} catch (Throwable t) {
			System.out.println(addLineNumber(code));
			throw new InvalidProgramException(
				"Table program cannot be compiled. This is a bug. Please file an issue.", t);
		}
		try {
			setClassLoader(compiler.getClassLoader());
			//noinspection unchecked
			return (Class <T>) compiler.getClassLoader().loadClass(name);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Can not load class " + name, e);
		}
	}

	public org.apache.calcite.schema.TableFunction getCalciteTableFunction(
		String name, int numParameters, Class <? extends TableFunction <Row>> clazz) {
		Class <org.apache.calcite.schema.TableFunction> calciteClass =
			compileVarargsTableFunction(name, numParameters, clazz);
		try {
			return calciteClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new AkUnclassifiedErrorException("Failed to create instance.");
		}
	}

	private static String addLineNumber(String code) {
		String[] lines = code.split("\n");
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < lines.length; i++) {
			builder.append("/* ").append(i + 1).append(" */").append(lines[i]).append("\n");
		}
		return builder.toString();
	}
}
