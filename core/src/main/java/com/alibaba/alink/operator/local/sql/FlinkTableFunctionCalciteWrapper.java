package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase.ParameterListBuilder;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class FlinkTableFunctionCalciteWrapper<T extends TableFunction <Row>>
	implements org.apache.calcite.schema.TableFunction, ImplementableFunction {

	public T getInstance() {
		throw new AkUnsupportedOperationException("Must implement this method in subclasses.");
	}

	public int getNumParameters() {
		throw new AkUnsupportedOperationException("Must implement this method in subclasses.");
	}

	public Method getEvalMethod() {
		//noinspection OptionalGetWithoutIsPresent
		return Arrays.stream(getInstance().getClass().getMethods()).filter(
			d -> d.getName().equals("eval")
		).findFirst().get();
	}

	public ScannableTable eval(Object... values) {
		List <Row> results = new ArrayList <>();
		ListCollector <Row> collector = new ListCollector <>(results);
		getInstance().setCollector(collector);
		Method method = getEvalMethod();
		try {
			if (1 == method.getParameterCount() && method.isVarArgs()) {
				method.invoke(getInstance(), (Object) values);
			} else {
				method.invoke(getInstance(), values);
			}
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new AkUnclassifiedErrorException("Invoke eval function failed.");
		}
		// It seems OK to use null here for rowType.
		return new CalciteTableFunctionResultTable(results, null);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List <Object> list) {
		RowTypeInfo resultType = (RowTypeInfo) getInstance().getResultType();
		String[] fieldNames = resultType.getFieldNames();
		TypeInformation <?>[] fieldTypes = resultType.getFieldTypes();
		final JavaTypeFactory typeFactory = (JavaTypeFactory) relDataTypeFactory;
		RelDataType[] types = Arrays.stream(fieldTypes)
			.map(d -> SqlTypeUtil.addCharsetAndCollation(typeFactory.createJavaType(d.getTypeClass()), typeFactory))
			.toArray(RelDataType[]::new);
		return typeFactory.createStructType(Pair.zip(fieldNames, types));
	}

	@Override
	public Type getElementType(List <Object> list) {
		return Object[].class;
	}

	@Override
	public List <FunctionParameter> getParameters() {
		ParameterListBuilder builder = new ParameterListBuilder();
		for (int i = 0; i < getNumParameters(); i += 1) {
			builder.add(Object.class, "p" + i);
		}
		return builder.build();
	}

	@Override
	public CallImplementor getImplementor() {
		//noinspection OptionalGetWithoutIsPresent
		Method method = Arrays.stream(this.getClass().getMethods())
			.filter(d -> d.getName().equals("eval"))
			.findFirst().get();
		return RexImpTable.createImplementor(new ReflectiveCallNotNullImplementor(method) {
			// copy from TableFunctionImpl#createImplementor
			@SuppressWarnings("RedundantArrayCreation")
			public Expression implement(RexToLixTranslator translator, RexCall call,
										List <Expression> translatedOperands) {
				Expression exprx = super.implement(translator, call, translatedOperands);
				Class <?> returnType = this.method.getReturnType();
				MethodCallExpression expr;
				if (QueryableTable.class.isAssignableFrom(returnType)) {
					Expression queryable = Expressions.call(Expressions.convert_(exprx, QueryableTable.class),
						BuiltInMethod.QUERYABLE_TABLE_AS_QUERYABLE.method, new Expression[] {
							Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_QUERY_PROVIDER.method,
								new Expression[0]), Expressions.constant((Object) null, SchemaPlus.class),
							Expressions.constant(call.getOperator().getName(), String.class)});
					expr = Expressions.call(queryable, BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method,
						new Expression[0]);
				} else {
					expr = Expressions.call(exprx, BuiltInMethod.SCANNABLE_TABLE_SCAN.method,
						new Expression[] {DataContext.ROOT});
				}

				return expr;
			}
		}, NullPolicy.ANY, false);
	}
}
