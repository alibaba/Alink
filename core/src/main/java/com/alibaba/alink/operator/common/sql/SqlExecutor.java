package com.alibaba.alink.operator.common.sql;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * An interface for SQL executors.
 *
 * @param <T> the type of input and output tables.
 */
interface SqlExecutor<T> {

	/**
	 * Add a table.
	 *
	 * @param name table name.
	 * @param t    table.
	 */
	void addTable(String name, T t);

	/**
	 * Remove the table by its name.
	 *
	 * @param name table name.
	 */
	void removeTable(String name);

	void addFunction(String name, ScalarFunction function);

	void addFunction(String name, TableFunction <Row> function);

	/**
	 * Execute SQL query and return the result.
	 *
	 * @param sql SQL query.
	 * @return the result.
	 */
	T query(String sql);

	/**
	 * Generate a random name for table.
	 *
	 * @return a generated table name.
	 */
	default String generateTableName() {
		return RandomStringUtils.randomAlphabetic(16);
	}

	/**
	 * Execute select query.
	 *
	 * @param fields The query fields.
	 * @return The result.
	 */
	default T select(T t, String fields) {
		String name = generateTableName();
		addTable(name, t);
		T result = query(String.format("SELECT %s FROM %s", fields, name));
		removeTable(name);
		return result;
	}

	/**
	 * Rename the fields.
	 *
	 * @param fields Comma separated field names.
	 * @return The result.
	 */
	T as(T t, String fields);

	/**
	 * Execute where query.
	 *
	 * @param predicate The where predicate.
	 * @return The result.
	 */
	default T where(T t, String predicate) {
		String name = generateTableName();
		addTable(name, t);
		T result = query(String.format("SELECT * FROM %s WHERE %s", name, predicate));
		removeTable(name);
		return result;
	}

	/**
	 * Execute filter query.
	 *
	 * @param predicate The filter predicate.
	 * @return The result.
	 */
	default T filter(T t, String predicate) {
		return where(t, predicate);
	}

	/**
	 * Execute distinct query.
	 *
	 * @return The result.
	 */
	default T distinct(T t) {
		String name = generateTableName();
		addTable(name, t);
		T result = query(String.format("SELECT DISTINCT * FROM %s", name));
		removeTable(name);
		return result;
	}

	/**
	 * Order the records by a specific field and return a limited number of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param limit     The maximum number of records to keep.
	 * @return The result.
	 */
	default T orderBy(T t, String fieldName, boolean isAscending, int limit) {
		return orderByImpl(t, fieldName, isAscending, limit, -1, -1);
	}

	/**
	 * Order the records by a specific field and return a specific range of records.
	 *
	 * @param fieldName The name of the field by which the records are ordered.
	 * @param offset    The starting position of records to keep.
	 * @param fetch     The  number of records to keep.
	 * @return The result.
	 */
	default T orderBy(T t, String fieldName, boolean isAscending, int offset, int fetch) {
		return orderByImpl(t, fieldName, isAscending, -1, offset, fetch);
	}

	default T orderByImpl(T t, String fieldName, boolean isAscending, int limit, int offset, int fetch) {
		String name = generateTableName();
		addTable(name, t);
		StringBuilder s = new StringBuilder();
		s.append("SELECT * FROM ")
			.append(name)
			.append(" ORDER BY ")
			.append(fieldName)
			.append(" ")
			.append(isAscending ? "ASC" : "DESC");
		if (limit >= 0) {
			s.append(" LIMIT ").append(limit);
		}
		if (offset >= 0) {
			s.append(" OFFSET ").append(offset).append(" ROW ");
		}
		if (fetch >= 0) {
			s.append(" FETCH FIRST ").append(fetch).append(" ROW ONLY");
		}
		T result = query(s.toString());
		removeTable(name);
		return result;
	}

	/**
	 * Execute group by query.
	 *
	 * @param groupByPredicate The fields by which records are grouped.
	 * @param fields           The fields to select after group by.
	 * @return The result.
	 */
	default T groupBy(T t, String groupByPredicate, String fields) {
		String name = generateTableName();
		addTable(name, t);
		T result = query(String.format("SELECT %s FROM %s GROUP BY %s", fields, name, groupByPredicate));
		removeTable(name);
		return result;
	}

	default T joinImpl(T leftOp, T rightOp, String joinPredicate, String selectClause, String joinType) {
		String leftName = generateTableName();
		String rightName = generateTableName();
		addTable(leftName, leftOp);
		addTable(rightName, rightOp);
		T result = query(
			String.format("SELECT %s FROM %s AS a %s %s AS b ON %s", selectClause, leftName, joinType, rightName,
				joinPredicate));
		removeTable(leftName);
		removeTable(rightName);
		return result;
	}

	/**
	 * Execute join query.
	 *
	 * @param leftOp        table on the left side.
	 * @param rightOp       table on the right side.
	 * @param joinPredicate The join predicate.
	 * @param fields        The clause specifying the fields to select.
	 * @return The result.
	 */
	default T join(T leftOp, T rightOp, String joinPredicate, String fields) {
		return joinImpl(leftOp, rightOp, joinPredicate, fields, "JOIN");
	}

	/**
	 * Execute left-outer join query.
	 *
	 * @param leftOp        table on the left side.
	 * @param rightOp       table on the right side.
	 * @param joinPredicate The join predicate.
	 * @param fields        The clause specifying the fields to select.
	 * @return The result.
	 */
	default T leftOuterJoin(T leftOp, T rightOp, String joinPredicate, String fields) {
		return joinImpl(leftOp, rightOp, joinPredicate, fields, "LEFT OUTER JOIN");
	}

	/**
	 * Execute right-outer join query.
	 *
	 * @param leftOp        table on the left side.
	 * @param rightOp       table on the right side.
	 * @param joinPredicate The join predicate.
	 * @param fields        The clause specifying the fields to select.
	 * @return The result.
	 */
	default T rightOuterJoin(T leftOp, T rightOp, String joinPredicate, String fields) {
		return joinImpl(leftOp, rightOp, joinPredicate, fields, "RIGHT OUTER JOIN");
	}

	/**
	 * Execute full-outer join query.
	 *
	 * @param leftOp        table on the left side.
	 * @param rightOp       table on the right side.
	 * @param joinPredicate The join predicate.
	 * @param fields        The clause specifying the fields to select.
	 * @return The result.
	 */
	default T fullOuterJoin(T leftOp, T rightOp, String joinPredicate, String fields) {
		return joinImpl(leftOp, rightOp, joinPredicate, fields, "FULL OUTER JOIN");
	}

	default T intersect(T left, T right) {
		String leftName = generateTableName();
		String rightName = generateTableName();
		addTable(leftName, left);
		addTable(rightName, right);
		T result = query(String.format("SELECT * FROM %s INTERSECT SELECT * from %s", leftName, rightName));
		removeTable(leftName);
		removeTable(rightName);
		return result;
	}

	default T intersectAll(T left, T right) {
		String leftName = generateTableName();
		String rightName = generateTableName();
		addTable(leftName, left);
		addTable(rightName, right);
		T result = query(String.format("SELECT * FROM %s INTERSECT ALL SELECT * from %s", leftName, rightName));
		removeTable(leftName);
		removeTable(rightName);
		return result;
	}

	default T union(T left, T right) {
		String leftName = generateTableName();
		String rightName = generateTableName();
		addTable(leftName, left);
		addTable(rightName, right);
		T result = query(String.format("SELECT * FROM %s UNION SELECT * from %s", leftName, rightName));
		removeTable(leftName);
		removeTable(rightName);
		return result;
	}

	default T unionAll(T left, T right) {
		String leftName = generateTableName();
		String rightName = generateTableName();
		addTable(leftName, left);
		addTable(rightName, right);
		T result = query(String.format("SELECT * FROM %s UNION ALL SELECT * from %s", leftName, rightName));
		removeTable(leftName);
		removeTable(rightName);
		return result;
	}

	default T minus(T left, T right) {
		String leftName = generateTableName();
		String rightName = generateTableName();
		addTable(leftName, left);
		addTable(rightName, right);
		T result = query(String.format("SELECT * FROM %s EXCEPT SELECT * from %s", leftName, rightName));
		removeTable(leftName);
		removeTable(rightName);
		return result;
	}

	default T minusAll(T left, T right) {
		String leftName = generateTableName();
		String rightName = generateTableName();
		addTable(leftName, left);
		addTable(rightName, right);
		T result = query(String.format("SELECT * FROM %s EXCEPT ALL SELECT * from %s", leftName, rightName));
		removeTable(leftName);
		removeTable(rightName);
		return result;
	}
}
