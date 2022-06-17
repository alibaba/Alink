package com.alibaba.alink.operator.common.sql;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.dataproc.HasClause;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.sql.Select;
import com.alibaba.alink.testutil.AlinkTestBase;
import io.reactivex.rxjava3.functions.BiFunction;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SelectMapperTest extends AlinkTestBase {
	@Test
	public void testInputTypeSupport() throws Exception {
		{
			Select select = new Select().setClause("age + 1 as age");
			PipelineModel pipeline = new PipelineModel(select);
			LocalPredictor localPredictor = pipeline.collectLocalPredictor("age DECIMAL");
			Row result = localPredictor.map(Row.of(new BigDecimal("1.")));
			System.out.println(result);
		}

		{
			Select select = new Select().setClause("age + 1 as age");
			PipelineModel pipeline = new PipelineModel(select);
			LocalPredictor localPredictor = pipeline.collectLocalPredictor("age BIGINT");
			Row result = localPredictor.map(Row.of(new BigInteger("1")));
			System.out.println(result);
		}
	}

	@Test
	public void testGeneral() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"id, name as eman, id + 1 as id2, CASE WHEN id=1 THEN 'q' ELSE 'p' END as col3, UPPER(name) as col4");
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row expected = Row.of(1, "'abc'", 2, "q", "'ABC'");
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(expected, output);
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testComparison() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"id = id, id <> id, id > id, id >= id, id < id, id <=id, id IS NULL, id IS NOT NULL, "
				+ "id IS DISTINCT FROM id, id IS NOT DISTINCT FROM id, id BETWEEN id AND id, id NOT BETWEEN id AND "
				+ "id, "
				+ "name LIKE name, name NOT LIKE name, name SIMILAR TO name, name NOT SIMILAR TO name,"
				+ "name IN (name, name), name NOT IN (name, name)"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row expected = Row.of(true, false, false, true, false, true, false, true, false, true, true, false, true,
			false,
			true, false, true, false);
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(expected, output);
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testLogicalComparison() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"TRUE OR FALSE, true AND false, NOT true, true IS FALSE, true IS NOT FALSE,"
				+ "true IS TRUE, true IS NOT TRUE, unknown IS UNKNOWN, true IS NOT UNKNOWN"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row expected = Row.of(true, false, false, false, true, true, false, true, true);
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(expected, output);
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testArithmeticFunctions() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"+ id, - id, id + id, id - id, id * id, id / id, POWER(id, id), ABS(-id), MOD(id, id),"
				+ "SQRT(id), LN(id), LOG10(id), EXP(id), CEIL(id),"
				+ "CEILING(id), FLOOR(id), SIN(id), COS(id), TAN(id), COT(id), ASIN(id),"
				+ "ACOS(id), ATAN(id), ATAN2(id, id), DEGREES(id), RADIANS(id), SIGN(id),"
				+ "ROUND(id, id), PI, RAND(), RAND(id), RAND_INTEGER(id), RAND_INTEGER(id, id),"
				+ "TRUNCATE(id, id),"
				+ "LOG2(id), LOG(id), SINH(id), COSH(id), TANH(id), UUID(), BIN(id),"
				+ "LOG(3, id), HEX(id), HEX(name)"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row expected = Row.of(1, -1, 2, 0, 1, 1, 1.0, 1, 0, 1.0, 0.0, 0.0, 2.718281828459045, 1, 1, 1,
			0.8414709848078965, 0.5403023058681398, 1.5574077246549023, 0.6420926159343306, 1.5707963267948966, 0.0,
			0.7853981633974483, 0.7853981633974483, 57.29577951308232, 0.017453292519943295, 1, 1, 3.141592653589793,
			0.03295418033754882, 0.6333826038590553, 0, 0, 1, 0.0, 0.0, 1.1752011936438014, 1.543080634815244,
			0.7615941559557649, "3fe5d54b-3c07-4d0b-80a2-f78a1f9f95b3", "1", 0.0, "1", "2761626327");
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(expected.getArity(), output.getArity());
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testStringFunctions() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"name || name, CHAR_LENGTH(name), CHARACTER_LENGTH(name), UPPER(name), LOWER(name), POSITION(name IN "
				+ "name),"
				+ "TRIM('a' FROM name), REPEAT(name, 3)"
				+ ", OVERLAY('This is an old string' PLACING ' new' FROM 10 FOR 5)"
				+ ", SUBSTRING(name FROM 2)"
				+ ", REPLACE('hello world', 'world', 'flink')"
				+ ", INITCAP(name)"
				+ ", FROM_BASE64('aGVsbG8gd29ybGQ=')"
				+ ", TO_BASE64('hello world')"
				+ ", LPAD('hi',4,'??')"
				+ ", RPAD('hi',4,'??')"
				+ ", REGEXP_REPLACE('foobar', 'oo|ar', '')"
				+ ", REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 2)"
				+ ", LTRIM(' This is a test String.')"
				+ ", RTRIM('This is a test String. ')"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row expected = Row.of("'abc''abc'", 5, 5, "'ABC'", "'abc'", 1, "'abc'", "'abc''abc''abc'",
			"This is a new string", "abc'", "hello flink", "'Abc'", "hello world", "aGVsbG8gd29ybGQ=", "??hi", "hi??",
			"fb", "bar", "This is a test String.", "This is a test String.");
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		assertEquals(expected.getArity(), output.getArity());
		try {
			assertEquals(expected, output);
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testTemporalFunctions() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"DATE '1990-01-01', TIME '23:23:23', TIMESTAMP '1990-01-01 23:23:23', "
				+ "INTERVAL '10 00:00:00.004' DAY TO SECOND,"
				+ "CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP,"
				+ "EXTRACT(DAY FROM DATE '2006-06-05'), YEAR(DATE '1994-09-27'),"
				+ "QUARTER(DATE '1994-09-27'), MONTH(DATE '1994-09-27'),"
				+ "WEEK(DATE '1994-09-27'), DAYOFYEAR(DATE '1994-09-27'),"
				+ "DAYOFMONTH(DATE '1994-09-27'), DAYOFWEEK(DATE '1994-09-27'),"
				+ "HOUR(TIMESTAMP '1994-09-27 13:14:15'),"
				+ "MINUTE(TIMESTAMP '1994-09-27 13:14:15'),"
				+ "SECOND(TIMESTAMP '1994-09-27 13:14:15'),"
				+ "FLOOR(TIME '12:44:31' TO MINUTE),"
				+ "CEIL(TIME '12:44:31' TO MINUTE),"
				+ "(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '3:30:00', INTERVAL '2' HOUR),"
				+ "TIMESTAMPADD(WEEK, 1, DATE '2003-01-02'),"
				+ "TIMESTAMPDIFF(DAY, TIMESTAMP '2003-01-02 10:00:00', TIMESTAMP '2003-01-03 10:00:00')"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(25, output.getArity());
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testConditionalFunctions() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"CASE id WHEN 1 THEN -1 WHEN 2 THEN -2 END"
				+ ", CASE WHEN id=1 THEN -1 WHEN id=2 THEN -2 END"
				+ ", NULLIF(5, 5), NULLIF(5, 0)"
				+ ", COALESCE(NULL, 5)"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row expected = Row.of(-1, -1, null, 5, 5);
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(expected, output);
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testTypeConversionFunctions() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"CAST('42' AS INT), CAST(NULL AS VARCHAR)"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		try {
			Row output = selectMapper.map(Row.of(1, "'abc'"));
			assertEquals(output, Row.of(42, null));
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testCollectionFunctions() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"CARDINALITY(ARRAY[1,2,3])"
				+ ", ARRAY[1,2,3][2]"
				+ ", ELEMENT(ARRAY[2])"
				+ ", CARDINALITY(MAP[1, 2, 3, 4])"
				+ ", MAP[1, 2, 3, 4][3]"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row expected = Row.of(3, 2, 2, 2, 4);
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(expected, output);
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testValueConstructionFunctions() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"ROW(1, 2, 3), ARRAY[1, 2, 3], MAP[1, 2, 3, 4]"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(output.getArity(), 3);
		} finally {
			selectMapper.close();
		}
	}

	@Test
	public void testHashFunctions() throws Exception {
		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"id, MD5(name), SHA1(name), SHA224(name), SHA256(name), SHA384(name), SHA512(name), SHA2(name, 512)"
		);
		SelectMapper selectMapper = new SelectMapper(dataSchema, params);
		selectMapper.open();
		Row expected = Row.of(1, "e41225f8921fffcead7a35a3ddabdeeb", "ff13f5e89c51b0b9af963d080ef0899c7a169080",
			"66f30b83556e5b5b18559273e292cc64fff896dc1b9375f54c7f2b21",
			"62d9e539628b195b8df54c6b8fb6242fb0ba8da6aa793f7a482bdf723dd3edb5",
			"43b359d46d9c98d66a74be2e3ce99f9bbcc9195885af3aaf1ade323eb5eba45a51ec9b579fe0708bde6d2267a540d135",
			"3a08526868871f1d5f4efdf2f1229d65802818772a054a4a8cd272183275d53db5e40730d68af3dcdd8bfcd95bc1e97167947692e3c7b8d0dbd59cedb4aa650a",
			"3a08526868871f1d5f4efdf2f1229d65802818772a054a4a8cd272183275d53db5e40730d68af3dcdd8bfcd95bc1e97167947692e3c7b8d0dbd59cedb4aa650a");
		Row output = selectMapper.map(Row.of(1, "'abc'"));
		try {
			assertEquals(expected, output);
		} finally {
			selectMapper.close();
		}
	}

	public static <T extends Mapper> void testMultiThreadMapper(BiFunction <TableSchema, Params, T> constructor,
																TableSchema dataSchema, Params params, Row[] inputs,
																int numThreads) throws Throwable {
		int numItems = inputs.length;
		T mapper = constructor.apply(dataSchema, params);
		mapper.open();
		Row[] outputs = new Row[numItems];
		for (int i = 0; i < numItems; i += 1) {
			outputs[i] = mapper.map(inputs[i]);
		}
		mapper.close();

		T multiThreadMapper = constructor.apply(dataSchema, params);
		multiThreadMapper.open();

		ExecutorService executorService = new ThreadPoolExecutor(
			numThreads, numThreads, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue <>(numThreads));

		Future <?>[] futures = new Future[numThreads];
		for (int threadId = 0; threadId < numThreads; threadId += 1) {
			futures[threadId] = executorService.submit(() -> {
				try {
					for (int i = 0; i < numItems; i += 1) {
						Row output = multiThreadMapper.map(inputs[i]);
						assertEquals(output.toString(), outputs[i].toString());
						Thread.sleep(10);
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}
		for (int i = 0; i < numThreads; i += 1) {
			futures[i].get();
		}
		multiThreadMapper.close();
	}

	@Test
	public void testMultiThread() {
		int numItems = 100;

		Row[] inputs = new Row[numItems];
		for (int i = 0; i < numItems; i += 1) {
			inputs[i] = Row.of(i, RandomStringUtils.randomAlphanumeric(8));
		}

		TableSchema dataSchema = TableSchema.builder().fields(
			new String[] {"id", "name"},
			new DataType[] {DataTypes.INT(), DataTypes.STRING()}).build();
		Params params = new Params();
		params.set(HasClause.CLAUSE,
			"id, name as eman, id + 1 as id2, CASE WHEN id=1 THEN 'q' ELSE 'p' END as col3, UPPER(name) as col4");

		try {
			testMultiThreadMapper(SelectMapper::new, dataSchema, params, inputs, 4);
		} catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
	}
}