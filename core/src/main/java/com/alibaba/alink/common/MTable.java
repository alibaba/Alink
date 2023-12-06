package com.alibaba.alink.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable.MTableDeserializer;
import com.alibaba.alink.common.MTable.MTableSerializer;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.MTableSerializerException;
import com.alibaba.alink.common.io.filesystem.binary.BaseStreamRowSerializer;
import com.alibaba.alink.common.io.filesystem.binary.RowStreamSerializer;
import com.alibaba.alink.common.io.filesystem.binary.RowStreamSerializerV2;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.viz.DataTypeDisplayInterface;
import com.alibaba.alink.operator.common.io.csv.CsvFormatter;
import com.alibaba.alink.operator.common.io.csv.CsvParser;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@JsonSerialize(using = MTableSerializer.class)
@JsonDeserialize(using = MTableDeserializer.class)
public class MTable implements Serializable, DataTypeDisplayInterface {

	private final List <Row> rows;
	private final String schemaStr;
	private final int displaySize = 5;

	public MTable(Object[] vals, String colName) {
		rows = new ArrayList <>(vals.length);
		for (Object val : vals) {
			rows.add(Row.of(val));
		}
		this.schemaStr = initSchemaStr(this.rows, new String[] {colName});
	}

	public MTable(Object[][] vals, String[] colNames) {
		rows = new ArrayList <>(vals.length);
		for (Object[] val : vals) {
			rows.add(Row.of(val));
		}
		this.schemaStr = initSchemaStr(this.rows, colNames);
	}

	public MTable(List <Row> rows, String[] colNames, TypeInformation <?>[] colTypes) {
		this(rows, new TableSchema(colNames, colTypes));
	}

	public MTable(List <Row> rows, TableSchema schema) {
		this(rows, TableUtil.schema2SchemaStr(schema));
	}

	public MTable(List <Row> rows, String schemaStr) {
		this.rows = rows;
		this.schemaStr = schemaStr;
	}

	public MTable(List <Row> rows, String[] colNames) {
		this(rows, initSchemaStr(rows, colNames));
	}

	public MTable(Row[] rows, String[] colNames, TypeInformation <?>[] colTypes) {
		this(Arrays.asList(rows), colNames, colTypes);
	}

	public MTable(Row[] rows, TableSchema schema) {
		this(Arrays.asList(rows), schema);
	}

	public MTable(Row[] rows, String schemaStr) {
		this(Arrays.asList(rows), schemaStr);
	}

	public MTable(Row[] rows, String[] colNames) {
		this(Arrays.asList(rows), colNames);
	}

	private static String initSchemaStr(List <Row> rows, String[] colNames) {
		if (rows == null || rows.size() < 1) {
			throw new AkIllegalArgumentException("Values can not be empty.");
		}

		int arity = rows.get(0).getArity();

		TypeInformation <?>[] types = new TypeInformation[arity];

		for (int col = 0; col < arity; ++col) {
			types[col] = AlinkTypes.STRING;
			for (int i = 0; i < rows.size(); i++) {
				if (null != rows.get(i).getField(col)) {
					types[col] = TypeExtractor.getForObject(rows.get(i).getField(col));
					break;
				}
			}
		}

		return TableUtil.schema2SchemaStr(new TableSchema(colNames, types));
	}

	public List <Row> getRows() {
		return rows;
	}

	public Row getRow(int index) {
		return rows.get(index);
	}

	public TableSchema getSchema() {
		return TableUtil.schemaStr2Schema(this.schemaStr);
	}

	public String[] getColNames() {
		return TableUtil.getColNames(this.schemaStr);
	}

	public TypeInformation <?>[] getColTypes() {
		return TableUtil.getColTypes(this.schemaStr);
	}

	public int getNumRow() {
		return rows != null ? rows.size() : -1;
	}

	public int getNumCol() {
		return getColNames().length;
	}

	public String getSchemaStr() {
		return schemaStr;
	}

	public Object getEntry(int row, int col) {
		return rows.get(row).getField(col);
	}

	public void setEntry(int row, int col, Object val) {
		rows.get(row).setField(col, val);
	}

	public MTable copy() {
		return MTableUtil.copy(this);
	}

	public MTable select(String... colNames) {
		return MTableUtil.select(this, colNames);
	}

	public MTable select(int... colIndexes) {
		return MTableUtil.select(this, colIndexes);
	}

	/**
	 * summary for MTable.
	 */
	public TableSummary summary(String... selectedColNames) {
		if (null == selectedColNames || 0 == selectedColNames.length) {
			return subSummary(null, 0, this.getNumRow());
		} else {
			return subSummary(selectedColNames, 0, this.getNumRow());
		}
	}

	//summary for data from fromId line to endId line, include fromId and exclude endId.
	public TableSummary subSummary(String[] selectedColNames, int fromId, int endId) {
		TableSummarizer srt = new TableSummarizer(getSchema(), false, selectedColNames);
		for (int i = Math.max(fromId, 0); i < Math.min(endId, this.getNumRow()); i++) {
			srt.visit(this.rows.get(i));
		}
		return srt.toSummary();
		//if (null == selectedColNames || 0 == selectedColNames.length) {
		//	TableSummarizer srt = new TableSummarizer(getSchema(), false);
		//	for (int i = Math.max(fromId, 0); i < Math.min(endId, this.getNumRow()); i++) {
		//		srt.visit(this.rows.get(i));
		//	}
		//	return srt.toSummary();
		//} else {
		//	TableSchema schema = new TableSchema(selectedColNames,
		//		TableUtil.findColTypes(getSchema(), selectedColNames));
		//	int[] selectColIndices = TableUtil.findColIndices(getSchema(), selectedColNames);
		//	TableSummarizer srt = new TableSummarizer(schema, false);
		//	for (int i = Math.max(fromId, 0); i < Math.min(endId, this.getNumRow()); i++) {
		//		srt.visit(Row.project(this.rows.get(i), selectColIndices));
		//	}
		//	return srt.toSummary();
		//}
	}

	/**
	 * Sampling without replacement.
	 */
	public MTable sampleWithSize(int numSamples, Random rnd) {
		PriorityQueue <Tuple2 <Double, Row>> q = new PriorityQueue <>(
			numSamples,
			Comparator.comparing(o -> o.f0)
		);

		for (Row row : rows) {
			if (q.size() < numSamples) {
				q.offer(Tuple2.of(rnd.nextDouble(), row));
			} else {
				Double rand = rnd.nextDouble();

				if (rand > q.element().f0) {
					q.poll();
					q.offer(Tuple2.of(rand, row));
				}
			}
		}

		return new MTable(q.stream().map(item -> item.f1).collect(Collectors.toList()), schemaStr);
	}

	public MTable sampleWithSizeReplacement(int numSamples, Random rnd) {
		if (rows.size() == 0) {
			return new MTable(new ArrayList <Row>(), schemaStr);
		} else {
			List <Row> chosenRows = IntStream.range(0, numSamples)
				.map(d -> rnd.nextInt(rows.size()))
				.mapToObj(rows::get)
				.collect(Collectors.toList());
			return new MTable(chosenRows, schemaStr);
		}
	}

	/**
	 * Ascending order
	 */
	public void orderBy(String... fields) {
		boolean[] orders = new boolean[fields.length];
		Arrays.fill(orders, true);
		orderBy(fields, orders);
	}

	/**
	 * Ascending if order is true, otherwise descending.
	 */
	public void orderBy(String[] fields, boolean[] orders) {
		orderBy(TableUtil.findColIndicesWithAssertAndHint(getColNames(), fields), orders);
	}

	/**
	 * Ascending order
	 */
	public void orderBy(int... fields) {
		boolean[] orders = new boolean[fields.length];
		Arrays.fill(orders, true);
		orderBy(fields, orders);
	}

	/**
	 * Ascending if order is true, otherwise descending.
	 */
	public void orderBy(int[] fields, boolean[] orders) {

		final TypeComparator <Row> comparator = new RowTypeInfo(getColTypes(), getColNames())
			.createComparator(fields, orders, 0, new ExecutionConfig());

		rows.sort(comparator::compare);
	}

	public void reduceGroup(int[] fields, Consumer <List <Row>> consumer) {

		boolean[] orders = new boolean[fields.length];
		Arrays.fill(orders, true);

		reduceGroup(fields, orders, consumer);
	}

	public void reduceGroup(int[] fields, boolean[] orders, Consumer <List <Row>> consumer) {

		if (rows == null || rows.isEmpty()) {
			return;
		}

		orderBy(fields, orders);

		final TypeComparator <Row> comparator = new RowTypeInfo(getColTypes(), getColNames())
			.createComparator(fields, orders, 0, new ExecutionConfig());

		int cursor = 1;
		int latest = 0;

		while (cursor < rows.size()) {
			if (comparator.compare(rows.get(latest), rows.get(cursor)) != 0) {
				consumer.accept(rows.subList(latest, cursor));

				latest = cursor;
			}

			cursor++;
		}

		if (latest != cursor) {
			consumer.accept(rows.subList(latest, cursor));
		}
	}

	public MTable subTable(int fromIndex, int toIndex) {
		return new MTable(rows.subList(fromIndex, toIndex), schemaStr);
	}

	@Override
	public String toDisplaySummary() {
		return toDisplaySummary(displaySize);
	}

	public String toDisplaySummary(int n) {
		StringBuilder summary = new StringBuilder("MTable(");
		summary.append(getNumRow()).append(",").append(getColNames().length).append(")(");
		int maxSize = Math.min(n, getColNames().length);
		for (int i = 0; i < maxSize - 1; ++i) {
			summary.append(getColNames()[i]).append(",");
		}
		if (maxSize == getColNames().length) {
			summary.append(getColNames()[getColNames().length - 1]).append(")");
		} else {
			summary.append("...)");
		}
		return summary.toString();
	}

	@Override
	public String toDisplayData(int n) {
		StringBuilder sbd = new StringBuilder();
		if (n == Integer.MAX_VALUE) {
			sbd.append(toDisplaySummary(n)).append("\n");
		}
		if (n < 0) {
			n = Integer.MAX_VALUE;
		}
		int i = 0;
		for (Row row : this.rows) {
			if (i < n) {
				if (row.getArity() <= n) {
					sbd.append(TableUtil.formatRows(row)).append("\n");
				} else {
					Row simpleRow = new Row(n);
					for (int j = 0; j < n - 1; ++j) {
						simpleRow.setField(j, row.getField(j));
					}
					simpleRow.setField(n - 1, "...");
					sbd.append(TableUtil.formatRows(simpleRow)).append("\n");
				}
				i++;
			} else {
				break;
			}
		}
		return sbd.toString();
	}

	@Override
	public String toShortDisplayData() {
		return toDisplayData(displaySize);
	}

	@Override
	public String toString() {
		return toDisplaySummary() + "\n" + toShortDisplayData();
	}

	public String toJson() {
		return JsonConverter.toJson(this);
	}

	public static MTable fromJson(String json) {
		return JsonConverter.fromJson(json, MTable.class);
	}

	static final String M_TABLE_SCHEMA_STR = "schema";
	static final String M_TABLE_DATA = "data";

	static class MTableSerializer extends JsonSerializer <MTable> implements Serializable {
		private static final Map <TypeInformation <?>, Function <Object, String>> SERIALIZERS;

		static {
			Map <TypeInformation <?>, Function <Object, String>> serializer = new HashMap <>();

			serializer.put(AlinkTypes.VECTOR, VectorUtil::serialize);
			serializer.put(AlinkTypes.DENSE_VECTOR, VectorUtil::serialize);
			serializer.put(AlinkTypes.SPARSE_VECTOR, VectorUtil::serialize);

			serializer.put(AlinkTypes.TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.FLOAT_TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.DOUBLE_TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.BOOL_TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.INT_TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.BYTE_TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.LONG_TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.STRING_TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.UBYTE_TENSOR, TensorUtil::serialize);
			serializer.put(AlinkTypes.SQL_TIMESTAMP, Object::toString);
			SERIALIZERS = Collections.unmodifiableMap(serializer);
		}

		private static Object to(Object obj, TypeInformation <?> type) {

			Function <Object, String> function = SERIALIZERS.get(type);

			if (function != null) {
				if (obj == null) {
					return null;
				}
				return function.apply(obj);
			}

			return obj;
		}

		@Override
		public void serialize(MTable value, JsonGenerator gen, SerializerProvider serializers)
			throws IOException, JsonProcessingException {

			final TableSchema schema = new TableSchema(value.getColNames(), value.getColTypes());
			final TypeInformation <?>[] types = value.getColTypes();
			final List <Row> rows = value.getRows();

			final int arity = types.length;

			gen.writeStartObject();

			gen.writeFieldName(M_TABLE_DATA);

			gen.writeStartObject();
			final String[] names = value.getColNames();
			for (int i = 0; i < arity; ++i) {
				gen.writeFieldName(names[i]);
				gen.writeStartArray();
				for (Row row : rows) {
					gen.writeObject(to(row.getField(i), types[i]));
				}
				gen.writeEndArray();
			}
			gen.writeEndObject();

			gen.writeFieldName(M_TABLE_SCHEMA_STR);
			gen.writeObject(TableUtil.schema2SchemaStr(schema));

			gen.writeEndObject();
		}
	}

	static class MTableDeserializer extends JsonDeserializer <MTable> implements Serializable {

		private static final Map <TypeInformation <?>, Function <String, Object>> DESERIALIZERS;

		static {
			Map <TypeInformation <?>, Function <String, Object>> deserializers = new HashMap <>();

			deserializers.put(AlinkTypes.VECTOR, VectorUtil::getVector);
			deserializers.put(AlinkTypes.DENSE_VECTOR, VectorUtil::getVector);
			deserializers.put(AlinkTypes.SPARSE_VECTOR, VectorUtil::getVector);

			deserializers.put(AlinkTypes.TENSOR, TensorUtil::getTensor);
			deserializers.put(AlinkTypes.FLOAT_TENSOR, TensorUtil::getTensor);
			deserializers.put(AlinkTypes.DOUBLE_TENSOR, TensorUtil::getTensor);
			deserializers.put(AlinkTypes.BOOL_TENSOR, TensorUtil::getTensor);
			deserializers.put(AlinkTypes.INT_TENSOR, TensorUtil::getTensor);
			deserializers.put(AlinkTypes.BYTE_TENSOR, TensorUtil::getTensor);
			deserializers.put(AlinkTypes.LONG_TENSOR, TensorUtil::getTensor);
			deserializers.put(AlinkTypes.STRING_TENSOR, TensorUtil::getTensor);
			deserializers.put(AlinkTypes.UBYTE_TENSOR, TensorUtil::getTensor);
			deserializers.put(Types.SQL_TIMESTAMP, Timestamp::valueOf);

			DESERIALIZERS = Collections.unmodifiableMap(deserializers);
		}

		private static Object from(
			ObjectCodec codec,
			JsonNode jsonNode,
			TypeInformation <?> typeInformation) throws JsonProcessingException {

			final Function <String, Object> function = DESERIALIZERS.get(typeInformation);

			if (function != null) {
				if (jsonNode.isNull()) {
					return null;
				}
				return function.apply(jsonNode.asText());
			}

			return codec.treeToValue(jsonNode, typeInformation.getTypeClass());
		}

		@Override
		public MTable deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
			throws IOException, JsonProcessingException {

			final ObjectCodec codec = jsonParser.getCodec();
			final JsonNode node = codec.readTree(jsonParser);
			final JsonNode schemaNode = node.get(M_TABLE_SCHEMA_STR);
			final JsonNode dataNode = node.get(M_TABLE_DATA);

			final TableSchema schema = TableUtil.schemaStr2Schema(schemaNode.asText());
			final String[] names = schema.getFieldNames();
			final TypeInformation <?>[] types = schema.getFieldTypes();

			final int arity = names.length;

			final List <Row> data = new ArrayList <>();

			for (int j = 0; j < names.length; ++j) {
				final Iterator <JsonNode> objectIterator = dataNode.get(names[j]).elements();
				int index = 0;
				while (objectIterator.hasNext()) {
					JsonNode item = objectIterator.next();
					if (j == 0) {
						Row row = new Row(arity);
						row.setField(j, from(codec, item, types[j]));
						data.add(index, row);
					} else {
						data.get(index).setField(j, from(codec, item, types[j]));
					}
					index++;
				}
			}
			return new MTable(data, schema);
		}
	}

	public static MTable readCsvFromFile(BufferedReader reader, String schemaStr) throws IOException {
		TableSchema schema = TableUtil.schemaStr2Schema(schemaStr);
		CsvParser parser = new CsvParser(schema.getFieldTypes(), ",", '"');
		List <Row> rows = new ArrayList <>();
		while (true) {
			String line = reader.readLine();
			if (null == line) {
				break;
			}
			Tuple2 <Boolean, Row> tuple2 = parser.parse(line);
			if (tuple2.f0) {
				rows.add(tuple2.f1);
			} else {
				throw new AkParseErrorException("Fail to parse line: \"" + line + "\"");
			}
		}
		return new MTable(rows, schema);
	}

	public void writeCsvToFile(BufferedWriter writer) throws IOException {
		TableSchema schema = getSchema();
		CsvFormatter formatter = new CsvFormatter(schema.getFieldTypes(), ",", '"');
		for (Row row : rows) {
			String line = formatter.format(row);
			writer.write(line);
			writer.newLine();
		}
	}

	public static class MTableKryoSerializer extends Serializer <MTable> implements Serializable {

		@Override
		public void write(Kryo kryo, Output output, MTable mTable) {
			mTableKyroSerializerWrite(kryo, output, mTable, new RowStreamSerializerFactoryV1());
		}

		@Override
		public MTable read(Kryo kryo, Input input, Class <MTable> type) {
			return mTableKyroSerializerRead(kryo, input, type, new RowStreamSerializerFactoryV1());
		}
	}

	public static class MTableKryoSerializerV2 extends Serializer <MTable> implements Serializable {

		@Override
		public void write(Kryo kryo, Output output, MTable mTable) {
			mTableKyroSerializerWrite(kryo, output, mTable, new RowStreamSerializerFactoryV2());
		}

		@Override
		public MTable read(Kryo kryo, Input input, Class <MTable> type) {
			return mTableKyroSerializerRead(kryo, input, type, new RowStreamSerializerFactoryV2());
		}
	}

	private static void mTableKyroSerializerWrite(Kryo kryo, Output output, MTable mTable,
												  RowStreamSerializerFactory factory) {
		String schemaStr = mTable.getSchemaStr();
		List <Row> data = mTable.getRows();

		// null mask, 2-bits
		byte nullMask = 0x0;

		if (schemaStr == null) {
			nullMask |= 0x1;
		}

		if (data == null) {
			nullMask |= 0x2;
		}

		output.writeByte(nullMask);

		// schema str is null and data is null
		if (schemaStr == null && data == null) {
			output.flush();
			return;
		}

		// schema str is null and data is not null
		if (schemaStr == null) {
			kryo.writeClassAndObject(output, data);
			output.flush();
			return;
		}

		// schema str is not null and data is null
		if (data == null) {
			byte[] schemaStrBytes = schemaStr.getBytes(StandardCharsets.UTF_8);

			int schemaLen = schemaStrBytes.length;

			output.writeInt(schemaLen);

			output.write(schemaStrBytes);

			output.flush();

			return;
		}

		// schema str is not null and data is not null
		TableSchema tableSchema = mTable.getSchema();

		byte[] schemaStrBytes = schemaStr.getBytes(StandardCharsets.UTF_8);
		int schemaLen = schemaStrBytes.length;
		int dataLen = data.size();

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

		try (GZIPOutputStream gzOut = new GZIPOutputStream(byteArrayOutputStream)) {

			// write the length of schema.
			writeInt(schemaLen, gzOut);

			// write schema
			gzOut.write(schemaStrBytes);

			// write the length of data.
			writeInt(dataLen, gzOut);

			// write rows
			BaseStreamRowSerializer rowSerializer = factory.create(
				tableSchema.getFieldNames(), tableSchema.getFieldTypes(), null, gzOut
			);

			for (Row row : data) {
				rowSerializer.serialize(row);
			}

		} catch (IOException e) {
			throw new MTableSerializerException("Write gzip output stream in MTable serializer error.", e);
		}

		output.writeInt(byteArrayOutputStream.size());

		try {
			byteArrayOutputStream.writeTo(output);
		} catch (IOException e) {
			throw new MTableSerializerException("Write data to output stream in MTable serializer error.", e);
		}

		output.flush();
	}

	public interface RowStreamSerializerFactory {
		BaseStreamRowSerializer create(
			String[] fieldNames, TypeInformation <?>[] fieldTypes,
			InputStream boundInputStream, OutputStream boundOutputStream
		);
	}

	public static class RowStreamSerializerFactoryV1 implements RowStreamSerializerFactory {

		@Override
		public BaseStreamRowSerializer create(String[] fieldNames, TypeInformation <?>[] fieldTypes,
											  InputStream boundInputStream, OutputStream boundOutputStream) {
			return new RowStreamSerializer(fieldNames, fieldTypes, boundInputStream, boundOutputStream);
		}
	}

	public static class RowStreamSerializerFactoryV2 implements RowStreamSerializerFactory {

		@Override
		public BaseStreamRowSerializer create(String[] fieldNames, TypeInformation <?>[] fieldTypes,
											  InputStream boundInputStream, OutputStream boundOutputStream) {
			return new RowStreamSerializerV2(fieldNames, fieldTypes, boundInputStream, boundOutputStream);
		}
	}

	private static MTable mTableKyroSerializerRead(Kryo kryo, Input input, Class <MTable> type,
												   RowStreamSerializerFactory factory) {

		// read mask
		byte nullMask = input.readByte();

		boolean schemaIsNull = (nullMask & 0x1) > 0;
		boolean dataIsNull = (nullMask & 0x2) > 0;

		// schema is null and data is null
		if (schemaIsNull && dataIsNull) {
			return new MTable((List <Row>) null, (String) null);
		}

		// schema is null and data is not null
		if (schemaIsNull) {
			return new MTable((List <Row>) kryo.readClassAndObject(input), (String) null);
		}

		// schema is not null and data is null
		if (dataIsNull) {
			byte[] buffer = new byte[input.readInt()];
			try {
				IOUtils.readFully(input, buffer, 0, buffer.length);
			} catch (IOException e) {
				throw new MTableSerializerException("Read data from input stream in MTable serializer error.", e);
			}

			return new MTable((List <Row>) null, new String(buffer, StandardCharsets.UTF_8));
		}

		int bufferLen = input.readInt();

		byte[] buffer = new byte[bufferLen];

		try {
			IOUtils.readFully(input, buffer, 0, bufferLen);
		} catch (IOException e) {
			throw new MTableSerializerException("Read data from input stream in MTable serializer error.", e);
		}

		try (GZIPInputStream gzIn = new GZIPInputStream(new ByteArrayInputStream(buffer))) {

			// read the length of schema.
			int schemaLen = readInt(gzIn);

			// read schema
			byte[] strBytes = new byte[schemaLen];

			IOUtils.readFully(gzIn, strBytes, 0, schemaLen);

			String schemaStr = new String(strBytes, 0, schemaLen, StandardCharsets.UTF_8);

			TableSchema schema = TableUtil.schemaStr2Schema(schemaStr);

			// read the length of data
			int dataLen = readInt(gzIn);

			// read rows
			BaseStreamRowSerializer rowSerializer = factory.create(
				schema.getFieldNames(), schema.getFieldTypes(), gzIn, null
			);

			ArrayList <Row> data = new ArrayList <>(dataLen);

			for (int i = 0; i < dataLen; ++i) {
				data.add(rowSerializer.deserialize());
			}

			return new MTable(data, schemaStr);
		} catch (IOException e) {
			throw new MTableSerializerException("Read gzip input stream in MTable serializer error.", e);
		}
	}

	private static void writeInt(int value, OutputStream outputStream) throws IOException {
		outputStream.write((value >> 24) & 0xFF);
		outputStream.write((value >> 16) & 0xFF);
		outputStream.write((value >> 8) & 0xFF);
		outputStream.write((value) & 0xFF);
	}

	private static int readInt(InputStream inputStream) throws IOException {
		return (inputStream.read() & 0xFF) << 24
			| (inputStream.read() & 0xFF) << 16
			| (inputStream.read() & 0xFF) << 8
			| (inputStream.read() & 0xFF);
	}
}
