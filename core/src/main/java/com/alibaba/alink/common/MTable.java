package com.alibaba.alink.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
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
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvFormatter;
import com.alibaba.alink.operator.common.io.csv.CsvParser;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

@JsonSerialize(using = MTableSerializer.class)
@JsonDeserialize(using = MTableDeserializer.class)
public class MTable implements Serializable {

	private final List <Row> rows;
	private final String schemaStr;

	public MTable(String json) {
		MTable mTable = deserialize(json);

		this.rows = mTable.rows;
		this.schemaStr = mTable.schemaStr;
	}

	public MTable(List <Row> rows, String[] colNames, TypeInformation <?>[] colTypes) {
		this(rows, new TableSchema(colNames, colTypes));
	}

	public MTable(List <Row> rows, TableSchema schema) {
		this(rows, CsvUtil.schema2SchemaStr(schema));
	}

	public MTable(List <Row> rows, String schemaStr) {
		if (rows == null) {
			this.rows = new ArrayList <>();
		} else {
			this.rows = rows;
		}
		this.schemaStr = schemaStr;
	}

	public List <Row> getTable() {
		return rows;
	}

	public Object getEntry(int row, int col) {
		return rows.get(row).getField(col);
	}

	public TableSchema getTableSchema() {
		return CsvUtil.schemaStr2Schema(schemaStr);
	}

	public String[] getColNames() {
		return CsvUtil.getColNames(schemaStr);
	}

	public TypeInformation <?>[] getColTypes() {
		return CsvUtil.getColTypes(schemaStr);
	}

	public int getNumRow() {
		return rows != null ? rows.size() : -1;
	}

	public int getNumCol() {
		return getColNames() != null ? getColNames().length : -1;
	}

	public List <Row> getRows() {
		return rows;
	}

	public String getSchemaStr() {
		return schemaStr;
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

	@Override
	public String toString() {
		return JsonConverter.toJson(this);
	}

	private static MTable deserialize(String json) {
		return JsonConverter.fromJson(json, MTable.class);
	}

	static final String M_TABLE_SCHEMA_STR = "schema";
	static final String M_TABLE_DATA = "data";

	static class MTableSerializer extends JsonSerializer <MTable> implements Serializable {
		private static final Map <TypeInformation <?>, Function <Object, String>> SERIALIZERS;

		static {
			Map <TypeInformation <?>, Function <Object, String>> serializer = new HashMap <>();

			serializer.put(VectorTypes.VECTOR, Object::toString);
			serializer.put(VectorTypes.DENSE_VECTOR, Object::toString);
			serializer.put(VectorTypes.SPARSE_VECTOR, Object::toString);

			serializer.put(TensorTypes.TENSOR, Object::toString);
			serializer.put(TensorTypes.FLOAT_TENSOR, Object::toString);
			serializer.put(TensorTypes.DOUBLE_TENSOR, Object::toString);
			serializer.put(TensorTypes.BOOL_TENSOR, Object::toString);
			serializer.put(TensorTypes.INT_TENSOR, Object::toString);
			serializer.put(TensorTypes.BYTE_TENSOR, Object::toString);
			serializer.put(TensorTypes.LONG_TENSOR, Object::toString);
			serializer.put(TensorTypes.STRING_TENSOR, Object::toString);
			serializer.put(TensorTypes.UBYTE_TENSOR, Object::toString);
			serializer.put(Types.SQL_TIMESTAMP, Object::toString);
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
			final List <Row> table = value.getTable();

			final int arity = types.length;

			gen.writeStartObject();

			gen.writeFieldName(M_TABLE_DATA);

			gen.writeStartObject();
			final String[] names = value.getColNames();
			for (int i = 0; i < arity; ++i) {
				gen.writeFieldName(names[i]);
				gen.writeStartArray();
				for (Row row : table) {
					gen.writeObject(to(row.getField(i), types[i]));
				}
				gen.writeEndArray();
			}
			gen.writeEndObject();

			gen.writeFieldName(M_TABLE_SCHEMA_STR);
			gen.writeObject(CsvUtil.schema2SchemaStr(schema));

			gen.writeEndObject();
		}
	}

	static class MTableDeserializer extends JsonDeserializer <MTable> implements Serializable {

		private static final Map <TypeInformation <?>, Function <String, Object>> DESERIALIZERS;

		static {
			Map <TypeInformation <?>, Function <String, Object>> deserializers = new HashMap <>();

			deserializers.put(VectorTypes.VECTOR, VectorUtil::getVector);
			deserializers.put(VectorTypes.DENSE_VECTOR, VectorUtil::getVector);
			deserializers.put(VectorTypes.SPARSE_VECTOR, VectorUtil::getVector);

			deserializers.put(TensorTypes.TENSOR, TensorUtil::getTensor);
			deserializers.put(TensorTypes.FLOAT_TENSOR, TensorUtil::getTensor);
			deserializers.put(TensorTypes.DOUBLE_TENSOR, TensorUtil::getTensor);
			deserializers.put(TensorTypes.BOOL_TENSOR, TensorUtil::getTensor);
			deserializers.put(TensorTypes.INT_TENSOR, TensorUtil::getTensor);
			deserializers.put(TensorTypes.BYTE_TENSOR, TensorUtil::getTensor);
			deserializers.put(TensorTypes.LONG_TENSOR, TensorUtil::getTensor);
			deserializers.put(TensorTypes.STRING_TENSOR, TensorUtil::getTensor);
			deserializers.put(TensorTypes.UBYTE_TENSOR, TensorUtil::getTensor);
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

			final TableSchema schema = CsvUtil.schemaStr2Schema(schemaNode.asText());
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
		TableSchema schema = CsvUtil.schemaStr2Schema(schemaStr);
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
				throw new RuntimeException("Fail to parse line: \"" + line + "\"");
			}
		}
		return new MTable(rows, schema);
	}

	public void writeCsvToFile(BufferedWriter writer) throws IOException {
		TableSchema schema = CsvUtil.schemaStr2Schema(schemaStr);
		CsvFormatter formatter = new CsvFormatter(schema.getFieldTypes(), ",", '"');
		for (Row row : rows) {
			String line = formatter.format(row);
			writer.write(line);
			writer.newLine();
		}
	}
}
