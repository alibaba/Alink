package com.alibaba.alink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.UDFBatchOp;
import com.alibaba.alink.operator.batch.utils.UDTFBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.UDFStreamOp;
import com.alibaba.alink.operator.stream.utils.UDTFStreamOp;

import java.util.HashMap;
import java.util.Map.Entry;

public class Chap06 {

	public static void main(String[] args) throws Exception {
		BatchOperator.setParallelism(1);

		c_1_3();

		c_1_4();

		c_2_3();

		c_2_4();

	}

	public static class FromUnixTimestamp extends ScalarFunction {

		public java.sql.Timestamp eval(Long ts) {
			return new java.sql.Timestamp(ts * 1000);
		}

		@Override
		public TypeInformation <?> getResultType(Class <?>[] signature) {
			return Types.SQL_TIMESTAMP;
		}

	}

	static void c_1_3() throws Exception {
		BatchOperator <?> ratings = Chap24.getSourceRatings();

		ratings.firstN(5).print();

		ratings
			.link(
				new UDFBatchOp()
					.setFunc(new FromUnixTimestamp())
					.setSelectedCols("ts")
					.setOutputCol("ts")
			)
			.firstN(5)
			.print();

		BatchOperator.registerFunction("from_unix_timestamp", new FromUnixTimestamp());

		ratings
			.select("user_id, item_id, rating, from_unix_timestamp(ts) AS ts")
			.firstN(5)
			.print();

		ratings.registerTableName("ratings");

		BatchOperator
			.sqlQuery("SELECT user_id, item_id, rating, from_unix_timestamp(ts) AS ts FROM ratings")
			.firstN(5)
			.print();

	}

	static void c_1_4() throws Exception {

		StreamOperator <?> ratings = Chap24.getStreamSourceRatings();

		ratings = ratings.filter("user_id=1 AND item_id<5");

		ratings.print();

		StreamOperator.execute();

		ratings
			.link(
				new UDFStreamOp()
					.setFunc(new FromUnixTimestamp())
					.setSelectedCols("ts")
					.setOutputCol("ts")
			)
			.print();

		StreamOperator.execute();

		StreamOperator.registerFunction("from_unix_timestamp", new FromUnixTimestamp());

		ratings
			.select("user_id, item_id, rating, from_unix_timestamp(ts) AS ts")
			.print();

		StreamOperator.execute();

		ratings.registerTableName("ratings");

		StreamOperator
			.sqlQuery("SELECT user_id, item_id, rating, from_unix_timestamp(ts) AS ts FROM ratings")
			.print();

		StreamOperator.execute();

	}

	public static class WordCount extends TableFunction <Row> {

		private HashMap <String, Integer> map = new HashMap <>();

		public void eval(String str) {
			if (null == str || str.isEmpty()) {
				return;
			}
			for (String s : str.split(" ")) {
				if (map.containsKey(s)) {
					map.put(s, 1 + map.get(s));
				} else {
					map.put(s, 1);
				}
			}
			for (Entry <String, Integer> entry : map.entrySet()) {
				collect(Row.of(entry.getKey(), entry.getValue()));
			}
			map.clear();
		}

		@Override
		public TypeInformation <Row> getResultType() {
			return Types.ROW(Types.STRING, Types.INT);
		}
	}

	public static void c_2_3() throws Exception {

		BatchOperator items = Chap24.getSourceItems();

		items.select("item_id, title").lazyPrint(10, "<- original data ->");

		BatchOperator <?> words = items.link(
			new UDTFBatchOp()
				.setFunc(new WordCount())
				.setSelectedCols("title")
				.setOutputCols("word", "cnt")
				.setReservedCols("item_id")
		);

		words.lazyPrint(20, "<- after word count ->");

		words.groupBy("word", "word, SUM(cnt) AS cnt")
			.orderBy("cnt", 20, false)
			.print();

		BatchOperator.registerFunction("word_count", new WordCount());

		items.registerTableName("items");

		BatchOperator
			.sqlQuery("SELECT item_id, word, cnt FROM items, "
				+ "LATERAL TABLE(word_count(title)) as T(word, cnt)")
			.firstN(20)
			.print();

	}

	public static void c_2_4() throws Exception {

		StreamOperator items = Chap24.getStreamSourceItems();

		items = items.select("item_id, title").filter("item_id<4");

		items.print();
		StreamOperator.execute();

		StreamOperator <?> words = items.link(
			new UDTFStreamOp()
				.setFunc(new WordCount())
				.setSelectedCols("title")
				.setOutputCols("word", "cnt")
				.setReservedCols("item_id")
		);

		words.print();
		StreamOperator.execute();

		StreamOperator.registerFunction("word_count", new WordCount());

		items.registerTableName("items");

		StreamOperator.sqlQuery("SELECT item_id, word, cnt FROM items, "
			+ "LATERAL TABLE(word_count(title)) as T(word, cnt)")
			.print();

		StreamOperator.execute();

	}

}