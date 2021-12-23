package com.alibaba.alink;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.LdaPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.LdaTrainBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.batch.nlp.DocWordCountBatchOp;
import com.alibaba.alink.operator.batch.nlp.KeywordsExtractionBatchOp;
import com.alibaba.alink.operator.batch.nlp.RegexTokenizerBatchOp;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp;
import com.alibaba.alink.operator.batch.nlp.TokenizerBatchOp;
import com.alibaba.alink.operator.batch.nlp.WordCountBatchOp;
import com.alibaba.alink.operator.batch.similarity.StringSimilarityPairwiseBatchOp;
import com.alibaba.alink.operator.batch.similarity.TextSimilarityPairwiseBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.nlp.Method;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.similarity.StringSimilarityPairwiseStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.nlp.DocCountVectorizer;
import com.alibaba.alink.pipeline.nlp.DocHashCountVectorizer;
import com.alibaba.alink.pipeline.nlp.Segment;
import com.alibaba.alink.pipeline.similarity.StringApproxNearestNeighbor;
import com.alibaba.alink.pipeline.similarity.StringNearestNeighbor;
import com.alibaba.alink.pipeline.similarity.TextApproxNearestNeighbor;
import com.alibaba.alink.pipeline.similarity.TextNearestNeighbor;

import java.io.File;

public class Chap21 {

	static final String DATA_DIR = Utils.ROOT_DIR + "news_toutiao" + File.separator;

	static final String ORIGIN_TRAIN_FILE = "toutiao_cat_data.txt";
	static final String FIELD_DELIMITER = "_!_";

	static final String SNN_MODEL_FILE = "snn_model.ak";
	static final String APPROX_SNN_MODEL_FILE = "approx_snn_model.ak";
	static final String LDA_MODEL_FILE = "lda_model.ak";
	static final String LDA_PWZ_FILE = "lda_pwz.ak";

	static final String SCHEMA_STRING =
		"id string, category_code int, category_name string, news_title string, keywords string";

	private static final String TXT_COL_NAME = "news_title";
	private static final String LABEL_COL_NAME = "category_name";
	private static final String PREDICTION_COL_NAME = "pred";

	public static void main(String[] args) throws Exception {

		BatchOperator.setParallelism(1);

		c_1();

		c_2_1();

		c_2_2();

		c_3();

		c_4();

		c_5_2();

		c_6_1();

		c_6_2();

		c_7();

	}

	static void c_1() throws Exception {
		BatchOperator.setParallelism(1);

		getSource()
			.lazyPrint(10)
			.lazyPrintStatistics();

		getSource()
			.groupBy("category_code, category_name", "category_code, category_name, COUNT(category_name) AS cnt")
			.orderBy("category_code", 100)
			.lazyPrint(-1);

		BatchOperator.execute();
	}

	static void c_2_1() throws Exception {
		BatchOperator.setParallelism(1);

		String[] strings = new String[] {
			"大家好！我在学习、使用Alink。",
			"【流式计算和批式计算】、(Alink)",
			"《人工智能》，“机器学习”？2020"
		};

		MemSourceBatchOp source = new MemSourceBatchOp(strings, "sentence");

		source.link(
			new SegmentBatchOp()
				.setSelectedCol("sentence")
				.setOutputCol("words")
		).print();

		source.link(
			new SegmentBatchOp()
				.setSelectedCol("sentence")
				.setOutputCol("words")
				.setUserDefinedDict("流式计算", "机器学习")
		).print();

		source.link(
			new SegmentBatchOp()
				.setSelectedCol("sentence")
				.setOutputCol("words")
				.setUserDefinedDict("流式计算", "机器学习")
		).link(
			new StopWordsRemoverBatchOp()
				.setSelectedCol("words")
				.setOutputCol("left_words")
		).print();

		source.link(
			new SegmentBatchOp()
				.setSelectedCol("sentence")
				.setOutputCol("words")
				.setUserDefinedDict("流式计算", "机器学习")
		).link(
			new StopWordsRemoverBatchOp()
				.setSelectedCol("words")
				.setOutputCol("left_words")
				.setStopWords("计算", "2020")
		).print();

		getSource()
			.select("news_title")
			.link(
				new SegmentBatchOp()
					.setSelectedCol("news_title")
					.setOutputCol("segmented_title")
			)
			.firstN(10)
			.print();

	}

	static void c_2_2() throws Exception {
		String[] strings = new String[] {
			"Hello!      This is Alink!",
			"Flink,Alink..AI#ML@2020"
		};

		MemSourceBatchOp source = new MemSourceBatchOp(strings, "sentence");

		source
			.link(
				new TokenizerBatchOp()
					.setSelectedCol("sentence")
					.setOutputCol("tokens")
			)
			.link(
				new RegexTokenizerBatchOp()
					.setSelectedCol("sentence")
					.setOutputCol("regex_tokens")
			).lazyPrint(-1);

		source
			.link(
				new RegexTokenizerBatchOp()
					.setSelectedCol("sentence")
					.setOutputCol("tokens_1")
					.setPattern("\\W+")
			)
			.link(
				new RegexTokenizerBatchOp()
					.setSelectedCol("sentence")
					.setOutputCol("tokens_2")
					.setGaps(false)
					.setPattern("\\w+")
			).lazyPrint(-1);

		source
			.link(
				new RegexTokenizerBatchOp()
					.setSelectedCol("sentence")
					.setOutputCol("tokens_1")
					.setPattern("\\W+")
			)
			.link(
				new RegexTokenizerBatchOp()
					.setSelectedCol("sentence")
					.setOutputCol("tokens_2")
					.setPattern("\\W+")
					.setToLowerCase(false)
			).lazyPrint(-1);

		source
			.link(
				new RegexTokenizerBatchOp()
					.setSelectedCol("sentence")
					.setOutputCol("tokens")
					.setPattern("\\W+")
			)
			.link(
				new StopWordsRemoverBatchOp()
					.setSelectedCol("tokens")
					.setOutputCol("left_tokens")
			).lazyPrint(-1);

		BatchOperator.execute();

	}

	static void c_3() throws Exception {
		BatchOperator.setParallelism(1);

		BatchOperator titles = getSource()
			.firstN(10)
			.select("news_title")
			.link(
				new SegmentBatchOp()
					.setSelectedCol("news_title")
					.setOutputCol("segmented_title")
					.setReservedCols(new String[] {})
			);

		titles
			.link(
				new WordCountBatchOp()
					.setSelectedCol("segmented_title")
			)
			.orderBy("cnt", 100, false)
			.lazyPrint(-1, "WordCount");

		titles
			.link(
				new DocWordCountBatchOp()
					.setDocIdCol("segmented_title")
					.setContentCol("segmented_title")
			)
			.lazyPrint(-1, "DocWordCount");

		BatchOperator.execute();
	}

	static void c_4() throws Exception {
		BatchOperator.setParallelism(1);

		BatchOperator titles = getSource()
			.firstN(10)
			.select("news_title")
			.link(
				new SegmentBatchOp()
					.setSelectedCol("news_title")
					.setOutputCol("segmented_title")
					.setReservedCols(new String[] {})
			);

		for (String featureType : new String[] {"WORD_COUNT", "BINARY", "TF", "IDF", "TF_IDF"}) {
			new DocCountVectorizer()
				.setFeatureType(featureType)
				.setSelectedCol("segmented_title")
				.setOutputCol("vec")
				.fit(titles)
				.transform(titles)
				.lazyPrint(-1, "DocCountVectorizer + " + featureType);
		}

		for (String featureType : new String[] {"WORD_COUNT", "BINARY", "TF", "IDF", "TF_IDF"}) {
			new DocHashCountVectorizer()
				.setFeatureType(featureType)
				.setSelectedCol("segmented_title")
				.setOutputCol("vec")
				.setNumFeatures(100)
				.fit(titles)
				.transform(titles)
				.lazyPrint(-1, "DocHashCountVectorizer + " + featureType);
		}

		BatchOperator.execute();

	}

	static void c_5_2() throws Exception {
		BatchOperator.setParallelism(1);

		String[] strings = new String[] {
			"蒸羊羔、蒸熊掌、蒸鹿尾儿、烧花鸭、烧雏鸡、烧子鹅、卤猪、卤鸭、酱鸡、腊肉、松花小肚儿、晾肉、香肠儿、什锦苏盘、熏鸡白肚儿、清蒸八宝猪、江米酿鸭子、罐儿野鸡、罐儿鹌鹑。"
				+ "卤什件儿、卤子鹅、山鸡、兔脯、菜蟒、银鱼、清蒸哈什蚂、烩鸭丝、烩鸭腰、烩鸭条、清拌鸭丝、黄心管儿、焖白鳝、焖黄鳝、豆豉鲇鱼、锅烧鲤鱼、烀烂甲鱼、抓炒鲤鱼、抓炒对儿虾。"
				+ "软炸里脊、软炸鸡、什锦套肠儿、卤煮寒鸦儿、麻酥油卷儿、熘鲜蘑、熘鱼脯、熘鱼肚、熘鱼片儿、醋熘肉片儿、烩三鲜、烩白蘑、烩鸽子蛋、炒银丝、烩鳗鱼、炒白虾、炝青蛤、炒面鱼。"
				+ "炒竹笋、芙蓉燕菜、炒虾仁儿、烩虾仁儿、烩腰花儿、烩海参、炒蹄筋儿、锅烧海参、锅烧白菜、炸木耳、炒肝尖儿、桂花翅子、清蒸翅子、炸飞禽、炸汁儿、炸排骨、清蒸江瑶柱。"
				+ "糖熘芡仁米、拌鸡丝、拌肚丝、什锦豆腐、什锦丁儿、糟鸭、糟熘鱼片儿、熘蟹肉、炒蟹肉、烩蟹肉、清拌蟹肉、蒸南瓜、酿倭瓜、炒丝瓜、酿冬瓜、烟鸭掌儿、焖鸭掌儿、焖笋、炝茭白。"
				+ "茄子晒炉肉、鸭羹、蟹肉羹、鸡血汤、三鲜木樨汤、红丸子、白丸子、南煎丸子、四喜丸子、三鲜丸子、氽丸子、鲜虾丸子、鱼脯丸子、饹炸丸子、豆腐丸子、樱桃肉、马牙肉、米粉肉。"
				+ "一品肉、栗子肉、坛子肉、红焖肉、黄焖肉、酱豆腐肉、晒炉肉、炖肉、黏糊肉、烀肉、扣肉、松肉、罐儿肉、烧肉、大肉、烤肉、白肉、红肘子、白肘子、熏肘子、水晶肘子、蜜蜡肘子。"
				+ "锅烧肘子、扒肘条、炖羊肉、酱羊肉、烧羊肉、烤羊肉、清羔羊肉、五香羊肉、氽三样儿、爆三样儿、炸卷果儿、烩散丹、烩酸燕儿、烩银丝、烩白杂碎、氽节子、烩节子、炸绣球。"
				+ "三鲜鱼翅、栗子鸡、氽鲤鱼、酱汁鲫鱼、活钻鲤鱼、板鸭、筒子鸡、烩脐肚、烩南荠、爆肚仁儿、盐水肘花儿、锅烧猪蹄儿、拌稂子、炖吊子、烧肝尖儿、烧肥肠儿、烧心、烧肺。"
				+ "烧紫盖儿、烧连帖、烧宝盖儿、油炸肺、酱瓜丝儿、山鸡丁儿、拌海蜇、龙须菜、炝冬笋、玉兰片、烧鸳鸯、烧鱼头、烧槟子、烧百合、炸豆腐、炸面筋、炸软巾、糖熘饹儿。"
				+ "拔丝山药、糖焖莲子、酿山药、杏仁儿酪、小炒螃蟹、氽大甲、炒荤素儿、什锦葛仙米、鳎目鱼、八代鱼、海鲫鱼、黄花鱼、鲥鱼、带鱼、扒海参、扒燕窝、扒鸡腿儿、扒鸡块儿。"
				+ "扒肉、扒面筋、扒三样儿、油泼肉、酱泼肉、炒虾黄、熘蟹黄、炒子蟹、炸子蟹、佛手海参、炸烹儿、炒芡子米、奶汤、翅子汤、三丝汤、熏斑鸠、卤斑鸠、海白米、烩腰丁儿。"
				+ "火烧茨菰、炸鹿尾儿、焖鱼头、拌皮渣儿、氽肥肠儿、炸紫盖儿、鸡丝豆苗、十二台菜、汤羊、鹿肉、驼峰、鹿大哈、插根儿、炸花件儿，清拌粉皮儿、炝莴笋、烹芽韭、木樨菜。"
				+ "烹丁香、烹大肉、烹白肉、麻辣野鸡、烩酸蕾、熘脊髓、咸肉丝儿、白肉丝儿、荸荠一品锅、素炝春不老、清焖莲子、酸黄菜、烧萝卜、脂油雪花儿菜、烩银耳、炒银枝儿。"
				+ "八宝榛子酱、黄鱼锅子、白菜锅子、什锦锅子、汤圆锅子、菊花锅子、杂烩锅子、煮饽饽锅子、肉丁辣酱、炒肉丝、炒肉片儿、烩酸菜、烩白菜、烩豌豆、焖扁豆、氽毛豆、炒豇豆，外加腌苤蓝丝儿。",
		};

		new MemSourceBatchOp(strings, "doc")
			.link(
				new SegmentBatchOp()
					.setSelectedCol("doc")
					.setOutputCol("words")
			)
			.link(
				new StopWordsRemoverBatchOp()
					.setSelectedCol("words")
			)
			.link(
				new KeywordsExtractionBatchOp()
					.setMethod(Method.TEXT_RANK)
					.setSelectedCol("words")
					.setOutputCol("extract_keywords")
			)
			.select("extract_keywords")
			.print();

		getSource()
			.link(
				new SegmentBatchOp()
					.setSelectedCol("news_title")
					.setOutputCol("segmented_title")
			)
			.link(
				new StopWordsRemoverBatchOp()
					.setSelectedCol("segmented_title")
			)
			.link(
				new KeywordsExtractionBatchOp()
					.setTopN(5)
					.setMethod(Method.TF_IDF)
					.setSelectedCol("segmented_title")
					.setOutputCol("extract_keywords")
			)
			.select("news_title, extract_keywords")
			.firstN(10)
			.print();

	}

	static void c_6_1() throws Exception {
		Row[] rows = new Row[] {
			Row.of("机器学习", "机器学习"),
			Row.of("批式计算", "流式计算"),
			Row.of("Machine Learning", "ML"),
			Row.of("Flink", "Alink"),
			Row.of("Good Morning!", "Good Evening!")
		};

		MemSourceBatchOp source = new MemSourceBatchOp(rows, new String[] {"col1", "col2"});

		source.lazyPrint(-1);

		source
			.link(
				new StringSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LEVENSHTEIN")
					.setOutputCol("LEVENSHTEIN")
			)
			.link(
				new StringSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LEVENSHTEIN_SIM")
					.setOutputCol("LEVENSHTEIN_SIM")
			)
			.link(
				new StringSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LCS")
					.setOutputCol("LCS")
			)
			.link(
				new StringSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LCS_SIM")
					.setOutputCol("LCS_SIM")
			)
			.link(
				new StringSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("JACCARD_SIM")
					.setOutputCol("JACCARD_SIM")
			)
			.lazyPrint(-1, "\n## StringSimilarityPairwiseBatchOp ##");

		source
			.link(
				new SegmentBatchOp()
					.setSelectedCol("col1")
			)
			.link(
				new SegmentBatchOp()
					.setSelectedCol("col2")
			)
			.link(
				new TextSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LEVENSHTEIN")
					.setOutputCol("LEVENSHTEIN")
			)
			.link(
				new TextSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LEVENSHTEIN_SIM")
					.setOutputCol("LEVENSHTEIN_SIM")
			)
			.link(
				new TextSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LCS")
					.setOutputCol("LCS")
			)
			.link(
				new TextSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LCS_SIM")
					.setOutputCol("LCS_SIM")
			)
			.link(
				new TextSimilarityPairwiseBatchOp()
					.setSelectedCols("col1", "col2")
					.setMetric("JACCARD_SIM")
					.setOutputCol("JACCARD_SIM")
			)
			.lazyPrint(-1, "\n## TextSimilarityPairwiseBatchOp ##");

		BatchOperator.execute();

		MemSourceStreamOp source_stream = new MemSourceStreamOp(rows, new String[] {"col1", "col2"});

		source_stream
			.link(
				new StringSimilarityPairwiseStreamOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LEVENSHTEIN")
					.setOutputCol("LEVENSHTEIN")
			)
			.link(
				new StringSimilarityPairwiseStreamOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LEVENSHTEIN_SIM")
					.setOutputCol("LEVENSHTEIN_SIM")
			)
			.link(
				new StringSimilarityPairwiseStreamOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LCS")
					.setOutputCol("LCS")
			)
			.link(
				new StringSimilarityPairwiseStreamOp()
					.setSelectedCols("col1", "col2")
					.setMetric("LCS_SIM")
					.setOutputCol("LCS_SIM")
			)
			.link(
				new StringSimilarityPairwiseStreamOp()
					.setSelectedCols("col1", "col2")
					.setMetric("JACCARD_SIM")
					.setOutputCol("JACCARD_SIM")
			)
			.print();

		StreamOperator.execute();

	}

	private static void c_6_2() throws Exception {
		BatchOperator.setParallelism(2);

		Row[] rows = new Row[] {
			Row.of("林徽因什么理由拒绝了徐志摩而选择梁思成为终身伴侣"),
			Row.of("发酵床的垫料种类有哪些？哪种更好？"),
			Row.of("京城最值得你来场文化之旅的博物馆"),
			Row.of("什么是超写实绘画？")
		};

		MemSourceBatchOp target = new MemSourceBatchOp(rows, new String[] {TXT_COL_NAME});

		BatchOperator <?> source = getSource();

		for (String metric : new String[] {"LEVENSHTEIN", "LCS", "SSK", "COSINE"}) {
			new StringNearestNeighbor()
				.setMetric(metric)
				.setSelectedCol(TXT_COL_NAME)
				.setIdCol(TXT_COL_NAME)
				.setTopN(5)
				.setOutputCol("similar_titles")
				.fit(source)
				.transform(target)
				.lazyPrint(-1, "StringNeareastNeighbor + " + metric.toString());
			BatchOperator.execute();
		}

		for (String metric : new String[] {"LEVENSHTEIN", "LCS", "SSK", "COSINE"}) {
			new Pipeline()
				.add(
					new Segment()
						.setSelectedCol(TXT_COL_NAME)
						.setOutputCol("segmented_title")
				)
				.add(
					new TextNearestNeighbor()
						.setMetric(metric)
						.setSelectedCol("segmented_title")
						.setIdCol(TXT_COL_NAME)
						.setTopN(5)
						.setOutputCol("similar_titles")
				)
				.fit(source)
				.transform(target)
				.lazyPrint(-1, "TextNeareastNeighbor + " + metric.toString());
			BatchOperator.execute();
		}

		for (String metric : new String[] {"JACCARD_SIM", "MINHASH_JACCARD_SIM", "SIMHASH_HAMMING_SIM",}) {
			new StringApproxNearestNeighbor()
				.setMetric(metric)
				.setSelectedCol(TXT_COL_NAME)
				.setIdCol(TXT_COL_NAME)
				.setTopN(5)
				.setOutputCol("similar_titles")
				.fit(source)
				.transform(target)
				.lazyPrint(-1, "StringApproxNeareastNeighbor + " + metric.toString());
			BatchOperator.execute();
		}

		for (String metric : new String[] {"JACCARD_SIM", "MINHASH_JACCARD_SIM", "SIMHASH_HAMMING_SIM"}) {
			new Pipeline()
				.add(
					new Segment()
						.setSelectedCol(TXT_COL_NAME)
						.setOutputCol("segmented_title")
				)
				.add(
					new TextApproxNearestNeighbor()
						.setMetric(metric)
						.setSelectedCol("segmented_title")
						.setIdCol(TXT_COL_NAME)
						.setTopN(5)
						.setOutputCol("similar_titles")
				)
				.fit(source)
				.transform(target)
				.lazyPrint(-1, "TextApproxNeareastNeighbor + " + metric.toString());
			BatchOperator.execute();
		}

		Pipeline snn = new Pipeline()
			.add(
				new StringNearestNeighbor()
					.setMetric("LEVENSHTEIN")
					.setSelectedCol(TXT_COL_NAME)
					.setIdCol(TXT_COL_NAME)
					.setTopN(5)
					.setOutputCol("similar_titles")
			);

		Pipeline approx_snn = new Pipeline()
			.add(
				new StringApproxNearestNeighbor()
					.setMetric("JACCARD_SIM")
					.setSelectedCol(TXT_COL_NAME)
					.setIdCol(TXT_COL_NAME)
					.setTopN(5)
					.setOutputCol("similar_titles")
			);

		Stopwatch sw = new Stopwatch();

		if (!new File(DATA_DIR + SNN_MODEL_FILE).exists()) {
			sw.reset();
			sw.start();
			snn.fit(source)
				.save(DATA_DIR + SNN_MODEL_FILE);
			BatchOperator.execute();
			sw.stop();
			System.out.println(sw.getElapsedTimeSpan());
		}

		if (!new File(DATA_DIR + APPROX_SNN_MODEL_FILE).exists()) {
			sw.reset();
			sw.start();
			approx_snn
				.fit(source)
				.save(DATA_DIR + APPROX_SNN_MODEL_FILE);
			BatchOperator.execute();
			sw.stop();
			System.out.println(sw.getElapsedTimeSpan());
		}

		BatchOperator <?> target_stock = source.filter("category_name = 'stock'");
		BatchOperator <?> target_news_story = source.filter("category_name = 'news_story'");

		sw.reset();
		sw.start();
		PipelineModel
			.load(DATA_DIR + SNN_MODEL_FILE)
			.transform(target_stock)
			.lazyPrint(10, "StringNeareastNeighbor + LEVENSHTEIN");
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		sw.reset();
		sw.start();
		PipelineModel
			.load(DATA_DIR + APPROX_SNN_MODEL_FILE)
			.transform(target_stock)
			.lazyPrint(10, "JACCARD_SIM + stock");
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		sw.reset();
		sw.start();
		PipelineModel
			.load(DATA_DIR + APPROX_SNN_MODEL_FILE)
			.transform(target_news_story)
			.lazyPrint(10, "JACCARD_SIM + news_story");
		BatchOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

		StreamOperator.setParallelism(1);

		StreamOperator <?> stream_target
			= new MemSourceStreamOp(rows, new String[] {TXT_COL_NAME});

		PipelineModel
			.load(DATA_DIR + SNN_MODEL_FILE)
			.transform(stream_target)
			.print();
		StreamOperator.execute();

		StreamOperator <?> stream_target_stock
			= getStreamSource().filter("category_name = 'stock'");

		sw.reset();
		sw.start();
		PipelineModel
			.load(DATA_DIR + APPROX_SNN_MODEL_FILE)
			.transform(stream_target_stock)
			.sample(0.02)
			.print();
		StreamOperator.execute();
		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());

	}

	private static void c_7() throws Exception {

		BatchOperator <?> docs = getSource()
			.select(LABEL_COL_NAME + ", " + TXT_COL_NAME)
			.link(new SegmentBatchOp().setSelectedCol(TXT_COL_NAME))
			.link(new StopWordsRemoverBatchOp().setSelectedCol(TXT_COL_NAME));

		docs.lazyPrint(10);

		if (!new File(DATA_DIR + LDA_MODEL_FILE).exists()) {
			LdaTrainBatchOp lda = new LdaTrainBatchOp()
				.setTopicNum(10)
				.setNumIter(200)
				.setVocabSize(20000)
				.setSelectedCol(TXT_COL_NAME)
				.setRandomSeed(123);

			docs.link(lda);

			lda.lazyPrintModelInfo();

			lda.link(
				new AkSinkBatchOp().setFilePath(DATA_DIR + LDA_MODEL_FILE)
			);

			lda.getSideOutput(0)
				.link(
					new AkSinkBatchOp().setFilePath(DATA_DIR + LDA_PWZ_FILE)
				);

			BatchOperator.execute();
		}

		new LdaPredictBatchOp()
			.setSelectedCol(TXT_COL_NAME)
			.setPredictionCol(PREDICTION_COL_NAME)
			.setPredictionDetailCol("predinfo")
			.linkFrom(
				new AkSourceBatchOp().setFilePath(DATA_DIR + LDA_MODEL_FILE),
				docs
			)
			.lazyPrint(5)
			.link(
				new EvalClusterBatchOp()
					.setLabelCol(LABEL_COL_NAME)
					.setPredictionCol(PREDICTION_COL_NAME)
					.lazyPrintMetrics()
			);

		AkSourceBatchOp pwz = new AkSourceBatchOp().setFilePath(DATA_DIR + LDA_PWZ_FILE);

		pwz.sample(0.001).lazyPrint(10);

		for (int t = 0; t < 10; t++) {
			pwz.select("word, topic_" + t)
				.orderBy("topic_" + t, 20, false)
				.lazyPrint(-1, "topic" + t);
		}

		BatchOperator.execute();
	}

	private static CsvSourceBatchOp getSource() {
		return new CsvSourceBatchOp()
			.setFilePath(DATA_DIR + ORIGIN_TRAIN_FILE)
			.setSchemaStr(SCHEMA_STRING)
			.setFieldDelimiter(FIELD_DELIMITER);
	}

	private static CsvSourceStreamOp getStreamSource() {
		return new CsvSourceStreamOp()
			.setFilePath(DATA_DIR + ORIGIN_TRAIN_FILE)
			.setSchemaStr(SCHEMA_STRING)
			.setFieldDelimiter(FIELD_DELIMITER);
	}

}
