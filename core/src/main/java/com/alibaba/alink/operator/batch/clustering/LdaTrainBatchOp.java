package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocCountVectorizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.clustering.LdaModelData;
import com.alibaba.alink.operator.common.clustering.LdaModelDataConverter;
import com.alibaba.alink.operator.common.clustering.LdaModelMapper;
import com.alibaba.alink.operator.common.clustering.lda.BuildEmLdaModel;
import com.alibaba.alink.operator.common.clustering.lda.BuildOnlineLdaModel;
import com.alibaba.alink.operator.common.clustering.lda.EmCorpusStep;
import com.alibaba.alink.operator.common.clustering.lda.EmLogLikelihood;
import com.alibaba.alink.operator.common.clustering.lda.LdaUtil;
import com.alibaba.alink.operator.common.clustering.lda.LdaVariable;
import com.alibaba.alink.operator.common.clustering.lda.OnlineCorpusStep;
import com.alibaba.alink.operator.common.clustering.lda.OnlineLogLikelihood;
import com.alibaba.alink.operator.common.clustering.lda.UpdateLambdaAndAlpha;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelData;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelMapper;
import com.alibaba.alink.operator.common.nlp.FeatureType;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.clustering.LdaTrainParams;
import com.google.common.collect.Lists;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_128;

/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 * Input articles data, then LDA algorithm can give the probability that each word in
 * the input articles belongs to each topic, and it can predict the topic of other input articles.
 * This algorithm can also generate the probability that each word belongs to each topic and the
 * perplexity which can evaluate the fitting effect of this algorithm.
 */
public class LdaTrainBatchOp extends BatchOperator <LdaTrainBatchOp>
	implements LdaTrainParams <LdaTrainBatchOp>,
	WithModelInfoBatchOp <LdaModelInfo, LdaTrainBatchOp, LdaModelInfoBatchOp> {

	private static final long serialVersionUID = -7108371385756952009L;

	/**
	 * Constructor.
	 */
	public LdaTrainBatchOp() {
		super(null);
	}

	/**
	 * Constructor.
	 *
	 * @param params the params of the algorithm.
	 */
	public LdaTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public LdaTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		int parallelism = BatchOperator.getExecutionEnvironmentFromOps(in).getParallelism();
		long mlEnvId = getMLEnvironmentId();
		int numTopic = getTopicNum();
		int numIter = getNumIter();
		Integer seed = getRandomSeed();
		boolean setSeed = true;
		if (seed == null) {
			setSeed = false;
		}

		String vectorColName = getSelectedCol();
		Method optimizer = getMethod();
		final DataSet <DocCountVectorizerModelData> resDocCountModel = DocCountVectorizerTrainBatchOp
			.generateDocCountModel(getParams(), in);
		int index = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName);
		DataSet <Row> resRow = in.getDataSet()
			.flatMap(new Document2Vector(index)).withBroadcastSet(resDocCountModel, "DocCountModel");
		TypeInformation <?>[] types = in.getColTypes().clone();
		types[index] = VectorTypes.SPARSE_VECTOR;
		BatchOperator trainData = new TableSourceBatchOp(DataSetConversionUtil
			.toTable(mlEnvId, resRow, in.getColNames(), types))
			.setMLEnvironmentId(mlEnvId);
		Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>> dataAndStat
			= StatisticsHelper.summaryHelper(trainData, null, vectorColName);

		if (setSeed) {

			DataSet <Tuple2 <Long, Vector>> hashValue = dataAndStat.f0
				.map(new MapHashValue(seed))
				.partitionCustom(new Partitioner <Long>() {
					private static final long serialVersionUID = 5179898093029365608L;

					@Override
					public int partition(Long key, int numPartitions) {
						return (int) (Math.abs(key) % ((long) numPartitions));
					}
				}, 0);
			dataAndStat.f0 = hashValue.mapPartition(new MapPartitionFunction <Tuple2 <Long, Vector>, Vector>() {
				private static final long serialVersionUID = -550512476573928350L;

				@Override
				public void mapPartition(Iterable <Tuple2 <Long, Vector>> values, Collector <Vector> out)
					throws Exception {
					List <Tuple2 <Long, Vector>> listValues = Lists.newArrayList(values);
					listValues.sort(new Comparator <Tuple2 <Long, Vector>>() {
						@Override
						public int compare(Tuple2 <Long, Vector> o1, Tuple2 <Long, Vector> o2) {
							int compare1 = o1.f0.compareTo(o2.f0);
							if (compare1 == 0) {
								String o1s = o1.f1.toString();
								String o2s = o2.f1.toString();
								return o1s.compareTo(o2s);
							}
							return compare1;
						}
					});
					listValues.forEach(x -> out.collect(x.f1));
				}
			}).setParallelism(parallelism);
		}
		double beta = getParams().get(BETA);
		double alpha = getParams().get(ALPHA);
		int gammaShape = 250;
		switch (optimizer) {
			case EM:
				gibbsSample(dataAndStat, numTopic, numIter, alpha, beta, resDocCountModel, seed);
				break;
			case Online:
				online(dataAndStat, numTopic, numIter, alpha, beta, resDocCountModel, gammaShape, seed);
				break;
			default:
				throw new NotImplementedException("Optimizer not support.");
		}
		return this;
	}

	private void gibbsSample(Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>> dataAndStat,
							 int numTopic,
							 int numIter,
							 double alpha,
							 double beta,
							 DataSet <DocCountVectorizerModelData> resDocCountModel, Integer seed) {
		if (beta == -1) {
			beta = 0.01 + 1;
		}
		if (alpha == -1) {
			alpha = 50.0 / numTopic + 1;
		}
		DataSet <Vector> data = dataAndStat.f0;
		DataSet <Integer> colNum = dataAndStat.f1
			.map(new MapFunction <BaseVectorSummary, Integer>() {
				private static final long serialVersionUID = -7170259222827300492L;

				@Override
				public Integer map(BaseVectorSummary srt) {
					return srt.vectorSize();
				}
			});

		DataSet <Row> ldaModelData = new IterativeComQueue()
			.initWithPartitionedData(LdaVariable.data, data)
			.initWithBroadcastData(LdaVariable.vocabularySize, colNum)
			.add(new EmCorpusStep(numTopic, alpha, beta, seed))
			.add(new AllReduce(LdaVariable.nWordTopics))
			.add(new EmLogLikelihood(numTopic, alpha, beta, numIter))
			.add(new AllReduce(LdaVariable.logLikelihood))
			.closeWith(new BuildEmLdaModel(numTopic, alpha, beta))
			.setMaxIter(numIter)
			.exec();
		DataSet <Row> model = ldaModelData.flatMap(new BuildResModel(seed))
			.withBroadcastSet(resDocCountModel, "DocCountModel");
		setOutput(model, new LdaModelDataConverter().getModelSchema());
		saveWordTopicModelAndPerplexity(model, numTopic, false);
	}

	private void online(Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>> dataAndStat,
						int numTopic,
						int numIter,
						double alpha,
						double beta,
						DataSet <DocCountVectorizerModelData> resDocCountModel,
						int gammaShape, Integer seed) {
		if (beta == -1) {
			beta = 1.0 / numTopic;
		}
		if (alpha == -1) {
			alpha = 1.0 / numTopic;
		}
		double learningOffset = getParams().get(ONLINE_LEARNING_OFFSET);
		double learningDecay = getParams().get(LEARNING_DECAY);
		double subSamplingRate = getParams().get(SUBSAMPLING_RATE);
		boolean optimizeDocConcentration = getParams().get(OPTIMIZE_DOC_CONCENTRATION);

		DataSet <Vector> data = dataAndStat.f0;
		DataSet <Tuple2 <Long, Integer>> shape = dataAndStat.f1
			.map(new MapFunction <BaseVectorSummary, Tuple2 <Long, Integer>>() {
				private static final long serialVersionUID = 1305270477796787466L;

				@Override
				public Tuple2 <Long, Integer> map(BaseVectorSummary srt) {
					return new Tuple2 <>(srt.count(), srt.vectorSize());
				}
			});
		DataSet <Tuple2 <DenseMatrix, DenseMatrix>> initModel = data
			.mapPartition(new OnlineInit(numTopic, gammaShape, alpha, seed))
			.name("init lambda")
			.withBroadcastSet(shape, LdaVariable.shape);
		DataSet <Row> ldaModelData = new IterativeComQueue()
			.initWithPartitionedData(LdaVariable.data, data)
			.initWithBroadcastData(LdaVariable.shape, shape)
			.initWithBroadcastData(LdaVariable.initModel, initModel)
			.add(new OnlineCorpusStep(numTopic, subSamplingRate, gammaShape, seed))
			.add(new AllReduce(LdaVariable.wordTopicStat))
			.add(new AllReduce(LdaVariable.logPhatPart))
			.add(new AllReduce(LdaVariable.nonEmptyWordCount))
			.add(new AllReduce(LdaVariable.nonEmptyDocCount))
			.add(new UpdateLambdaAndAlpha(numTopic, learningOffset, learningDecay,
				subSamplingRate, optimizeDocConcentration, beta))
			.add(new OnlineLogLikelihood(beta, numTopic, numIter, gammaShape, seed))
			.add(new AllReduce(LdaVariable.logLikelihood))
			.closeWith(new BuildOnlineLdaModel(numTopic, beta))
			.setMaxIter(numIter)
			.exec();
		DataSet <Row> model = ldaModelData.flatMap(new BuildResModel(seed))
			.withBroadcastSet(resDocCountModel, "DocCountModel");
		setOutput(model, new LdaModelDataConverter().getModelSchema());
		saveWordTopicModelAndPerplexity(model, numTopic, true);
	}

	/**
	 * Save the word-topic model in the sideOutputs.
	 */
	private void saveWordTopicModelAndPerplexity(DataSet <Row> model, int numTopic,
												 Boolean ifOnline) {
		DataSet <Row> wordTopicDataSet;
		if (ifOnline) {
			wordTopicDataSet = model.mapPartition(new BuildWordTopicModelOnline()).setParallelism(1);
		} else {
			wordTopicDataSet = model.mapPartition(new BuildWordTopicModelGibbs()).setParallelism(1);
		}
		String[] colNames = new String[numTopic + 1];
		TypeInformation[] colTypes = new TypeInformation[colNames.length];
		colNames[0] = "word";
		colTypes[0] = Types.STRING;
		for (int i = 0; i < numTopic; i++) {
			colNames[1 + i] = "topic_" + i;
			colTypes[1 + i] = Types.DOUBLE;
		}

		DataSet <Row> logPerplexity = model.mapPartition(new CalculatePerplexityAndLikelihood()).setParallelism(1);
		this.setSideOutputTables(new Table[] {
			DataSetConversionUtil.toTable(getMLEnvironmentId(), wordTopicDataSet, colNames, colTypes),
			DataSetConversionUtil.toTable(getMLEnvironmentId(),
				logPerplexity, new String[] {"logPerplexity", "logLikelihood"},
				new TypeInformation[] {Types.DOUBLE, Types.DOUBLE})
		});
	}

	@Override
	public LdaModelInfoBatchOp getModelInfoBatchOp() {
		return new LdaModelInfoBatchOp(getParams()).linkFrom(this);
	}

	private static class MapHashValue extends RichMapFunction <Vector, Tuple2 <Long, Vector>> {
		private static final long serialVersionUID = 3109898948593135554L;
		int seed;
		HashFunction hashFunc;

		MapHashValue(int seed) {
			this.seed = seed;
			hashFunc = murmur3_128(seed);
		}

		@Override
		public Tuple2 <Long, Vector> map(Vector value) throws Exception {
			Long hashValue = hashFunc.hashUnencodedChars(value.toString()).asLong();
			return Tuple2.of(hashValue, value);
		}
	}

	/**
	 * Initialize model for online train.
	 */
	private static class OnlineInit extends RichMapPartitionFunction <Vector, Tuple2 <DenseMatrix, DenseMatrix>> {
		private static final long serialVersionUID = -7532989140899942816L;
		private int vocabularySize;
		private int numTopic;
		private int gammaShape;
		private double alpha;
		private Integer seed;

		OnlineInit(int numTopic, int gammaShape, double alpha, Integer seed) {
			this.numTopic = numTopic;
			this.gammaShape = gammaShape;
			this.alpha = alpha;
			this.seed = seed;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple2 <Long, Integer>> tuple2 = this.getRuntimeContext().getBroadcastVariable(LdaVariable.shape);
			vocabularySize = tuple2.get(0).f1;
		}

		@Override
		public void mapPartition(Iterable <Vector> iterable, Collector <Tuple2 <DenseMatrix, DenseMatrix>> result) {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				RandomDataGenerator rand = new RandomDataGenerator();
				if (seed != null) {
					rand.reSeed(seed);
				}
				double[] randomData = new double[numTopic * vocabularySize];
				for (int i = 0; i < randomData.length; i++) {
					randomData[i] = rand.nextGamma(gammaShape, 1.0 / gammaShape);
				}
				DenseMatrix lambda = new DenseMatrix(vocabularySize, numTopic, randomData, true).transpose();
				double[] alphaVec = new double[numTopic];
				Arrays.fill(alphaVec, alpha);
				DenseMatrix alphaMatrix = new DenseMatrix(numTopic, 1, alphaVec);
				result.collect(new Tuple2 <>(lambda, alphaMatrix));
			}
		}
	}

	/**
	 * Transform LdaModelData into DataSet<Row> model.
	 */
	private static class BuildResModel extends RichFlatMapFunction <Row, Row> {
		private static final long serialVersionUID = 2523941574777534645L;
		private DocCountVectorizerModelData docCountModelData;
		Integer seed;

		BuildResModel(Integer seed) {
			this.seed = seed;
		}

		@Override
		public void open(Configuration parameters) {
			docCountModelData = (DocCountVectorizerModelData)
				this.getRuntimeContext().getBroadcastVariable("DocCountModel").get(0);
		}

		@Override
		public void flatMap(Row value, Collector <Row> res) throws Exception {
			LdaModelData modelData = (LdaModelData) value.getField(0);
			modelData.seed = seed;
			modelData.list = docCountModelData.list;
			LdaModelDataConverter converter = new LdaModelDataConverter();
			RowCollector collector = new RowCollector();
			converter.save(modelData, collector);
			List <Row> out = collector.getRows();
			for (Row row : out) {
				res.collect(row);
			}
		}
	}

	/**
	 * Transform the input article to SparseVector.
	 */
	private static class Document2Vector extends RichFlatMapFunction <Row, Row> {
		private static final long serialVersionUID = -7610300103336726860L;
		private double minTF;
		private HashMap <String, Tuple2 <Integer, Double>> wordIdWeight;
		private int featureNum;
		private int index;
		private FeatureType featureType;

		Document2Vector(int index) {
			this.index = index;
		}

		@Override
		public void open(Configuration parameters) {
			DocCountVectorizerModelData data = (DocCountVectorizerModelData)
				this.getRuntimeContext().getBroadcastVariable("DocCountModel").get(0);
			featureNum = data.list.size();
			minTF = data.minTF;
			this.featureType = FeatureType.valueOf(data.featureType.toUpperCase());
			this.wordIdWeight = LdaUtil.setWordIdWeightPredict(data.list);
		}

		@Override
		public void flatMap(Row value, Collector <Row> out) throws Exception {
			SparseVector sv = DocCountVectorizerModelMapper.predictSparseVector((String) value.getField(index),
				minTF, wordIdWeight, featureType, featureNum);
			if (sv.getIndices() != null && sv.getIndices().length != 0) {
				value.setField(index, sv);
				out.collect(value);
			}
		}
	}

	/**
	 * Build the word-topic model with online method.
	 */
	private static class BuildWordTopicModelOnline implements MapPartitionFunction <Row, Row> {

		private static final long serialVersionUID = -5305654619082861506L;

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
			List <Row> rows = new ArrayList <>();
			for (Row row : values) {
				rows.add(row);
			}
			LdaModelDataConverter model = new LdaModelDataConverter();
			LdaModelData modelData = model.load(rows);

			DenseMatrix wordTopicMatrix = modelData.wordTopicCounts;
			int topicNum = wordTopicMatrix.numRows();
			int wordNum = wordTopicMatrix.numCols();
			HashMap <Integer, String> wordIdWeight = LdaUtil.setWordIdWeightTrain(modelData.list);
			for (int i = 0; i < wordNum; i++) {
				out.collect(standardizeWordTopicModel(wordTopicMatrix, i, topicNum, wordIdWeight));
			}
		}
	}

	/**
	 * Build the word-topic model with em method.
	 */
	private static class BuildWordTopicModelGibbs implements MapPartitionFunction <Row, Row> {

		private static final long serialVersionUID = 6572052177423807502L;

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
			List <Row> rows = new ArrayList <>();
			for (Row row : values) {
				rows.add(row);
			}
			LdaModelDataConverter model = new LdaModelDataConverter();
			LdaModelData modelData = model.load(rows);
			int vocabularySize = modelData.vocabularySize;
			int topicNum = modelData.topicNum;
			DenseMatrix gamma = modelData.gamma;

			DenseMatrix wordTopicMatrix = LdaModelMapper.getWordTopicMatrixGibbs(vocabularySize, topicNum, gamma,
				modelData);
			int wordNum = wordTopicMatrix.numCols();
			HashMap <Integer, String> wordIdWeight = LdaUtil.setWordIdWeightTrain(modelData.list);
			for (int i = 0; i < wordNum; i++) {
				out.collect(standardizeWordTopicModel(wordTopicMatrix, i, topicNum, wordIdWeight));
			}
		}
	}

	/**
	 * Calculate the log perplexity and log likelihood.
	 */
	private static class CalculatePerplexityAndLikelihood implements MapPartitionFunction <Row, Row> {

		private static final long serialVersionUID = 859887532899418780L;

		@Override
		public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
			List <Row> rows = new ArrayList <>();
			for (Row row : values) {
				rows.add(row);
			}
			LdaModelDataConverter model = new LdaModelDataConverter();
			LdaModelData modelData = model.load(rows);
			out.collect(Row.of(modelData.logPerplexity, modelData.logLikelihood));
		}
	}

	private static Row standardizeWordTopicModel(DenseMatrix pwz, int i, int topicNum, HashMap wordIdWeight) {
		Row out = new Row(topicNum + 1);
		out.setField(0, wordIdWeight.get(i));
		double sum = 0;
		for (int j = 0; j < topicNum; j++) {
			sum += pwz.get(j, i);
		}
		if (sum != 0) {
			for (int j = 0; j < topicNum; j++) {
				out.setField(j + 1, pwz.get(j, i) / sum);
			}
		} else {
			for (int j = 0; j < topicNum; j++) {
				out.setField(j + 1, pwz.get(j, i));
			}
		}
		return out;
	}

}
