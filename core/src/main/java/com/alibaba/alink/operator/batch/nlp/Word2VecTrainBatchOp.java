package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.nlp.Word2VecModelDataConverter;
import com.alibaba.alink.operator.common.nlp.WordCountUtil;
import com.alibaba.alink.common.utils.ExpTableArray;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.params.nlp.Word2VecTrainParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Word2VecTrainBatchOp extends BatchOperator<Word2VecTrainBatchOp>
	implements Word2VecTrainParams <Word2VecTrainBatchOp> {
	private static final Logger LOG = LoggerFactory.getLogger(Word2VecTrainBatchOp.class);

	public static int MAX_CODE_LENGTH = 40;

	public Word2VecTrainBatchOp() {
	}

	public Word2VecTrainBatchOp(Params params) {
		super(params);
	}

	private static void createBinaryTree(Word[] vocab) {
		int vocabSize = vocab.length;

		int[] point = new int[MAX_CODE_LENGTH];
		int[] code = new int[MAX_CODE_LENGTH];
		long[] count = new long[vocabSize * 2 - 1];
		int[] binary = new int[vocabSize * 2 - 1];
		int[] parent = new int[vocabSize * 2 - 1];

		for (int i = 0; i < vocabSize; ++i) {
			count[i] = vocab[i].cnt;
		}

		Arrays.fill(count, vocabSize, vocabSize * 2 - 1, Integer.MAX_VALUE);

		int min1i, min2i, pos1, pos2;

		pos1 = vocabSize - 1;
		pos2 = vocabSize;

		for (int a = 0; a < vocabSize - 1; ++a) {
			if (pos1 >= 0) {
				if (count[pos1] < count[pos2]) {
					min1i = pos1;
					pos1--;
				} else {
					min1i = pos2;
					pos2++;
				}
			} else {
				min1i = pos2;
				pos2++;
			}

			if (pos1 >= 0) {
				if (count[pos1] < count[pos2]) {
					min2i = pos1;
					pos1--;
				} else {
					min2i = pos2;
					pos2++;
				}
			} else {
				min2i = pos2;
				pos2++;
			}

			count[vocabSize + a] = count[min1i] + count[min2i];
			parent[min1i] = vocabSize + a;
			parent[min2i] = vocabSize + a;
			binary[min2i] = 1;
		}

		for (int a = 0; a < vocabSize; ++a) {
			int b = a;
			int i = 0;

			do {
				code[i] = binary[b];
				point[i] = b;
				i++;
				b = parent[b];
			} while (b != vocabSize * 2 - 2);

			vocab[a].code = new int[i];

			for (b = 0; b < i; ++b) {
				vocab[a].code[i - b - 1] = code[b];
			}

			vocab[a].point = new int[i];
			vocab[a].point[0] = vocabSize - 2;
			for (b = 1; b < i; ++b) {
				vocab[a].point[i - b] = point[b] - vocabSize;
			}
		}
	}

	private static DataSet <Row> sortedIndexVocab(DataSet <Row> vocab) {
		final int sortIdx = 1;
		Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> sorted
			= SortUtils.pSort(vocab, sortIdx);

		DataSet <Tuple2 <Integer, Row>> partitioned = sorted.f0.partitionCustom(new Partitioner <Integer>() {
			@Override
			public int partition(Integer key, int numPartitions) {
				return key;
			}
		}, 0);

		DataSet <Tuple2 <Integer, Long>> cnt = DataSetUtils.countElementsPerPartition(partitioned);

		return partitioned.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, Row>, Row>() {
			int start;
			int curLen;
			int total;

			@Override
			public void open(Configuration parameters) throws Exception {
				List <Tuple2 <Integer, Long>> cnts = getRuntimeContext().getBroadcastVariable("cnt");
				int taskId = getRuntimeContext().getIndexOfThisSubtask();
				start = 0;
				curLen = 0;
				total = 0;

				for (Tuple2 <Integer, Long> val : cnts) {
					if (val.f0 < taskId) {
						start += val.f1;
					}

					if (val.f0 == taskId) {
						curLen = val.f1.intValue();
					}

					total += val.f1.intValue();
				}
			}

			@Override
			public void mapPartition(Iterable <Tuple2 <Integer, Row>> values, Collector <Row> out) throws Exception {
				if (curLen <= 0) {
					return;
				}

				Row[] all = new Row[curLen];

				int i = 0;
				for (Tuple2 <Integer, Row> val : values) {
					all[i++] = val.f1;
				}

				Arrays.sort(all, (o1, o2) -> (int) ((Long) o1.getField(sortIdx) - (Long) o2.getField(sortIdx)));

				i = start;
				for (Row row : all) {
					out.collect(RowUtil.merge(row, -(i - total + 1)));
					++i;
				}
			}
		}).withBroadcastSet(cnt, "cnt");
	}

	private static DataSet <int[]> encodeContent(
		DataSet <String[]> content,
		DataSet <Tuple3 <Integer, String, Word>> vocab) {
		return content
			.mapPartition(new RichMapPartitionFunction <String[], Tuple4 <Integer, Long, Integer, String>>() {
				@Override
				public void mapPartition(Iterable <String[]> values,
										 Collector <Tuple4 <Integer, Long, Integer, String>> out)
					throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					long localCnt = 0L;
					for (String[] val : values) {
						if (val == null || val.length == 0) {
							continue;
						}

						for (int i = 0; i < val.length; ++i) {
							out.collect(new Tuple4 <>(taskId, localCnt, i, val[i]));
						}

						++localCnt;
					}
				}
			}).coGroup(vocab)
			.where(3)
			.equalTo(1)
			.with(new CoGroupFunction <Tuple4 <Integer, Long, Integer, String>, Tuple3 <Integer, String, Word>,
				Tuple4 <Integer, Long, Integer, Integer>>() {
				@Override
				public void coGroup(Iterable <Tuple4 <Integer, Long, Integer, String>> first,
									Iterable <Tuple3 <Integer, String, Word>> second,
									Collector <Tuple4 <Integer, Long, Integer, Integer>> out) {
					for (Tuple3 <Integer, String, Word> row : second) {
						for (Tuple4 <Integer, Long, Integer, String> tuple : first) {
							out.collect(
								Tuple4.of(tuple.f0, tuple.f1, tuple.f2,
									row.getField(0)));
						}
					}
				}
			}).groupBy(0, 1)
			.reduceGroup(new GroupReduceFunction <Tuple4 <Integer, Long, Integer, Integer>, int[]>() {
				@Override
				public void reduce(Iterable <Tuple4 <Integer, Long, Integer, Integer>> values, Collector <int[]> out) {
					ArrayList <Tuple2 <Integer, Integer>> elements = new ArrayList <>();

					for (Tuple4 <Integer, Long, Integer, Integer> val : values) {
						elements.add(Tuple2.of(val.f2, val.f3));
					}

					Collections.sort(elements, new Comparator <Tuple2 <Integer, Integer>>() {
						@Override
						public int compare(Tuple2 <Integer, Integer> o1, Tuple2 <Integer, Integer> o2) {
							return o1.f0.compareTo(o2.f0);
						}
					});

					int[] ret = new int[elements.size()];

					for (int i = 0; i < elements.size(); ++i) {
						ret[i] = elements.get(i).f1;
					}

					out.collect(ret);
				}
			});
	}

	@Override
	public Word2VecTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
		BatchOperator<?> in = checkAndGetFirst(inputs);
		final int vectorSize = getVectorSize();

		DataSet <Row> wordCnt = WordCountUtil
			.splitDocAndCount(in, getSelectedCol(), getWordDelimiter())
			.filter("cnt >= " + String.valueOf(getMinCount()))
			.getDataSet();

		DataSet <Row> sorted = sortedIndexVocab(wordCnt);

		DataSet <Long> vocSize = DataSetUtils
			.countElementsPerPartition(sorted)
			.sum(1)
			.map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
				@Override
				public Long map(Tuple2 <Integer, Long> value) throws Exception {
					return value.f1;
				}
			});

		DataSet <Tuple3 <Integer, String, Word>> vocab = sorted
			.reduceGroup(new CreateVocab())
			.withBroadcastSet(vocSize, "vocSize")
			.rebalance();

		DataSet <String[]> split = in
			.select("`" + getSelectedCol() + "`")
			.getDataSet()
			.flatMap(new WordCountUtil.WordSpliter(getWordDelimiter()))
			.rebalance();

		DataSet <int[]> trainData = encodeContent(split, vocab)
			.rebalance();

		final long seed = System.currentTimeMillis();

		DataSet <Tuple2 <Integer, Word>> vocabWithoutWordStr = vocab
			.map(new UseVocabWithoutWordString());

		DataSet <Tuple2 <Integer, double[]>> initialModel = vocabWithoutWordStr
			.mapPartition(new initialModel(seed, vectorSize))
			.rebalance();

		DataSet <Integer> syncNum = DataSetUtils
			.countElementsPerPartition(trainData)
			.sum(1)
			.map(new MapFunction <Tuple2 <Integer, Long>, Integer>() {
				@Override
				public Integer map(Tuple2 <Integer, Long> value) throws Exception {
					return Math.max((int) (value.f1 / 100000L), 5);
				}
			});

		DataSet <Row> model = new IterativeComQueue()
			.initWithPartitionedData("trainData", trainData)
			.initWithBroadcastData("vocSize", vocSize)
			.initWithBroadcastData("initialModel", initialModel)
			.initWithBroadcastData("vocabWithoutWordStr", vocabWithoutWordStr)
			.initWithBroadcastData("syncNum", syncNum)
			.add(new InitialVocabAndBuffer(getParams()))
			.add(new UpdateModel(getParams()))
			.add(new AllReduce("input"))
			.add(new AllReduce("output"))
			.add(new AvgInputOutput())
			.setCompareCriterionOfNode0(new Criterion(getParams()))
			.closeWith(new SerializeModel(getParams()))
			.exec();

		model = model
			.map(new MapFunction <Row, Tuple2 <Integer, DenseVector>>() {
				@Override
				public Tuple2 <Integer, DenseVector> map(Row value) throws Exception {
					return Tuple2.of((Integer) value.getField(0), (DenseVector) value.getField(1));
				}
			})
			.join(vocab)
			.where(0)
			.equalTo(0)
			.with(new JoinFunction <Tuple2 <Integer, DenseVector>, Tuple3 <Integer, String, Word>, Row>() {
				@Override
				public Row join(Tuple2 <Integer, DenseVector> first, Tuple3 <Integer, String, Word> second)
					throws Exception {
					return Row.of(second.f1, first.f1);
				}
			})
			.mapPartition(new MapPartitionFunction <Row, Row>() {
				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
					Word2VecModelDataConverter model = new Word2VecModelDataConverter();

					model.modelRows = StreamSupport
						.stream(values.spliterator(), false)
						.collect(Collectors.toList());

					model.save(model, out);
				}
			});

		setOutput(model, new Word2VecModelDataConverter().getModelSchema());

		return this;
	}

	private static class Word implements Serializable {
		public long cnt;
		public int[] point;
		public int[] code;
	}

	private static class InitialVocabAndBuffer extends ComputeFunction {
		Params params;

		public InitialVocabAndBuffer(Params params) {
			this.params = params;
		}

		@Override
		public void calc(ComContext context) {
			if (context.getStepNo() == 1) {
				int vectorSize = params.get(Word2VecTrainParams.VECTOR_SIZE);
				List <Long> vocSizeList = context.getObj("vocSize");
				List <Tuple2 <Integer, double[]>> initialModel = context.getObj("initialModel");
				List <Tuple2 <Integer, Word>> vocabWithoutWordStr = context.getObj("vocabWithoutWordStr");

				int vocSize = vocSizeList.get(0).intValue();

				double[] input = new double[vectorSize * vocSize];

				Word[] vocab = new Word[vocSize];

				for (int i = 0; i < vocSize; ++i) {
					Tuple2 <Integer, double[]> item = initialModel.get(i);
					System.arraycopy(item.f1, 0, input,
						item.f0 * vectorSize, vectorSize);
					Tuple2 <Integer, Word> vocabItem = vocabWithoutWordStr.get(i);
					vocab[vocabItem.f0] = vocabItem.f1;
				}

				context.putObj("input", input);
				context.putObj("output", new double[vectorSize * (vocSize - 1)]);
				context.putObj("vocab", vocab);

				context.removeObj("initialModel");
				context.removeObj("vocabWithoutWordStr");
			}
		}
	}

	private static class CalcModel {
		private final int vectorSize;
		private final int window;
		private final double alpha;
		private boolean randomWindow;
		private double[] input;
		private double[] output;
		private int taskId;
		private Word[] vocab;
		private Random random;

		public CalcModel(
			int vectorSize, long seed, boolean randomWindow, int window,
			double alpha, int taskId, Word[] vocab,
			double[] input, double[] output) {
			this.vectorSize = vectorSize;
			this.randomWindow = randomWindow;
			this.window = window;
			this.alpha = alpha;
			this.vocab = vocab;
			this.input = input;
			this.output = output;
			this.taskId = taskId;
			random = new Random(seed);
		}

		public void update(List <int[]> values) {
			LOG.info("taskId: {}, map partition start", taskId);

			double[] neu1e = new double[vectorSize];
			double f, g;
			int b, c, lastWord, l1, l2;

			for (int[] val : values) {
				for (int i = 0; i < val.length; ++i) {
					if (randomWindow) {
						b = random.nextInt(window);
					} else {
						b = 0;
					}

					int bound = window * 2 + 1 - b;
					for (int a = b; a < bound; ++a) {
						if (a != window) {
							c = i - window + a;
							if (c < 0 || c >= val.length) {
								continue;
							}

							lastWord = val[c];
							l1 = lastWord * vectorSize;

							Arrays.fill(neu1e, 0.f);

							Word w = vocab[val[i]];
							int codeLen = w.code.length;

							for (int d = 0; d < codeLen; ++d) {
								f = 0.f;
								l2 = w.point[d] * vectorSize;

								for (int t = 0; t < vectorSize; ++t) {
									f += input[l1 + t] * output[l2 + t];
								}

								if (f > -6.0f && f < 6.0f) {
									f = ExpTableArray.sigmoidTable[(int) ((f + 6.0) * 84.0)];
									g = (1.f - w.code[d] - f) * alpha;

									for (int t = 0; t < vectorSize; ++t) {
										neu1e[t] += g * output[l2 + t];
									}

									for (int t = 0; t < vectorSize; ++t) {
										output[l2 + t] += g * input[l1 + t];
									}

								}
							}

							for (int t = 0; t < vectorSize; ++t) {
								input[l1 + t] += neu1e[t];
							}
						}
					}
				}
			}
			LOG.info("taskId: {}, map partition end", taskId);
		}
	}

	private static class initialModel
		extends RichMapPartitionFunction <Tuple2 <Integer, Word>, Tuple2 <Integer, double[]>> {
		private final long seed;
		private final int vectorSize;
		Random random;

		public initialModel(long seed, int vectorSize) {
			this.seed = seed;
			this.vectorSize = vectorSize;
			random = new Random();
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			random.setSeed(seed + getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public void mapPartition(Iterable <Tuple2 <Integer, Word>> values,
								 Collector <Tuple2 <Integer, double[]>> out) throws Exception {
			for (Tuple2 <Integer, Word> val : values) {
				double[] inBuf = new double[vectorSize];

				for (int i = 0; i < vectorSize; ++i) {
					inBuf[i] = random.nextFloat();
				}

				out.collect(Tuple2.of(val.f0, inBuf));
			}
		}
	}

	private static class UseVocabWithoutWordString
		implements MapFunction <Tuple3 <Integer, String, Word>, Tuple2 <Integer, Word>> {
		@Override
		public Tuple2 <Integer, Word> map(Tuple3 <Integer, String, Word> value) throws Exception {
			return Tuple2.of(value.f0, value.f2);
		}
	}

	private static class CreateVocab extends RichGroupReduceFunction <Row, Tuple3 <Integer, String, Word>> {
		int vocSize;

		@Override
		public void open(Configuration parameters) throws Exception {
			vocSize = getRuntimeContext().getBroadcastVariableWithInitializer("vocSize",
				new BroadcastVariableInitializer <Long, Integer>() {
					@Override
					public Integer initializeBroadcastVariable(Iterable <Long> data) {
						return data.iterator().next().intValue();
					}
				});
		}

		@Override
		public void reduce(Iterable <Row> values, Collector <Tuple3 <Integer, String, Word>> out) throws Exception {
			String[] words = new String[vocSize];
			Word[] vocab = new Word[vocSize];

			for (Row row : values) {
				Word word = new Word();
				word.cnt = (long) row.getField(1);
				vocab[(int) row.getField(2)] = word;
				words[(int) row.getField(2)] = (String) row.getField(0);
			}

			createBinaryTree(vocab);

			for (int i = 0; i < vocab.length; ++i) {
				out.collect(Tuple3.of(i, words[i], vocab[i]));
			}
		}
	}

	private static class AvgInputOutput extends ComputeFunction {
		@Override
		public void calc(ComContext context) {
			double[] input = context.getObj("input");

			for (int i = 0; i < input.length; ++i) {
				input[i] /= context.getNumTask();
			}

			double[] output = context.getObj("output");

			for (int i = 0; i < output.length; ++i) {
				output[i] /= context.getNumTask();
			}
		}
	}

	private static class Criterion extends CompareCriterionFunction {
		Params params;

		public Criterion(Params params) {
			this.params = params;
		}

		@Override
		public boolean calc(ComContext context) {
			return (context.getStepNo() - 1)
				== ((List <Integer>) context.getObj("syncNum")).get(0)
				* params.get(Word2VecTrainParams.NUM_ITER);
		}
	}

	private static class UpdateModel extends ComputeFunction {
		Params params;

		public UpdateModel(Params params) {
			this.params = params;
		}

		@Override
		public void calc(ComContext context) {
			List <int[]> trainData = context.getObj("trainData");

			int syncNum = ((List <Integer>) context.getObj("syncNum")).get(0);

			if (trainData == null) {
				return;
			}

			DistributedInfo distributedInfo = new DefaultDistributedInfo();

			long startPos = distributedInfo.startPos(
				(context.getStepNo() - 1) % syncNum,
				syncNum,
				trainData.size()
			);

			long localRowCnt = distributedInfo.localRowCnt(
				(context.getStepNo() - 1) % syncNum,
				syncNum,
				trainData.size()
			);

			new CalcModel(
				params.get(Word2VecTrainParams.VECTOR_SIZE),
				System.currentTimeMillis(),
				Boolean.parseBoolean(params.get(Word2VecTrainParams.RANDOM_WINDOW)),
				params.get(Word2VecTrainParams.WINDOW),
				params.get(Word2VecTrainParams.ALPHA),
				context.getTaskId(),
				context.getObj("vocab"),
				context.getObj("input"),
				context.getObj("output")
			).update(trainData.subList((int) startPos, (int) (startPos + localRowCnt)));
		}
	}

	private static class SerializeModel extends CompleteResultFunction {
		Params params;

		public SerializeModel(Params params) {
			this.params = params;
		}

		@Override
		public List <Row> calc(ComContext context) {
			if (context.getTaskId() != 0) {
				return null;
			}

			int vocSize = ((List <Long>) context.getObj("vocSize")).get(0).intValue();
			int vectorSize = params.get(Word2VecTrainParams.VECTOR_SIZE);

			List <Row> ret = new ArrayList <>(vocSize);

			double[] input = context.getObj("input");

			for (int i = 0; i < vocSize; ++i) {
				DenseVector dv = new DenseVector(vectorSize);
                System.arraycopy(input, i * vectorSize, dv.getData(), 0, vectorSize);

				ret.add(Row.of(i, dv));
			}

			return ret;
		}
	}
}
