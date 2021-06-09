package com.alibaba.alink.operator.batch.huge.impl;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.huge.word2vec.ApsIteratorW2V;
import com.alibaba.alink.operator.batch.huge.word2vec.ApsSerializeDataW2V;
import com.alibaba.alink.operator.batch.huge.word2vec.ApsSerializeModelW2V;
import com.alibaba.alink.operator.batch.huge.word2vec.InitVecs;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.aps.ApsContext;
import com.alibaba.alink.operator.common.aps.ApsEnv;
import com.alibaba.alink.operator.common.aps.ApsCheckpoint;
import com.alibaba.alink.operator.common.nlp.WordCountUtil;
import com.alibaba.alink.params.huge.HasNumCheckpoint;
import com.alibaba.alink.params.nlp.HasBatchSize;
import com.alibaba.alink.params.nlp.Word2VecParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Word2VecImpl<T extends Word2VecImpl <T>> extends BatchOperator <T>
	implements Word2VecParams <T> {

	public final static ParamInfo <Boolean> METE_PATH_MODE = ParamInfoFactory
		.createParamInfo("metapathMode", Boolean.class)
		.setDescription("Is metapath or not")
		.setOptional()
		.setHasDefaultValue(false)
		.build();

	private final static Logger LOG = LoggerFactory.getLogger(Word2VecImpl.class);

	private static final double POWER = 0.75;
	private static final long serialVersionUID = 1004822828820732709L;

	private ApsEnv <int[], float[]> apsEnv;

	public Word2VecImpl(Params params) {
		this(params, null);
	}

	public Word2VecImpl(Params params, ApsCheckpoint checkpoint) {
		super(params);

		apsEnv = new ApsEnv <>(
			checkpoint,
			new ApsSerializeDataW2V(),
			new ApsSerializeModelW2V(),
			getMLEnvironmentId()
		);
	}

	static DataSet <int[]> encodeContent(
		DataSet <String[]> content,
		DataSet <Row> vocab) {
		return content
			.mapPartition(new RichMapPartitionFunction <String[], Tuple4 <Integer, Long, Integer, String>>() {
				private static final long serialVersionUID = 4837202824157637433L;

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
			.equalTo(new KeySelector <Row, String>() {
				private static final long serialVersionUID = -5072064155656481156L;

				@Override
				public String getKey(Row value) {
					return (String) value.getField(0);
				}
			})
			.with(
				new CoGroupFunction <Tuple4 <Integer, Long, Integer, String>, Row,
					Tuple4 <Integer, Long, Integer, Long>>() {
					private static final long serialVersionUID = -6679964211169574897L;

					@Override
					public void coGroup(Iterable <Tuple4 <Integer, Long, Integer, String>> first, Iterable <Row>
						second,
										Collector <Tuple4 <Integer, Long, Integer, Long>> out) {
						for (Row row : second) {
							for (Tuple4 <Integer, Long, Integer, String> tuple : first) {
								out.collect(
									Tuple4.of(tuple.f0, tuple.f1, tuple.f2,
										(Long) row.getField(2)));
							}
						}
					}
				}).groupBy(0, 1)
			.reduceGroup(new GroupReduceFunction <Tuple4 <Integer, Long, Integer, Long>, int[]>() {
				private static final long serialVersionUID = 2777302181976220500L;

				@Override
				public void reduce(Iterable <Tuple4 <Integer, Long, Integer, Long>> values, Collector <int[]> out) {
					ArrayList <Tuple2 <Integer, Long>> elements = new ArrayList <>();

					for (Tuple4 <Integer, Long, Integer, Long> val : values) {
						elements.add(Tuple2.of(val.f2, val.f3));
					}

					Collections.sort(elements, new Comparator <Tuple2 <Integer, Long>>() {
						@Override
						public int compare(Tuple2 <Integer, Long> o1, Tuple2 <Integer, Long> o2) {
							return o1.f0.compareTo(o2.f0);
						}
					});

					int[] ret = new int[elements.size()];

					for (int i = 0; i < elements.size(); ++i) {
						ret[i] = (elements.get(i).f1).intValue();
					}

					out.collect(ret);
				}
			});
	}

	public static DataSet <Long> countVoc(DataSet <Row> in) {
		return in.map(new MapFunction <Row, Long>() {
			private static final long serialVersionUID = 1154685972382889196L;

			@Override
			public Long map(Row value) throws Exception {
				return 1L;
			}
		})
			.reduce(new ReduceFunction <Long>() {
				private static final long serialVersionUID = 5405245221868091031L;

				@Override
				public Long reduce(Long value1, Long value2) throws Exception {
					return value1 + value2;
				}
			});
	}

	public static DataSet <Tuple2 <Long, float[]>> initModelWithDataSet(
		DataSet <Row> vocab,
		DataSet <Row> init,
		DataSet <Tuple2 <Long, float[]>> model) {

		model = model.leftOuterJoin(
			/**
			 * function of this code block:
			 *
			 * 1. convert word to index.
			 * 2. getVector tensor to float array.
			 */
			vocab.join(init)
				.where(new KeySelector <Row, String>() {
					private static final long serialVersionUID = -8174662539973010918L;

					@Override
					public String getKey(Row value) throws Exception {
						return (String) value.getField(0);
					}
				})
				.equalTo(new KeySelector <Row, String>() {
					private static final long serialVersionUID = -8495357791284735430L;

					@Override
					public String getKey(Row value) throws Exception {
						return (String) value.getField(0);
					}
				})
				.with(new JoinFunction <Row, Row, Tuple2 <Long, float[]>>() {
					private static final long serialVersionUID = -2294750901999278320L;

					@Override
					public Tuple2 <Long, float[]> join(Row first, Row second) throws Exception {
						DenseVector vec = VectorUtil.parseDense((String) second.getField(1));
						double[] dvec = vec.getData();
						float[] fvec = new float[dvec.length];
						for (int i = 0; i < dvec.length; ++i) {
							fvec[i] = (float) dvec[i];
						}
						return new Tuple2 <>((Long) first.getField(2), fvec);
					}
				}))
			.where(0)
			.equalTo(0)
			.with(
				new JoinFunction <Tuple2 <Long, float[]>, Tuple2 <Long, float[]>, Tuple2 <Long, float[]>>() {
					private static final long serialVersionUID = -7804574255118420268L;

					@Override
					public Tuple2 <Long, float[]> join(Tuple2 <Long, float[]> first,
													   Tuple2 <Long, float[]> second) throws Exception {
						return second == null ? first : second;
					}
				});

		return model;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		final BatchOperator <?> in = inputs[0];

		BatchOperator <?> init = null;
		if (inputs.length > 1 && inputs[1] != null) {
			init = inputs[1];
		}

		final String contentColName = getSelectedCol();
		final String wordDelimiter = getWordDelimiter();
		final int vectorSize = getVectorSize();
		final int minCount = getMinCount();

		BatchOperator <?> metaPathOp = null;
		if (inputs.length > 2 && inputs[2] != null) {
			metaPathOp = inputs[2];
			getParams().set(METE_PATH_MODE, true);
		}

		ApsContext context = new ApsContext(getMLEnvironmentId()).put(getParams());

		DataSet <Row> wordCnt = WordCountUtil
			.splitDocAndCount(in, contentColName, wordDelimiter)
			.filter("cnt >= " + String.valueOf(minCount))
			.getDataSet();

		DataSet <Row> weighted = wordCnt
			.mapPartition(new MapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 3673494090420762051L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out)
					throws Exception {
					for (Row value : values) {
						out.collect(Row.of(value.getField(0), value.getField(1),
							Math.pow(((Long) value.getField(1)).doubleValue(), POWER)));
					}
				}
			})
			.returns(
				new RowTypeInfo(
					((RowTypeInfo) wordCnt.getType()).getTypeAt(0),
					((RowTypeInfo) wordCnt.getType()).getTypeAt(1),
					Types.DOUBLE
				)
			);

		if (getParams().get(METE_PATH_MODE)) {
			DataSet <Row> metaPathDataSet = metaPathOp.getDataSet();

			/*
			 * the most of fail in metapath is that user do not distinguish the
			 * keys in type table, so we should spend some cost to check it.
			 */
			DataSet <Tuple2 <String, Object>> metaPath = metaPathDataSet
				.map(new MapFunction <Row, Tuple3 <String, Object, Long>>() {
					private static final long serialVersionUID = 9096982986234491291L;

					@Override
					public Tuple3 <String, Object, Long> map(Row value) throws Exception {
						return Tuple3.of((String) value.getField(0), value.getField(1), 1L);
					}
				})
				.groupBy(0)
				.reduce(new ReduceFunction <Tuple3 <String, Object, Long>>() {
					private static final long serialVersionUID = 3040652618402549584L;

					@Override
					public Tuple3 <String, Object, Long> reduce(
						Tuple3 <String, Object, Long> value1,
						Tuple3 <String, Object, Long> value2) throws Exception {
						throw new IllegalArgumentException(
							"There are duplicated key with different type in the type table. " +
								"duplicated key: " + value1.f0
						);
					}
				})
				.map(new MapFunction <Tuple3 <String, Object, Long>, Tuple2 <String, Object>>() {
					private static final long serialVersionUID = 9089005828601878196L;

					@Override
					public Tuple2 <String, Object> map(Tuple3 <String, Object, Long> value) throws Exception {
						return Tuple2.of(value.f0, value.f1);
					}
				});

			weighted = weighted
				.join(metaPath)
				.where(new KeySelector <Row, String>() {
					private static final long serialVersionUID = -3260557286793094188L;

					@Override
					public String getKey(Row value) throws Exception {
						return (String) value.getField(0);
					}
				})
				.equalTo(0)
				.with(new JoinFunction <Row, Tuple2 <String, Object>, Row>() {
					private static final long serialVersionUID = -2561210954811247655L;

					@Override
					public Row join(Row first, Tuple2 <String, Object> second) throws Exception {
						return Row.of(
							first.getField(0),
							first.getField(1),
							first.getField(2),
							second.f1
						);
					}
				})
				.returns(
					new RowTypeInfo(
						((RowTypeInfo) weighted.getType()).getTypeAt(0),
						((RowTypeInfo) weighted.getType()).getTypeAt(1),
						((RowTypeInfo) weighted.getType()).getTypeAt(2),
						((RowTypeInfo) metaPathOp.getDataSet().getType()).getTypeAt(1)
					)
				);
		}

		Tuple3 <DataSet <Row>, DataSet <Long[]>, DataSet <long[]>> sortedIndexVocab
			= WordCountUtil.sortedIndexVocab(weighted, 1, getParams().get(METE_PATH_MODE));

		DataSet <Row> vocab = sortedIndexVocab.f0;

		context.put("negBound", sortedIndexVocab.f1);

		if (getParams().get(METE_PATH_MODE)) {
			context.put("groupIdxes", sortedIndexVocab.f2);
		}

		DataSet <String[]> split = in
			.select("`" + contentColName + "`")
			.getDataSet()
			.flatMap(new WordCountUtil.WordSpliter(wordDelimiter));

		DataSet <int[]> trainData = encodeContent(split, vocab);
		//.partitionCustom(new RandomPartitioner(), new RandomKeySelector());

		context.put("vocSize", countVoc(vocab));

		DataSet <Tuple2 <Long, float[]>> model = vocab
			.mapPartition(new InitVecs(0, vectorSize))
			.withBroadcastSet(context.getDataSet(), "w2vContext");

		if (init != null) {
			model = initModelWithDataSet(vocab, init.getDataSet(), model);
		}

		context.put("corpusRowCnt",
			DataSetUtils.countElementsPerPartition(trainData)
				.sum(1)
				.map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
					private static final long serialVersionUID = -6699143217778589517L;

					@Override
					public Long map(Tuple2 <Integer, Long> value) throws Exception {
						return value.f1;
					}
				})
		);

		if (!getParams().contains("batchSize")) {
			context.put("corpusWordCnt",
				trainData
					.mapPartition(
						new RichMapPartitionFunction <int[], Tuple2 <Integer, Long>>() {
							private static final long serialVersionUID = 3945808066159298068L;

							@Override
							public void mapPartition(Iterable <int[]> values, Collector <Tuple2 <Integer, Long>> out) {
								long cnt = 0L;
								for (int[] val : values) {
									cnt += val == null ? 0L : val.length;
								}
								out.collect(Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), cnt));
							}
						})
					.sum(1)
					.map(new MapFunction <Tuple2 <Integer, Long>, Long>() {
						private static final long serialVersionUID = 6513951971329287300L;

						@Override
						public Long map(Tuple2 <Integer, Long> value) throws Exception {
							return value.f1;
						}
					})
			);

			final int parallelism = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment()
				.getParallelism();

			context = context.map(new MapFunction <Params, Params>() {
				private static final long serialVersionUID = -2913069167264459122L;

				@Override
				public Params map(Params value) throws Exception {
					long corpusRowCnt = value.getLong("corpusRowCnt");
					long corpusWordCnt = value.getLong("corpusWordCnt");

					int batchSize;
					if (corpusRowCnt < 10000) {
						batchSize = (int) (corpusRowCnt / 2 + corpusRowCnt % 2);
					} else if (corpusRowCnt < 100000) {
						batchSize = (int) (corpusRowCnt / 5 + corpusRowCnt % 5);
					} else {

						int window = value.getIntegerOrDefault("window", 5);
						int negative = value.getIntegerOrDefault("negative", 5);
						int vectorSize = value.getIntegerOrDefault("vectorSize", 100);

						// 762 for sina news avg word per doc
						batchSize = new Double(
							Math.ceil(
								((double) corpusRowCnt * 762.0 * 6000.0 * parallelism * 5.0 * 10.0 * 100.0)
									/ ((double) corpusWordCnt * window * negative * vectorSize))
								/ 5.0
						).intValue();

						if (batchSize == 0) {
							batchSize = (int) corpusRowCnt;
						}
					}

					LOG.info("batchSize: {}, corpusWordCnt: {}, corpusRowCnt: {}", batchSize, corpusWordCnt,
						corpusRowCnt);

					return value.set(HasBatchSize.BATCH_SIZE, batchSize);
				}
			});
		}

		final int numCheckpoint = getParams().get(HasNumCheckpoint.NUM_CHECKPOINT);
		final int numIter = getNumIter();

		context = context.map(new MapFunction <Params, Params>() {
			private static final long serialVersionUID = -6203846043382163372L;

			@Override
			public Params map(Params param) throws Exception {
				int batchSize = param.getInteger("batchSize");
				long corpusRowCnt = param.getLong("corpusRowCnt");
				int div = (int) (corpusRowCnt / batchSize);
				int mod = (int) (corpusRowCnt % batchSize);

				int numMiniBatch;
				if (mod == 0) {
					numMiniBatch = div;
				} else {
					numMiniBatch = div + 1;
				}

				return param.set(ApsContext.ALINK_APS_NUM_MINI_BATCH, numMiniBatch);
			}
		});

		Tuple2 <DataSet <Tuple2 <Long, float[]>>, BatchOperator[]> res =
			apsEnv.iterate(
				model,
				trainData,
				context,
				new BatchOperator[] {vocab2Op(vocab)},
				true,
				numIter, numCheckpoint,
				getParams(),
				new ApsIteratorW2V(),
				new ApsEnv.PersistentHook <Tuple2 <Long, float[]>>() {
					@Override
					public DataSet <Tuple2 <Long, float[]>> hook(DataSet <Tuple2 <Long, float[]>> input) {
						return input.groupBy(0).withPartitioner(new RandomPartitioner())
							.reduceGroup(new GroupReduceFunction <Tuple2 <Long, float[]>, Tuple2 <Long, float[]>>() {
								private static final long serialVersionUID = 7317565679719956206L;

								@Override
								public void reduce(Iterable <Tuple2 <Long, float[]>> values,
												   Collector <Tuple2 <Long, float[]>> out) throws Exception {
									out.collect(values.iterator().next());
								}
							});
					}
				}
			);

		model = res.f0;
		vocab = res.f1[0].getDataSet();

		DataSet <Row> modelRows = model.map(
			new MapFunction <Tuple2 <Long, float[]>, Row>() {
				private static final long serialVersionUID = 3197724941351505348L;

				@Override
				public Row map(Tuple2 <Long, float[]> value) throws Exception {
					DenseVector dv = new DenseVector(value.f1.length);

					double[] output = dv.getData();
					for (int i = 0; i < value.f1.length; ++i) {
						output[i] = value.f1[i];
					}

					return Row.of(value.f0, VectorUtil.toString(dv));
				}
			}
		);

		// compatible with old vector format
		if (getParams().get(
			ParamInfoFactory
				.createParamInfo("compatibleVectorFormat", Boolean.class)
				.setOptional()
				.setHasDefaultValue(false)
				.build()
		)) {
			modelRows = modelRows.map(
				new MapFunction <Row, Row>() {
					private static final long serialVersionUID = -9050650174159636222L;

					@Override
					public Row map(Row value) throws Exception {
						return Row.of(value.getField(0), ((String) (value.getField(1))).replace(' ', ','));
					}
				}
			);
		}

		//model to display
		Table tableIndexVector = DataSetConversionUtil.toTable(getMLEnvironmentId(),
			modelRows,
			new String[] {"idx1", "vec"}, new TypeInformation <?>[] {Types.LONG, Types.STRING}
		);

		Table tableWordIndex = DataSetConversionUtil.toTable(getMLEnvironmentId(),
			vocab,
			new String[] {"word", "cnt", "idx2"}, new TypeInformation <?>[] {Types.STRING, Types.LONG, Types.LONG}
		);

		this.setOutputTable(tableIndexVector.join(tableWordIndex).where("idx1 = idx2").select("word, vec"));
		return (T) this;
	}

	private BatchOperator vocab2Op(DataSet <Row> vocab) {
		return new TableSourceBatchOp(DataSetConversionUtil.toTable(getMLEnvironmentId(),
			vocab, new String[] {"word", "cnt", "idx2"},
			new TypeInformation <?>[] {Types.STRING, Types.LONG, Types.LONG}))
			.setMLEnvironmentId(getMLEnvironmentId());
	}

	private static class RandomPartitioner implements Partitioner <Long> {
		private static final long serialVersionUID = 8077234360287783092L;

		@Override
		public int partition(Long key, int numPartitions) {
			return (int) (key % numPartitions);
		}
	}

}
