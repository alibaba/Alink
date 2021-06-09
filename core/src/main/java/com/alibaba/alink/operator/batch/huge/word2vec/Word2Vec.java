package com.alibaba.alink.operator.batch.huge.word2vec;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.ExpTableArray;
import com.alibaba.alink.operator.common.nlp.WordCountUtil;
import com.alibaba.alink.params.nlp.Word2VecParams;
import com.github.fommil.netlib.BLAS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Word2Vec {
	private final static Logger LOG = LoggerFactory.getLogger(Word2Vec.class);

	private Integer window;
	private int negative;
	private int vectorSize;
	private int vocSize;
	private boolean isRandomWindow;
	private float alpha;

	private Long[] nsPool;
	private Object[] groupIdxObjs;
	private long[] groupIdxStarts;

	private Random random = new Random();

	public Word2Vec(Params params,
					int vocSize,
					Long[] pool,
					Object[] groupIdxObjs,
					long[] groupIdxStarts) {
		this.window = params.get(Word2VecParams.WINDOW);
		this.negative = params.get(Word2VecParams.NEGATIVE);
		this.vectorSize = params.get(Word2VecParams.VECTOR_SIZE);
		this.alpha = params.get(Word2VecParams.ALPHA).floatValue();

		this.isRandomWindow = true;
		if (params.get(Word2VecParams.RANDOM_WINDOW).toLowerCase().equals("false")) {
			this.isRandomWindow = false;
		}

		this.vocSize = vocSize;
		nsPool = pool;
		this.groupIdxObjs = groupIdxObjs;
		this.groupIdxStarts = groupIdxStarts;
	}

	public static int ns(Random random, Long[] bound, int obj, long[] groupIdxStarts) {
		int start = 0;
		int end = bound.length - 1;

		if (groupIdxStarts != null) {
			int idx = Arrays.binarySearch(groupIdxStarts, obj);
			if (idx < 0) {
				idx = -idx - 2;
			}
			start = idx * (WordCountUtil.BOUND_SIZE + 1);
			end = WordCountUtil.BOUND_SIZE;
		}

		double multi = random.nextDouble() * end;
		double floor = Math.floor(multi);
		double minus = multi - floor;
		int pos = start + (int) floor;

		return (int) ((Math.round((bound[pos + 1] - bound[pos]) * minus) + bound[pos]));
	}

	public void getIndexes(long seed, List <int[]> content, Set <Long> output) {
		int a = 0, b = 0, c = 0, d = 0;
		int target = -1;
		int word = -1;
		int lastWord = -1;
		List <int[]> docs = content;

		random.setSeed(seed);
		for (int[] words : docs) {
			int senLen = words.length;
			for (int i = 0; i < senLen; ++i) {
				word = words[i];

				for (d = 0; d < negative + 1; ++d) {
					if (d == 0) {
						target = word;
					} else {
						target = ns(random, nsPool, word, groupIdxStarts);
						if (target == word) {
							continue;
						}
					}

					output.add(Long.valueOf(target + this.vocSize));
				}

				if (isRandomWindow) {
					b = random.nextInt(window);
				}

				for (a = b; a < window * 2 + 1 - b; ++a) {
					if (a != window) {
						c = i - window + a;
						if (c < 0 || c >= senLen) {
							continue;
						}

						lastWord = words[c];
						output.add(Long.valueOf(lastWord));
					}
				}
			}
		}
	}

	public void train3(long seed, List <int[]> content,
					   float[] buffer, Map <Long, Integer> mapFeatureId2Local) {

		int a, b = 0, c, d;
		int target;
		int word;
		float loss = 0.0f;
		float nInst = 0.0f;
		float score;

		random.setSeed(seed);

		float[] grads = new float[this.vectorSize];
		float[] fVec = new float[this.negative + 1];
		float[] negVec = new float[this.vectorSize * (this.negative + 1)];
		int[] negIdxVec = new int[this.negative];

		int total = 0;
		for (int[] words : content) {
			total += words.length;
			int senLen = words.length;
			for (int i = 0; i < senLen; ++i) {
				word = mapFeatureId2Local.get(Long.valueOf(words[i] + this.vocSize));
				System.arraycopy(buffer, word * vectorSize,
					negVec, 0, vectorSize);

				int negLen = 0;
				for (d = 0; d < negative; ++d) {
					target = ns(random, nsPool, words[i], groupIdxStarts);
					if (target == words[i]) {
						continue;
					}
					negIdxVec[negLen] = mapFeatureId2Local.get(Long.valueOf(target + this.vocSize));
					System.arraycopy(buffer,
						negIdxVec[negLen] * vectorSize,
						negVec, (negLen + 1) * vectorSize, vectorSize);

					negLen++;
				}

				int outputLen = negLen + 1;

				if (isRandomWindow) {
					b = random.nextInt(window);
				}

				for (a = b; a < window * 2 + 1 - b; ++a) {
					if (a != window) {
						c = i - window + a;
						if (c < 0 || c >= senLen) {
							continue;
						}
						nInst += 1.0f;

						int lastWordPos = mapFeatureId2Local.get(Long.valueOf(words[c])) * vectorSize;

						BLAS.getInstance().sgemv(
							"t", vectorSize, outputLen, 1.0f, negVec, 0, vectorSize,
							buffer, lastWordPos, 1, 0, fVec, 0, 1);

						score = ExpTableArray.sigmoid(fVec[0]);
						fVec[0] = (1.0f - score) * alpha;
						loss += -ExpTableArray.log(score);
						for (int t = 1; t < outputLen; ++t) {
							score = ExpTableArray.sigmoid(fVec[t]);
							fVec[t] = -score * alpha;
							loss += -ExpTableArray.log(1.0f - score);
						}

						BLAS.getInstance().sgemv("n", vectorSize, outputLen, 1.0f, negVec, 0, vectorSize,
							fVec, 0, 1, 0, grads, 0, 1);

						BLAS.getInstance().sgemm("n", "n", vectorSize, outputLen, 1, 1.0f, buffer, lastWordPos,
							vectorSize,
							fVec, 0, 1, 1.0f, negVec, 0, vectorSize);

						BLAS.getInstance().saxpy(vectorSize, 1.0f, grads, 0, 1, buffer, lastWordPos, 1);
					}
				}

				System.arraycopy(negVec, 0, buffer, word * vectorSize, vectorSize);
				for (int z = 0; z < negLen; ++z) {
					System.arraycopy(negVec, (z + 1) * vectorSize, buffer, negIdxVec[z] * vectorSize, vectorSize);
				}
			}
		}

		LOG.info("total: {}, len: {}, loss: {}", total, content.size(), loss / nInst);
	}
}
