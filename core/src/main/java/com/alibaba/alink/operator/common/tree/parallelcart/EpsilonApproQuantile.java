package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Almost copy from xgboost.
 */
public final class EpsilonApproQuantile {
	private static final Logger LOG = LoggerFactory.getLogger(EpsilonApproQuantile.class);

	public final static class Entry {
		public double rMin;
		public double rMax;
		public double weight;
		public double value;

		public Entry(double rMin, double rMax, double weight, double value) {
			this.rMin = rMin;
			this.rMax = rMax;
			this.weight = weight;
			this.value = value;
		}

		public double rMinNext() {
			return rMin + weight;
		}

		public double rMaxPre() {
			return rMax - weight;
		}
	}

	public final static class WQSummary {
		public ArrayList <Entry> entries = new ArrayList <>();

		public void setCombine(WQSummary sa, WQSummary sb) {
			entries.clear();

			if (sa.entries.size() == 0) {
				entries.addAll(sb.entries);
				return;
			}

			if (sb.entries.size() == 0) {
				entries.addAll(sa.entries);
				return;
			}

			int a = 0, aEnd = sa.entries.size();
			int b = 0, bEnd = sb.entries.size();
			double aPrevMin = 0, bPrevMin = 0;

			while (a != aEnd && b != bEnd) {
				Entry ea = sa.entries.get(a);
				Entry eb = sb.entries.get(b);

				if (ea.value == eb.value) {
					entries.add(
						new Entry(
							ea.rMin + eb.rMin,
							ea.rMax + eb.rMax,
							ea.weight + eb.weight,
							ea.value
						)
					);

					aPrevMin = ea.rMinNext();
					bPrevMin = eb.rMinNext();
					a++;
					b++;
				} else if (ea.value < eb.value) {
					entries.add(
						new Entry(
							ea.rMin + bPrevMin,
							ea.rMax + eb.rMaxPre(),
							ea.weight,
							ea.value
						)
					);

					aPrevMin = ea.rMinNext();
					a++;
				} else {
					entries.add(
						new Entry(
							eb.rMin + aPrevMin,
							eb.rMax + ea.rMaxPre(),
							eb.weight,
							eb.value
						)
					);

					bPrevMin = eb.rMinNext();
					b++;
				}
			}

			if (a != aEnd) {
				do {
					double bRMax = lastValue(sb.entries).rMax;
					Entry ea = sa.entries.get(a);
					entries.add(
						new Entry(
							ea.rMin + bPrevMin,
							ea.rMax + bRMax,
							ea.weight,
							ea.value
						)
					);
					a++;
				} while (a != aEnd);
			}

			if (b != bEnd) {
				do {
					double aRMax = lastValue(sa.entries).rMax;
					Entry eb = sb.entries.get(b);
					entries.add(
						new Entry(
							eb.rMin + aPrevMin,
							eb.rMax + aRMax,
							eb.weight,
							eb.value
						)
					);
					b++;
				} while (b != bEnd);
			}

			double tol = 10;

			double prevRMin = 0, prevRMax = 0;
			double errMinGap = 0, errMaxGap = 0, errWGap = 0;

			for (Entry e : entries) {
				if (e.rMin < prevRMin) {
					e.rMin = prevRMin;
					errMinGap = Math.max(errMinGap, prevRMin - e.rMin);
				} else {
					prevRMin = e.rMin;
				}

				if (e.rMax < prevRMax) {
					e.rMax = prevRMax;
					errMaxGap = Math.max(errMaxGap, prevRMax - e.rMax);
				}

				double rMinNext = e.rMinNext();

				if (e.rMax < rMinNext) {
					e.rMax = rMinNext;
					errWGap = Math.max(errWGap, e.rMax - rMinNext);
				}

				prevRMax = e.rMax;
			}

			if (errMinGap > tol || errMaxGap > tol || errWGap > tol) {
				LOG.info("mingap = {}, maxgap = {}, wgap = {}", errMinGap, errMaxGap, errWGap);
			}
		}

		public void setPrune(WQSummary src, int maxSize) {
			entries.clear();
			if (src.entries.size() <= maxSize) {
				entries.addAll(src.entries);

				return;
			}

			double begin = firstValue(src.entries).rMax;
			double range = lastValue(src.entries).rMin - begin;

			if (range == 0.0f || maxSize <= 2) {
				entries.add(firstValue(src.entries));
				entries.add(lastValue(src.entries));
				return;
			} else {
				range = Math.max(range, 1e-3f);
			}

			int n = maxSize - 2, nBig = 0;
			double chunk = 2 * range / n;
			double mRange = 0;
			int bid = 0;

			for (int i = 1; i < src.entries.size() - 1; ++i) {
				if (src.entries.get(i).rMinNext() > (src.entries.get(i).rMaxPre() + chunk)) {
					if (bid != i - 1) {
						mRange += src.entries.get(i).rMaxPre() - src.entries.get(bid).rMinNext();
					}

					bid = i;
					++nBig;
				}
			}

			if (bid != src.entries.size() - 2) {
				mRange += lastValue(src.entries).rMaxPre() - src.entries.get(bid).rMinNext();
			}

			if (nBig >= n) {
				throw new RuntimeException("quantile: too many large chunk");
			}

			entries.add(firstValue(src.entries));
			n = n - nBig;
			bid = 0;

			int k = 1, lastIdx = 0;

			for (int end = 1; end < src.entries.size(); ++end) {
				if (end == src.entries.size() - 1
					|| src.entries.get(end).rMinNext() > (src.entries.get(end).rMaxPre() + chunk)) {
					if (bid != end - 1) {
						int i = bid;
						double maxDX2 = src.entries.get(end).rMaxPre() * 2;
						for (; k < n; ++k) {
							double dx2 = 2 * ((k * mRange) / n + begin);

							if (dx2 >= maxDX2) {
								break;
							}
							while (i < end && dx2 >= src.entries.get(i + 1).rMax + src.entries.get(i + 1).rMin) {
								++i;
							}

							if (i == end) {
								break;
							}
							if (dx2 < src.entries.get(i).rMinNext() + src.entries.get(i + 1).rMaxPre()) {
								if (i != lastIdx) {
									entries.add(src.entries.get(i));
									lastIdx = i;
								}
							} else {
								if (i + 1 != lastIdx) {
									entries.add(src.entries.get(i + 1));
									lastIdx = i + 1;
								}
							}
						}
					}

					if (lastIdx != end) {
						entries.add(src.entries.get(end));
						lastIdx = end;
					}

					bid = end;

					begin += src.entries.get(bid).rMinNext() - src.entries.get(bid).rMaxPre();
				}
			}
		}

		static <T> T firstValue(List <T> list) {
			return list.get(0);
		}

		static <T> T lastValue(List <T> list) {
			return list.get(list.size() - 1);
		}
	}

	public final static class QuantileSketch {
		public int nLevel;
		public int limitSize;
		public ArrayList <WQSummary> level = new ArrayList <>();
		public WQSummary temp = new WQSummary();

		/**
		 * Calc the level size.
		 *
		 * @param maxN maximum number of instance.
		 * @param eps  eps.
		 */
		public void limitSizeLevel(int maxN, double eps) {
			nLevel = 1;

			while (true) {
				/*
				  Summary with b memory budget is a (eps + 1 / b). The initial eps is 0, so nLevel's eps is
				  max(1/b, 2/b, ..., nLevel/b) = nLevel / b.
				  when nLevel / b = eps, the b is nLevel / eps.
				 */
				limitSize = (int) (Math.ceil(nLevel / eps) + 1);
				limitSize = Math.min(maxN, limitSize);
				int n = 1 << nLevel;
				if (n * limitSize >= maxN) {
					break;
				}
				++nLevel;
			}

			if ((1 << nLevel) * limitSize < maxN) {
				throw new IllegalArgumentException("Invalid params. maxN: " + maxN + ", eps: " + eps);
			}

			if (nLevel > Math.max(1, limitSize * eps)) {
				throw new IllegalArgumentException("Invalid params. maxN: " + maxN + ", eps: " + eps);
			}
		}

		public void pushTemp() {
			WQSummary tempSummary = new WQSummary();
			tempSummary.setPrune(temp, limitSize);
			level.add(tempSummary);
		}

		public void getSummary(WQSummary output) {
			output.entries = level == null || level.isEmpty() ? new ArrayList <>() : level.get(0).entries;
		}
	}

	public final static class SketchEntry {
		public double sumTotal;
		public double rMin;
		public double wMin;
		public double lastValue;
		public double nextGoal;

		public QuantileSketch sketch = new QuantileSketch();

		public void init(int maxSize) {
			nextGoal = -1.0;
			rMin = wMin = 0.0f;
			sketch.temp.entries.ensureCapacity(maxSize);
			sketch.temp.entries.clear();
			sketch.level.clear();
		}

		public void push(double fValue, double w, int maxSize) {
			if (nextGoal == -1.0) {
				nextGoal = 0.0f;
				lastValue = fValue;
				wMin = w;
				return;
			}

			if (lastValue != fValue) {
				double rMax = rMin + wMin;

				if (rMax >= nextGoal && sketch.temp.entries.size() != maxSize) {
					if (sketch.temp.entries.size() == 0
						|| lastValue > WQSummary.lastValue(sketch.temp.entries).value) {
						sketch.temp.entries.add(new Entry(rMin, rMax, wMin, lastValue));
					}

					if (sketch.temp.entries.size() == maxSize) {
						nextGoal = 2 * sumTotal;
					} else {
						nextGoal += sketch.temp.entries.size() * sumTotal / maxSize;
					}
				} else {
					// pass
					if (rMax >= nextGoal) {
						LOG.info(
							"INFO: rMax={}, rMin={}, total={}, size={}, maxSize={}",
							rMax, rMin, sumTotal, sketch.temp.entries.size(), maxSize
						);
					}
				}

				rMin = rMax;
				wMin = w;
				lastValue = fValue;
			} else {
				wMin += w;
			}
		}

		public void finalize(int maxSize) {
			double rMax = rMin + wMin;

			if (sketch.temp.entries.size() == 0 || lastValue > WQSummary.lastValue(sketch.temp.entries).value) {
				Preconditions.checkState(sketch.temp.entries.size() < maxSize);
				sketch.temp.entries.add(new Entry(rMin, rMax, wMin, lastValue));
			}

			sketch.pushTemp();
		}
	}
}
