package com.alibaba.alink.operator.batch.associationrule;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortSpec.OpType;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.associationrule.FpTree;
import com.alibaba.alink.operator.common.associationrule.FpTreeImpl;
import com.alibaba.alink.params.associationrule.GroupedFpGrowthParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * batch op of grouped fp-growth
 */
@InputPorts(values = @PortSpec(value = PortType.DATA, opType = OpType.BATCH))
@OutputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.ASSOCIATION_PATTERNS),
	@PortSpec(value = PortType.MODEL, desc = PortDesc.ASSOCIATION_RULES),
})
@ParamSelectColumnSpec(name = "itemsCol",
	allowedTypeCollections = TypeCollections.STRING_TYPE)
@ParamSelectColumnSpec(name = "groupCol")
@NameCn("分组FPGrowth训练")
@NameEn("Grouped FpGrowth Training")
public final class GroupedFpGrowthBatchOp
	extends BatchOperator <GroupedFpGrowthBatchOp>
	implements GroupedFpGrowthParams <GroupedFpGrowthBatchOp> {

	private static final long serialVersionUID = -3434563610385164063L;

	public GroupedFpGrowthBatchOp() {
	}

	public GroupedFpGrowthBatchOp(Params params) {
		super(params);
	}

	@Override
	public GroupedFpGrowthBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String itemsCol = getItemsCol();
		final String groupedCol = getGroupCol();
		final int minSupportCnt = getMinSupportCount();
		final double minSupportPercent = getMinSupportPercent();
		final int maxPatternLength = getMaxPatternLength();
		final double minLift = getMinLift();
		final double minConfidence = getMinConfidence();

		in = in.select(new String[] {groupedCol, itemsCol});
		DataSet <Row> rows = in.getDataSet();
		DataSet <Tuple2 <String, Row>> withGroupId = rows
			.map(new MapFunction <Row, Tuple2 <String, Row>>() {
				private static final long serialVersionUID = -2408050110594809903L;

				@Override
				public Tuple2 <String, Row> map(Row value) throws Exception {
					String key = String.valueOf(value.getField(0));
					return Tuple2.of(key, value);
				}
			});

		DataSet <Tuple2 <String, Long>> groupCount = withGroupId
			.map(new MapFunction <Tuple2 <String, Row>, Tuple2 <String, Long>>() {
				private static final long serialVersionUID = -9076532747012786151L;

				@Override
				public Tuple2 <String, Long> map(Tuple2 <String, Row> value) throws Exception {
					return Tuple2.of(value.f0, 1L);
				}
			})
			.groupBy(0)
			.reduce(new ReduceFunction <Tuple2 <String, Long>>() {
				private static final long serialVersionUID = 6511332413000462792L;

				@Override
				public Tuple2 <String, Long> reduce(Tuple2 <String, Long> value1, Tuple2 <String, Long> value2)
					throws Exception {
					value1.f1 += value2.f1;
					return value1;
				}
			});

		DataSet <Tuple2 <String, Row>> patterns = withGroupId
			.groupBy(0)
			.reduceGroup(new RichGroupReduceFunction <Tuple2 <String, Row>, Tuple2 <String, Row>>() {
				private static final long serialVersionUID = -2758244844753267506L;

				@Override
				public void reduce(Iterable <Tuple2 <String, Row>> values, Collector <Tuple2 <String, Row>> out)
					throws Exception {
					Object key = null;
					List <String> transactions = new ArrayList <>();
					for (Tuple2 <String, Row> v : values) {
						key = v.f1.getField(0);
						transactions.add((String) v.f1.getField(1));
					}
					final Object constKey = key;
					final int minSupportCount = decideMinSupportCount(minSupportCnt, minSupportPercent,
						transactions.size());
					final Map <String, Integer> itemCounts = getItemCounts(transactions);
					final Tuple2 <Map <String, Integer>, List <String>> ordered = orderItems(itemCounts);
					final Map <String, Integer> tokenToIndex = ordered.f0;
					final List <String> orderedItems = ordered.f1;
					final int[] qualifiedItemIndices = getQualifiedItemIndices(itemCounts, tokenToIndex,
						minSupportCount);

					FpTree fpTree = new FpTreeImpl();
					fpTree.createTree();
					for (String transaction : transactions) {
						if (StringUtils.isNullOrWhitespaceOnly(transaction)) {
							continue;
						}
						String[] items = transaction.split(FpGrowthBatchOp.ITEM_SEPARATOR);
						Set <Integer> qualifiedItems = new HashSet <>(items.length);
						for (String item : items) {
							if (itemCounts.get(item) >= minSupportCount) {
								qualifiedItems.add(tokenToIndex.get(item));
							}
						}
						int[] t = toArray(qualifiedItems);
						Arrays.sort(t);
						fpTree.addTransaction(t);
					}

					//                    System.out.println("key: " + key);
					fpTree.initialize();
					fpTree.printProfile();

					fpTree.extractAll(qualifiedItemIndices, minSupportCount, maxPatternLength,
						new Collector <Tuple2 <int[], Integer>>() {
							@Override
							public void collect(Tuple2 <int[], Integer> record) {
								String itemset = indicesToTokens(record.f0, orderedItems);
								long supportCount = record.f1;
								long itemCount = record.f0.length;
								Row row = new Row(FpGrowthBatchOp.ITEMSETS_COL_NAMES.length + 1);
								row.setField(0, constKey);
								row.setField(1, itemset);
								row.setField(2, supportCount);
								row.setField(3, itemCount);
								out.collect(Tuple2.of(String.valueOf(constKey), row));
							}

							@Override
							public void close() {
							}
						});

					fpTree.destroyTree();
				}
			})
			.name("gen_patterns");

		DataSet <Row> rules = patterns
			.groupBy(0)
			.reduceGroup(new RichGroupReduceFunction <Tuple2 <String, Row>, Row>() {
				private static final long serialVersionUID = 3848228188758749261L;
				transient List <Tuple2 <String, Long>> bc;

				@Override
				public void open(Configuration parameters) throws Exception {
					bc = getRuntimeContext().getBroadcastVariable("groupCount");
				}

				@Override
				public void reduce(Iterable <Tuple2 <String, Row>> values, Collector <Row> out) throws Exception {
					Map <String, Long> patterns = new HashMap <>();
					String key = null;
					for (Tuple2 <String, Row> t2 : values) {
						key = t2.f0;
						patterns.put((String) t2.f1.getField(1), (Long) t2.f1.getField(2));
					}
					final String constKey = key;
					Long tranCnt = null;
					for (Tuple2 <String, Long> c : bc) {
						if (c.f0.equals(key)) {
							tranCnt = c.f1;
							break;
						}
					}
					Preconditions.checkArgument(tranCnt != null);
					final Long transactionCnt = tranCnt;

					patterns.forEach((k, v) -> {
						String[] items = k.split(FpGrowthBatchOp.ITEM_SEPARATOR);
						if (items.length > 1) {
							for (int i = 0; i < items.length; i++) {
								int n = 0;
								StringBuilder sbd = new StringBuilder();
								for (int j = 0; j < items.length; j++) {
									if (j == i) {
										continue;
									}
									if (n > 0) {
										sbd.append(FpGrowthBatchOp.ITEM_SEPARATOR);
									}
									sbd.append(items[j]);
									n++;
								}
								String ante = sbd.toString();
								String conseq = items[i];
								Long supportXY = v;
								Long supportX = patterns.get(ante);
								Long supportY = patterns.get(conseq);
								Preconditions.checkArgument(supportX != null);
								Preconditions.checkArgument(supportY != null);
								Preconditions.checkArgument(supportXY != null);
								double confidence = supportXY.doubleValue() / supportX.doubleValue();
								double lift = supportXY.doubleValue() * transactionCnt.doubleValue() / (
									supportX.doubleValue()
										* supportY.doubleValue());
								double support = supportXY.doubleValue() / transactionCnt.doubleValue();

								Row ruleOutput = new Row(7);
								ruleOutput.setField(0, constKey);
								ruleOutput.setField(1, ante + "=>" + conseq);
								ruleOutput.setField(2, (long) items.length);
								ruleOutput.setField(3, lift);
								ruleOutput.setField(4, support);
								ruleOutput.setField(5, confidence);
								ruleOutput.setField(6, supportXY);
								if (lift >= minLift && confidence >= minConfidence) {
									out.collect(ruleOutput);
								}
							}
						}
					});
				}
			})
			.withBroadcastSet(groupCount, "groupCount")
			.name("gen_rules");

		DataSet <Row> outputPatterns = patterns
			.map(new MapFunction <Tuple2 <String, Row>, Row>() {
				private static final long serialVersionUID = -4247869441801301592L;

				@Override
				public Row map(Tuple2 <String, Row> value) throws Exception {
					return value.f1;
				}
			});

		Table patternsTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), outputPatterns,
			ArrayUtils.addAll(new String[] {in.getColNames()[0]}, FpGrowthBatchOp.ITEMSETS_COL_NAMES),
			ArrayUtils.addAll(new TypeInformation[] {in.getColTypes()[0]}, FpGrowthBatchOp.ITEMSETS_COL_TYPES));

		Table rulesTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), rules,
			ArrayUtils.addAll(new String[] {in.getColNames()[0]}, FpGrowthBatchOp.RULES_COL_NAMES),
			ArrayUtils.addAll(new TypeInformation[] {in.getColTypes()[0]}, FpGrowthBatchOp.RULES_COL_TYPES));

		this.setOutputTable(patternsTable);
		this.setSideOutputTables(new Table[] {rulesTable});
		return this;
	}

	private static int decideMinSupportCount(int minSupportCnt, double minSupportPercent, int transactionCount) {
		if (minSupportCnt >= 0) {
			return minSupportCnt;
		}
		return (int) Math.floor(transactionCount * minSupportPercent);
	}

	private static Map <String, Integer> getItemCounts(List <String> transactions) {
		Map <String, Integer> itemCounts = new HashMap <>();
		for (String transaction : transactions) {
			if (StringUtils.isNullOrWhitespaceOnly(transaction)) {
				continue;
			}
			String[] items = transaction.split(FpGrowthBatchOp.ITEM_SEPARATOR);
			Set <String> itemSet = new HashSet <>();
			itemSet.addAll(Arrays.asList(items));
			for (String item : itemSet) {
				itemCounts.merge(item, 1, ((a, b) -> a + b));
			}
		}
		return itemCounts;
	}

	private static Tuple2 <Map <String, Integer>, List <String>> orderItems(Map <String, Integer> itemCounts) {
		List <String> allItems = new ArrayList <>(itemCounts.size());
		itemCounts.forEach((k, v) -> {
			allItems.add(k);
		});
		allItems.sort(new Comparator <String>() {
			@Override
			public int compare(String o1, String o2) {
				return Integer.compare(itemCounts.get(o2), itemCounts.get(o1));
			}
		});
		Map <String, Integer> tokenToIndex = new HashMap <>(itemCounts.size());
		for (int i = 0; i < allItems.size(); i++) {
			tokenToIndex.put(allItems.get(i), i);
		}
		return Tuple2.of(tokenToIndex, allItems);
	}

	private static int[] toArray(Set <Integer> list) {
		int[] array = new int[list.size()];
		int n = 0;
		for (Integer i : list) {
			array[n++] = i;
		}
		return array;
	}

	private static int[] getQualifiedItemIndices(Map <String, Integer> itemCounts, Map <String, Integer> tokenToIndex,
												 int minSupportCount) {
		List <String> qualified = new ArrayList <>();
		itemCounts.forEach((k, v) -> {
			if (v >= minSupportCount) {
				qualified.add(k);
			}
		});
		int[] indices = new int[qualified.size()];
		for (int i = 0; i < qualified.size(); i++) {
			indices[i] = tokenToIndex.get(qualified.get(i));
		}
		return indices;
	}

	private static String indicesToTokens(int[] items, List <String> orderedItems) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < items.length; i++) {
			if (i > 0) {
				sbd.append(FpGrowthBatchOp.ITEM_SEPARATOR);
			}
			sbd.append(orderedItems.get(items[i]));
		}
		return sbd.toString();
	}

}

