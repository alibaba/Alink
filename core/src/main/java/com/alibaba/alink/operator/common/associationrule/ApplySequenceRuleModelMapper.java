package com.alibaba.alink.operator.common.associationrule;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.operator.batch.associationrule.PrefixSpanBatchOp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The model mapper of applying Sequence Rules.
 */
public class ApplySequenceRuleModelMapper extends SISOModelMapper {
	private static final long serialVersionUID = 2458317592464336059L;
	private transient List <List <Set <String>>> antecedents;
	private transient List <String> consequent;

	public ApplySequenceRuleModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	private static List <Set <String>> parseSequenceString(String seqStr) {
		String[] elements = seqStr.split(PrefixSpanBatchOp.ELEMENT_SEPARATOR);
		List <Set <String>> sequence = new ArrayList <>(elements.length);
		for (String element : elements) {
			sequence.add(new HashSet <>(Arrays.asList(element.split(PrefixSpanBatchOp.ITEM_SEPARATOR))));
		}
		return sequence;
	}

	private static boolean isSubSequence(List <Set <String>> seq, List <Set <String>> sub) {
		if (sub.size() > seq.size()) {
			return false;
		}
		int subLen = sub.size();
		int seqPos = 0;
		int numMatchedElement = 0;
		for (Set <String> aSub : sub) {
			while (seqPos < seq.size()) {
				if (isSubset(seq.get(seqPos++), aSub)) {
					numMatchedElement++;
					break;
				}
			}
		}
		return numMatchedElement == subLen;
	}

	private static boolean isSubset(Set <String> set, Set <String> sub) {
		return set.containsAll(sub);
	}

	@Override
	protected TypeInformation initPredResultColType() {
		return Types.STRING;
	}

	@Override
	protected Object predictResult(Object input) throws Exception {
		List <Set <String>> sequence = parseSequenceString((String) input);
		Set <String> prediction = new HashSet <>();
		for (int i = 0; i < antecedents.size(); i++) {
			if (isSubSequence(sequence, antecedents.get(i))) {
				String consequent = this.consequent.get(i);
				List <Set <String>> pred = new ArrayList <>();
				pred.addAll(antecedents.get(i));
				pred.addAll(parseSequenceString(consequent));
				if (!isSubSequence(sequence, pred)) {
					prediction.add(consequent);
				}
			}
		}
		StringBuilder sbd = new StringBuilder();
		int cnt = 0;
		for (String p : prediction) {
			if (cnt > 0) {
				sbd.append(PrefixSpanBatchOp.ELEMENT_SEPARATOR);
			}
			sbd.append(p);
			cnt++;
		}
		return sbd.toString();
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		final int numRules = modelRows.size();
		this.antecedents = new ArrayList <>(numRules);
		this.consequent = new ArrayList <>(numRules);
		modelRows.forEach(row -> {
			String[] rule = ((String) row.getField(0)).split(PrefixSpanBatchOp.RULE_SEPARATOR);
			String consequentStr = rule[1];
			String antecedentStr = rule[0];
			this.antecedents.add(parseSequenceString(antecedentStr));
			this.consequent.add(consequentStr);
		});
	}
}