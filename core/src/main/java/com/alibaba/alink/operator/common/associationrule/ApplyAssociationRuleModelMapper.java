package com.alibaba.alink.operator.common.associationrule;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.operator.batch.associationrule.FpGrowthBatchOp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The model mapper of applying the Association Rules.
 */
public class ApplyAssociationRuleModelMapper extends SISOModelMapper {
	private static final long serialVersionUID = 3709131767975976366L;
	private final String sep = FpGrowthBatchOp.ITEM_SEPARATOR;
	private transient List <Set <String>> antecedents;
	private transient List <String> consequences;

	public ApplyAssociationRuleModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected TypeInformation initPredResultColType() {
		return Types.STRING;
	}

	@Override
	protected Object predictResult(Object input) throws Exception {
		Set <String> items = new HashSet <>(Arrays.asList(((String) input).split(sep)));
		Set <String> prediction = new HashSet <>();
		for (int i = 0; i < antecedents.size(); i++) {
			if (items.containsAll(antecedents.get(i))) {
				String consequent = consequences.get(i);
				if (!items.contains(consequent)) {
					prediction.add(consequent);
				}
			}
		}
		StringBuilder sbd = new StringBuilder();
		int cnt = 0;
		for (String p : prediction) {
			if (cnt > 0) {
				sbd.append(sep);
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
		this.consequences = new ArrayList <>(numRules);
		modelRows.forEach(row -> {
			String[] rule = ((String) row.getField(0)).split("=>");
			this.consequences.add(rule[1]);
			this.antecedents.add(new HashSet <>(Arrays.asList(rule[0].split(sep))));
		});
	}
}