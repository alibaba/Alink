package com.alibaba.alink.common.insights;

import com.alibaba.alink.operator.local.LocalOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EasilyInferableInsights {

	// check if a set of columns determine another column, given a set of basicFDs
	public static boolean isDependent(LocalOperator<?> source, Set <String> determinantCols, String col, String[] basicFDs) {
		Map <String, Boolean> inspected = new HashMap <>();
		return qualify(source, determinantCols, col, inspected, basicFDs);
	}

	// check if a set of columns determine another column recursively
	public static boolean qualify(LocalOperator<?> source, Set <String> determinantCols, String col, Map <String, Boolean> inspected,
								  String[] basicFDs) {
		// reflexivity axiom
		if (determinantCols.contains(col)) {
			return true;
		}

		// this column has already been inspected
		if (inspected.containsKey(col)) {
			return inspected.get(col);
		}

		inspected.put(col, false);

		// retrieve all the determinant sets of col. it is possible that one
		// column can be determined by multiple determinant sets
		List <Set <String>> dtSets = getAllDeterminants(source, col, basicFDs);

		for (Set <String> set : dtSets) {
			boolean qualify = true;

			// if all the columns within this set can be determined,
			// then col can be determined according to transitivity axiom */
			for (String newCol : set) {
				if (!qualify(source, determinantCols, newCol, inspected, basicFDs)) {
					qualify = false;
					break;
				}
			}

			if (qualify) {
				inspected.put(col, true);
				return true;
			}
		}
		return false;
	}

	public static List <Set <String>> getAllDeterminants(LocalOperator<?> source, String col, String[] basicFDs) {
		// todo
		return null;
	}
}
