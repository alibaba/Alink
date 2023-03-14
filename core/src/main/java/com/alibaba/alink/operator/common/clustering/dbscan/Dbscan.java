package com.alibaba.alink.operator.common.clustering.dbscan;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.distance.FastDistance;

import java.util.ArrayList;
import java.util.List;

/**
 * @author guotao.gt
 */
@NameCn("Dbscan训练")
public class Dbscan {
	public static final int UNCLASSIFIED = -1;
	public static final int NOISE = Integer.MIN_VALUE;

	public static List <DbscanNewSample> findNeighbors(Iterable <DbscanNewSample> values, DbscanNewSample sample,
													   double epsilon,
													   FastDistance baseDistance) {
		List <DbscanNewSample> epsilonDenseVectorList = new ArrayList <>();
		for (DbscanNewSample record : values) {
			if (baseDistance.calc(record.getVec(), sample.getVec()).get(0, 0) <= epsilon) {
				epsilonDenseVectorList.add(record);
			}
		}
		return epsilonDenseVectorList;
	}

	/**
	 * Assigns this sample to a cluster or remains it as NOISE
	 *
	 * @param sample The DbscanNewSample that needs to be assigned
	 * @return true, if the DbscanNewSample could be assigned, else false
	 */
	public static boolean expandCluster(Iterable <DbscanNewSample> values, DbscanNewSample sample, int clusterId,
										double epsilon, int minPoints, FastDistance distance) {
		List <DbscanNewSample> neighbors = findNeighbors(values, sample, epsilon, distance);
		/** sample is NOT CORE */
		if (neighbors.size() < minPoints) {
			sample.setType(Type.NOISE);
			sample.setClusterId(NOISE);
			return false;
		} else {
			/** sample is CORE */
			sample.setType(Type.CORE);
			for (int i = 0; i < neighbors.size(); i++) {
				DbscanNewSample neighbor = neighbors.get(i);
				/** label this neighbor with the current clusterId */
				neighbor.setClusterId(clusterId);
				if (neighbor.equals(sample)) {
					neighbors.remove(i);
					i--;
				}
			}

			/** Iterate the neighbors, add the UNCLASSIFIED sample to neighbors */
			for (int j = 0; j < neighbors.size(); j++) {
				List <DbscanNewSample> indirectNeighbours = findNeighbors(values, neighbors.get(j), epsilon, distance);

				/** neighbor is CORE */
				if (indirectNeighbours.size() >= minPoints) {
					neighbors.get(j).setType(Type.CORE);
					for (int k = 0; k < indirectNeighbours.size(); k++) {
						DbscanNewSample indirectNeighbour = indirectNeighbours.get(k);
						if (indirectNeighbour.getClusterId() == UNCLASSIFIED
							|| indirectNeighbour.getType() == Type.NOISE) {
							if (indirectNeighbour.getClusterId() == UNCLASSIFIED) {
								neighbors.add(indirectNeighbour);
							}
							if (indirectNeighbour.getType() == Type.NOISE) {
								indirectNeighbour.setType(Type.LINKED);
							}
							indirectNeighbour.setClusterId(clusterId);
						}
					}
				} else {
					neighbors.get(j).setType(Type.LINKED);
				}
				neighbors.remove(j);
				j--;
			}

			return true;
		}
	}
}
