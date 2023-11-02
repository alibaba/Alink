package com.alibaba.alink.operator.common.feature.address;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Areas {
	private static final String AREAS_PATH = "/areas.txt";
	static List <Area> AREAS = new ArrayList <>();

	static {
		InputStream is = Areas.class.getResourceAsStream(AREAS_PATH);
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = br.readLine()) != null) {
				if (line.length() > 0) {
					String[] strs = line.split(",");
					Area area = new Area();
					area.areaId = Integer.parseInt(strs[0].trim());
					area.areaName = strs[1].trim();
					area.areaParentId = Integer.parseInt(strs[2].trim());
					area.postcode = strs[3].trim();
					area.areaDeep = Integer.parseInt(strs[4].trim());
					AREAS.add(area);
				}
			}
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List <Area> fuzzyQuery(List <Area> areas, int areaDepth, String areaName) {
		List <Area> result = new ArrayList <>();
		for (Area area : areas) {
			int curAreaDepth = area.areaDeep;
			if (areaDepth == curAreaDepth) {
				if (area.areaName.contains(areaName)) {
					result.add(area);
				}

			}
		}
		return result;
	}

	public static List <Area> fuzzyQuery(int areaDepth, String areaName) {
		return fuzzyQuery(AREAS, areaDepth, areaName);
	}

	public static Area getArea(int id) {
		for (Area area : AREAS) {
			if (area.areaId == id) {
				return area;
			}
		}
		return null;
	}

	static class Area {
		int areaId;
		String areaName;
		int areaParentId;
		String postcode;

		int areaDeep;
	}

}
