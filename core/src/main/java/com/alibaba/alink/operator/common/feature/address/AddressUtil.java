package com.alibaba.alink.operator.common.feature.address;

import com.alibaba.alink.operator.common.feature.address.Areas.Area;

import java.io.Serializable;
import java.util.List;

public class AddressUtil {
	public static Address parseAddress(String addressStr) {
		addressStr = addressStr.replace(" ", "")
			.replace("自治区", "省")
			.replace("自治州", "州");

		// 如果同时出现 县和区 我们可以确定的是县一定在区前面，所以下面三个if顺序是有要求的，不能随便调整
		int deep3_keyword_pos = 0;
		String deep3_area_name = "";
		int city_pos;
		int zone_pos;
		String street;

		/**
		 * 1. 三级地址识别 共有2992个三级地址 高频词为【县，区，旗，市】是整个识别系统的关键
		 * 返回 [%第3级% 模糊地址] [街道地址]
		 * 三级地址 前面一般2或3个字符就够用了【3个字符，比如高新区，仁和区，武侯区，占比96%】【2个字符的县和区有140个左右，占比4%，比如理县】
		 */
		if (addressStr.contains("县") || addressStr.contains("区") || addressStr.contains("旗")) {
			if (addressStr.contains("旗")) {
				deep3_keyword_pos = addressStr.indexOf("旗");
				deep3_area_name = addressStr.substring(Math.max(0, deep3_keyword_pos - 1), 2);
			}

			if (addressStr.contains("区")) {
				deep3_keyword_pos = addressStr.indexOf("区");

				// 判断区、市是同时存在
				if (addressStr.contains("市")) {
					city_pos = addressStr.indexOf("市");
					zone_pos = addressStr.indexOf("区");
					deep3_area_name = addressStr.substring(city_pos + 1, zone_pos);
				} else {
					deep3_area_name = addressStr.substring(Math.max(0, deep3_keyword_pos - 2), deep3_keyword_pos + 1);
					//县名称最大的概率为3个字符 美姑县 阆中市 高新区
				}
			}

			if (addressStr.contains("县")) {
				deep3_keyword_pos = addressStr.indexOf("县");
				// 判断县市是同时存在 同时存在 可以简单 比如【湖南省常德市澧县】
				if (addressStr.contains("市")) {
					city_pos = addressStr.indexOf("市");
					zone_pos = addressStr.indexOf("县");
					deep3_area_name = addressStr.substring(city_pos + 1, zone_pos);
				} else {
					//考虑形如【甘肃省东乡族自治县布楞沟村1号】的情况
					if (addressStr.contains("自治县")) {
						deep3_area_name = addressStr.substring(Math.max(0, deep3_keyword_pos - 6), 7);
						String subAdd = deep3_area_name.substring(0, 1);
						if ("省".equals(subAdd) || "市".equals(subAdd) || "州".equals(subAdd)) {
							deep3_area_name = deep3_area_name.substring(1);
						}
					} else {
						deep3_area_name = addressStr.substring(Math.max(0, deep3_keyword_pos - 2),
							deep3_keyword_pos + 1);
					}
					//县名称最大的概率为3个字符 美姑县 阆中市 高新区
				}
			}

			street = addressStr.substring(deep3_keyword_pos + 1);
		} else {
			if (addressStr.contains("市")) {
				//最大的可能性为县级市 可能的情况有【四川省南充市阆中市公园路25号，四川省南充市阆中市公园路25号】市要找【最后一次】出现的位置
				deep3_keyword_pos = addressStr.indexOf("市");
				deep3_area_name = addressStr.substring(Math.max(0, deep3_keyword_pos - 2), deep3_keyword_pos + 1);
				street = addressStr.substring(deep3_keyword_pos + 1);
			} else {
				//不能识别的解析
				deep3_area_name = "";
				street = addressStr;
			}
		}

		/**
		 * 2. 二级地址的识别 共有410个二级地址 高频词为【市，盟，州】 高频长度为3,4个字符 因为有用户可能会填写 '四川省阆中市'，所以二级地址的识别可靠性并不高 需要与三级地址 综合使用
		 * 返回 [%第2级% 模糊地址]
		 */
		String deep2_area_name = null;
		if (addressStr.contains("市") || addressStr.contains("盟") || addressStr.contains("州")) {
			if (addressStr.contains("市")) {
				int pos = addressStr.lastIndexOf("市");
				deep2_area_name = addressStr.substring(Math.max(0, pos - 2), pos + 1);
			}
			if (addressStr.contains("盟")) {
				int pos = addressStr.lastIndexOf("盟");
				deep2_area_name = addressStr.substring(Math.max(0, pos - 2), pos + 1);
			}

			if (addressStr.contains("州")) {
				if (addressStr.contains("自治州")) {
					int pos = addressStr.lastIndexOf("自治州");
					deep2_area_name = addressStr.substring(Math.max(0, pos - 4), pos + 1);
				} else {
					int pos = addressStr.lastIndexOf("盟");
					deep2_area_name = addressStr.substring(Math.max(0, pos - 2), pos + 1);
				}
			}
		} else {
			deep2_area_name = "";
		}

		//3. 到数据中智能匹配
		List <Area> deep3_area_list;
		Area area_info_1 = null;
		Area area_info_2 = null;
		Area area_info_3 = null;

		Address ad = new Address();

		if (!deep3_area_name.isEmpty()) {
			deep3_area_list = Areas.fuzzyQuery(3, deep3_area_name);

			// 三级地址的匹配出现多个结果 依靠二级地址缩小范围
			if (deep3_area_list.size() > 1) {
				if (!deep2_area_name.isEmpty()) {
					for (Area r : deep3_area_list) {
						int curParentId = r.areaParentId;
						Area parent = Areas.getArea(curParentId);
						String parName = parent.areaName;
						if (parName.contains(deep2_area_name)) {
							area_info_3 = r;
							area_info_2 = parent;
						}
					}
					area_info_1 = Areas.getArea(area_info_2.areaParentId);

					ad.prov = area_info_1.areaName;
					ad.city = area_info_2.areaName;
					ad.district = area_info_3.areaName;
				}
			} else {
				if (deep3_area_list.size() == 1) {
					area_info_3 = deep3_area_list.get(0);
					area_info_2 = Areas.getArea(area_info_3.areaParentId);

					if (null != area_info_2) {
						area_info_1 = Areas.getArea(area_info_2.areaParentId);

						ad.prov = area_info_1.areaName;
						ad.city = area_info_2.areaName;
						ad.district = area_info_3.areaName;
					}
				} else if (deep2_area_name.equals(deep3_area_name)) {   //如出现内蒙古自治区乌兰察布市公安局交警支队车管所这种只有省市，没有区的情况
					area_info_2 = Areas.fuzzyQuery(2, deep2_area_name).get(0);
					if (null != area_info_2) {
						area_info_1 = Areas.getArea(area_info_2.areaParentId);
						//获得结果
						if (null != area_info_1) {
							ad.prov = area_info_1.areaName;
							ad.city = area_info_2.areaName;
							ad.district = "";
						}
					}
				}
			}

		}
		ad.street = street;
		return ad;
	}

	public static class Address implements Serializable {
		// 省级行政区
		public String prov;
		// 地级行政区
		public String city;
		// 县级行政区
		public String district;
		// 街道
		public String street;

		public boolean parseError() {
			return prov == null && city == null && district == null;
		}
	}
}
