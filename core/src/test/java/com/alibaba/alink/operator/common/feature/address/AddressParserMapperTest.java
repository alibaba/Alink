package com.alibaba.alink.operator.common.feature.address;

import com.alibaba.alink.common.utils.JsonConverter;
import junit.framework.TestCase;
import org.junit.Test;

public class AddressParserMapperTest extends TestCase {

	@Test
	public void test() {
		String[] address = new String[] {
			//"成都市高新区天府软件园B区科技大楼",
			//"高新区天府软件园B区科技大楼",
			//"双流县郑通路社保局区52050号",
			//"岳市岳阳楼区南湖求索路碧灏花园A座1101",
			//"四川省南充市阆中市公园路25号",
			//"四川省阆中市公园路25号",
			//"四川省 凉山州美姑县xxx小区18号院",
			//"重庆攀枝花市东区机场路3中学校",
			//"渝北区渝北中学51200街道地址",
			//"天津天津市红桥区水木天成1区临湾路9-3-1101",
			"四川",
			"",
			//"内蒙古自治区乌兰察布市公安局交警支队车管所"
		};
		for (String add : address) {
			System.out.println(add);
			System.out.println("----  " + JsonConverter.toJson(AddressUtil.parseAddress(add)));
		}
	}

}