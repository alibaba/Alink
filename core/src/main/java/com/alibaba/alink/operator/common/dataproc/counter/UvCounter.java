package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.operator.common.slidingwindow.windowtree.DeepCloneable;
import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

import java.util.HashSet;

/**
 * @author lqb
 * @date 2018/10/22
 */
public class UvCounter extends AbstractCounter implements DeepCloneable <UvCounter> {
	private static final long serialVersionUID = -6209305263412663845L;
	HashSet <Object> bitMap;
	private WebTrafficIndexParams.Index counter = WebTrafficIndexParams.Index.UV;

	//todo 临时接口DeepCloneable，待删除
	@Override
	public UvCounter deepClone() {
		return this.clone();
	}

	@Override
	public UvCounter clone() {
		UvCounter uvc = new UvCounter();
		uvc.bitMap = (HashSet <Object>) this.bitMap.clone();
		uvc.counter = this.counter;
		return uvc;
	}

	public UvCounter() {
		bitMap = new HashSet <>();
	}

	@Override
	public void visit(Object obj) {
		bitMap.add(obj);
	}

	@Override
	public long count() {
		return bitMap.size();
	}

	@Override
	public AbstractCounter merge(AbstractCounter counter) {
		if (counter == null) {
			return this;
		}

		UvCounter uvCounter = (UvCounter) counter;

		if (uvCounter.counter != this.counter) {
			throw new RuntimeException("Can't merge different counters");
		}

		UvCounter newCounter = new UvCounter();

		for (Object obj : this.bitMap) {
			newCounter.bitMap.add(obj);
		}
		for (Object obj : uvCounter.bitMap) {
			newCounter.bitMap.add(obj);
		}

		return newCounter;
	}
}
