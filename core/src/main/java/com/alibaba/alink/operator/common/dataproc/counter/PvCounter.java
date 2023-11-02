package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

/**
 * @author lqb
 * @date 2018/10/22
 */
public class PvCounter extends AbstractCounter {
	private static final long serialVersionUID = -3881084994716468119L;
	long total = 0L;
	private WebTrafficIndexParams.Index counter = WebTrafficIndexParams.Index.PV;

	public PvCounter() {
		this(0L);
	}

	public PvCounter(long total) {
		this.total = total;
	}

	@Override
	public void visit(Object obj) {
		total++;
	}

	@Override
	public long count() {
		return total;
	}

	@Override
	public AbstractCounter merge(AbstractCounter counter) {
		if (counter == null) {
			return this;
		}

		PvCounter pvCounter = (PvCounter) counter;

		if (pvCounter.counter != this.counter) {
			throw new RuntimeException("Can't merge different counters");
		}

		PvCounter newCounter = new PvCounter(this.total + pvCounter.total);

		return newCounter;
	}
}
