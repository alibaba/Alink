package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author shaomeng.wang
 */
public class WebTrafficIndexStreamOpTest extends AlinkTestBase {
	private static Row[] testArray =
		new Row[] {
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
			Row.of("a", 1.3, 1.1),
			Row.of("b", -2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", -99.9, 100.9),
			Row.of("a", 1.4, 1.1),
			Row.of("b", -2.2, 0.9),
			Row.of("c", 100.9, -0.01),
			Row.of("d", -99.5, 100.9),
		};
	private static String[] colnames = new String[] {"col1", "col2", "col3"};

	@Test
	public void linkFrom() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setIndex(WebTrafficIndexParams.Index.PV)
			.setTimeInterval(0.001);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void linkUV() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setTimeInterval(0.001)
			.setIndex(WebTrafficIndexParams.Index.UV);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void linkUIP() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setIndex(WebTrafficIndexParams.Index.UIP)
			.setFormat("sparse")
			.setTimeInterval(0.001);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void linkHyperPP() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setIndex(WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_HYPERLOGLOGPLUS.toString())
			.setFormat("sparse")
			.setTimeInterval(0.001);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void linkHyperLL() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setIndex(WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_HYPERLOGLOG)
			.setFormat("sparse")
			.setTimeInterval(0.001);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void linkAdap() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setIndex(WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_ADAPTIVE)
			.setFormat("sparse")
			.setTimeInterval(0.001);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void linkLinear() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setIndex(WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_LINEAR)
			.setFormat("sparse")
			.setTimeInterval(0.001);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void linkLL() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setIndex(WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_LOGLOG)
			.setFormat("sparse")
			.setTimeInterval(0.001);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}

	@Test
	public void linkSto() throws Exception {
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colnames);

		WebTrafficIndexStreamOp webTrafficIndexStreamOp = new WebTrafficIndexStreamOp()
			.setSelectedCol(colnames[0])
			.setIndex(WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_STOCHASTIC)
			.setFormat("sparse")
			.setTimeInterval(0.001);

		inOp.link(webTrafficIndexStreamOp).print();

		StreamOperator.execute();
	}
}