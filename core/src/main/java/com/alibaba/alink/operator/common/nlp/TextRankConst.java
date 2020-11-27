package com.alibaba.alink.operator.common.nlp;

public class TextRankConst {
	public final static Integer TOPN = 5;
	public final static Double DAMPINGFACTOR = 0.85;
	public final static Integer MAXITER = 100;
	public final static Double EPSILON = 0.000001;
	public final static String DELIMITER = "。？！";
	public final static GraphType GRAPHTYPE = GraphType.UNDITECTED;
	public final static String STOPWORDDICT = "/stop.txt";

	public enum GraphType {
		/**
		 * <code>UNDITECTED</code>
		 */
		UNDITECTED,
		/**
		 * <code>DIRECTED_FORWARD</code>;</code>
		 */
		DIRECTED_FORWARD,
		/**
		 * <code>DIRECTED_BACKWARD</code>
		 */
		DIRECTED_BACKWARD
	}
}
