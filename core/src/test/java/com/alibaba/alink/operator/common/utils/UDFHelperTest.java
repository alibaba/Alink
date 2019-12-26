package com.alibaba.alink.operator.common.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UDFHelperTest {

    @Test
    public void generateUDFClause() {
        String clause = UDFHelper.generateUDFClause(
            "input", "func", "c1", new String[]{"c0", "c1"}, new String[]{"c0", "c1", "c2"}
        );
        assertEquals(
            "SELECT `c0`,`c2`, func(`c0`,`c1`) as `c1` FROM input",
            clause);
    }

    @Test
    public void generateUDTFClauseCrossJoin() {
        String clause = UDFHelper.generateUDTFClause(
            "input", "func", new String[]{"c1", "c2"}, new String[]{"c0", "c1"}, new String[]{"c0", "c1", "c2"}, "cross"
        );
        String pattern = "SELECT `c0`, `c1_[0-9a-z]{32}` as `c1`, `c2_[0-9a-z]{32}` as `c2` FROM \\(" +
            "SELECT `c0`,`c1_[0-9a-z]{32}`,`c2_[0-9a-z]{32}` FROM input, " +
            "LATERAL TABLE\\(func\\(`c0`,`c1`\\)\\) as T\\(`c1_[0-9a-z]{32}`,`c2_[0-9a-z]{32}`\\)" +
            "\\)";
        assertTrue(clause.matches(pattern));
    }

    @Test
    public void generateUDTFClauseLeftOuterJoin() {
        String clause = UDFHelper.generateUDTFClause(
            "input", "func", new String[]{"c1", "c2"}, new String[]{"c0", "c1"}, new String[]{"c0", "c1", "c2"}, "leftOuter"
        );
        String pattern = "SELECT `c0`, `c1_[0-9a-z]{32}` as `c1`, `c2_[0-9a-z]{32}` as `c2` FROM \\(" +
            "SELECT `c0`,`c1_[0-9a-z]{32}`,`c2_[0-9a-z]{32}` FROM input " +
            "LEFT JOIN LATERAL TABLE\\(func\\(`c0`,`c1`\\)\\) as T\\(`c1_[0-9a-z]{32}`,`c2_[0-9a-z]{32}`\\)" +
            " ON TRUE\\)";
        assertTrue(clause.matches(pattern));
    }
}
