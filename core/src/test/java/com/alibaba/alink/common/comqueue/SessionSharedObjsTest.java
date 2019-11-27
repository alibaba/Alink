package com.alibaba.alink.common.comqueue;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SessionSharedObjsTest {

    @Test
    public void testPutGet() {
        int session = SessionSharedObjs.getNewSessionId();

        String objName = "obj_name";
        for (int i = 0; i < 4; i++) {
            SessionSharedObjs.put(objName, session, i, "obj:" + i);
        }

        for (int i = 0; i < 4; i++) {
            assertTrue(SessionSharedObjs.contains(objName, session, i));
            Object obj = SessionSharedObjs.get(objName, session, i);
            assertEquals(obj, "obj:" + i);
        }

        assertFalse(SessionSharedObjs.contains(objName, session, 5));
        assertNull(SessionSharedObjs.get(objName, session, 5));

        assertEquals("obj:0", SessionSharedObjs.remove(objName, session, 0));
        assertFalse(SessionSharedObjs.contains(objName, session, 0));

        for (int i = 1; i < 4; i++) {
            assertTrue(SessionSharedObjs.contains(objName, session, i));
        }

        SessionSharedObjs.clear(session);
        for (int i = -10; i < 10; i++) {
            assertFalse(SessionSharedObjs.contains(objName, session, i));
        }

    }

    @Test
    public void testPutGetPartitionData() {
        int session = SessionSharedObjs.getNewSessionId();

        String objName = "obj_name";
        for (int task = 0; task < 4; task++) {
            List<String> data = Collections.singletonList("obj:" + task);
            SessionSharedObjs.cachePartitionedData(objName, session, data);
        }

        for (int task = 0; task < 4; task++) {
            SessionSharedObjs.distributeCachedData(Arrays.asList(objName), session, task);
        }

        for (int i = 0; i < 4; i++) {
            // TODO(xiafei.qiuxf): contains is problematic.
            // assertTrue(SessionSharedObjs.contains(objName, session, i));
            Object obj = SessionSharedObjs.get(objName, session, i);
            // last attempt visible.
            assertEquals(Collections.singletonList("obj:" + i), obj);
        }

        assertFalse(SessionSharedObjs.contains(objName, session, 5));
        assertNull(SessionSharedObjs.get(objName, session, 5));

        assertEquals(Collections.singletonList("obj:0"), SessionSharedObjs.remove(objName, session, 0));
        assertFalse(SessionSharedObjs.contains(objName, session, 0));

        for (int i = 1; i < 4; i++) {
            assertTrue(SessionSharedObjs.contains(objName, session, i));
        }

        SessionSharedObjs.clear(session);
        for (int i = -10; i < 10; i++) {
            assertFalse(SessionSharedObjs.contains(objName, session, i));
        }
    }
}
