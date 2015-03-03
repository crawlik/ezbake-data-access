/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.data.graph.blueprints.visibility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.data.test.TestUtils;
import ezbake.thrift.ThriftUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertyFilterTest {

    protected Visibility visU;
    protected Visibility visS;
    protected PermissionContext ctxU;
    protected PermissionContext ctxS;

    @Before
    public void setUp() {
        visU = new Visibility();
        visU.setFormalVisibility("U");

        visS = new Visibility();
        visS.setFormalVisibility("S");

        ctxU = new DefaultPermissionContext(TestUtils.createTestToken("U"));
        ctxS = new DefaultPermissionContext(TestUtils.createTestToken("U","S"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateRejectsMissingValue() throws TException {
        Map<String, Object> map = new HashMap<>();
        map.put(PropertyFilter.VISIBILITY_KEY, ThriftUtils.serializeToBase64(visU));

        PropertyFilter filter = new PropertyFilter(ctxU);
        filter.validate(Collections.singletonList(map));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateRejectsMissingVisibility() {
        Map<String, Object> map = new HashMap<>();
        map.put(PropertyFilter.VALUE_KEY, "foo");

        PropertyFilter filter = new PropertyFilter(ctxU);
        filter.validate(Collections.singletonList(map));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateRejectsUnknownKey() throws TException {
        Map<String, Object> map = new HashMap<>();
        map.put(PropertyFilter.VALUE_KEY, "foo");
        map.put(PropertyFilter.VISIBILITY_KEY, ThriftUtils.serializeToBase64(visU));
        map.put("invalid", "bar");

        PropertyFilter filter = new PropertyFilter(ctxU);
        filter.validate(Collections.singletonList(map));
    }

    @Test
    public void testFilterRemovesUnreadableElements() throws TException {
        List<Map<String, Object>> values = new ArrayList<>();

        values.add(new PropertyValueMap("green", visU));
        values.add(new PropertyValueMap("red", visS));

        PropertyFilter filter = new PropertyFilter(ctxU);
        List<Map<String, Object>> filtered = Lists.newArrayList(filter.filter(values, Permission.READ));

        assertEquals(1, filtered.size());
        assertEquals(new PropertyValueMap("green", visU), filtered.get(0));
    }

    @Test
    public void testRemoveKeepsUnwritableElements() {
        List<Map<String, Object>> values = new ArrayList<>();

        values.add(new PropertyValueMap("green", visU));
        values.add(new PropertyValueMap("red", visS));

        PropertyFilter filter = new PropertyFilter(ctxU);
        List<Map<String, Object>> filtered = Lists.newArrayList(filter.reject(values, Permission.WRITE));

        assertEquals(1, filtered.size());
        assertEquals(new PropertyValueMap("red", visS), filtered.get(0));
    }

    @Test
    public void testAddValue() {
        List<Map<String, Object>> oldValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("green", visU));
        List<Map<String, Object>> newValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("red", visS));

        PropertyFilter filter = new PropertyFilter(ctxS);
        List<Map<String, Object>> allValues = filter.modify(oldValues, newValues);

        assertEquals(2, allValues.size());
        Set<Map<String, Object>> setValues = new HashSet<>(allValues);
        assertTrue(setValues.contains(new PropertyValueMap("green", visU)));
        assertTrue(setValues.contains(new PropertyValueMap("red", visS)));
    }

    @Test
    public void testAddDuplicateValueAtSameVisibility() {
        List<Map<String, Object>> oldValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("green", visU));
        List<Map<String, Object>> newValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("green", visU));

        PropertyFilter filter = new PropertyFilter(ctxS);
        List<Map<String, Object>> allValues = filter.modify(oldValues, newValues);

        assertEquals(1, allValues.size());
        assertEquals(new PropertyValueMap("green", visU), allValues.get(0));
    }

    @Test
    public void testAddDuplicateValueAtDifferentVisibility() {
        List<Map<String, Object>> oldValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("green", visU));
        List<Map<String, Object>> newValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("green", visS));

        PropertyFilter filter = new PropertyFilter(ctxS);
        List<Map<String, Object>> allValues = filter.modify(oldValues, newValues);

        assertEquals(2, allValues.size());
        Set<Map<String, Object>> setValues = new HashSet<>(allValues);
        assertTrue(setValues.contains(new PropertyValueMap("green", visU)));
        assertTrue(setValues.contains(new PropertyValueMap("green", visS)));
    }

    @Test
    public void testDeleteRemovesWritableElement() {
        List<Map<String, Object>> oldValues = new ArrayList<>();

        oldValues.add(new PropertyValueMap("green", visU));
        oldValues.add(new PropertyValueMap("red", visS));

        List<Map<String, Object>> newValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("red", visS, true));

        PropertyFilter filter = new PropertyFilter(ctxS);
        List<Map<String, Object>> modified = filter.modify(oldValues, newValues);

        assertEquals(1, modified.size());
        assertEquals(new PropertyValueMap("green", visU), modified.get(0));
    }

    @Test
    public void testDeleteRequiresWritableElement() {
        List<Map<String, Object>> oldValues = new ArrayList<>();

        oldValues.add(new PropertyValueMap("green", visU));
        oldValues.add(new PropertyValueMap("red", visS));

        List<Map<String, Object>> newValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("red", visS, true));

        PropertyFilter filter = new PropertyFilter(ctxU);
        List<Map<String, Object>> modified = filter.modify(oldValues, newValues);

        assertEquals(2, modified.size());
        Set<Map<String, Object>> setValues = new HashSet<>(modified);
        assertTrue(setValues.contains(new PropertyValueMap("green", visU)));
        assertTrue(setValues.contains(new PropertyValueMap("red", visS)));
    }

    @Test
    public void testDeleteRequiresMatchingValueAndVisibility() {
        List<Map<String, Object>> oldValues = new ArrayList<>();

        oldValues.add(new PropertyValueMap("green", visU));
        oldValues.add(new PropertyValueMap("red", visS));

        List<Map<String, Object>> newValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("red", visU, true));

        PropertyFilter filter = new PropertyFilter(ctxS);
        List<Map<String, Object>> modified = filter.modify(oldValues, newValues);

        assertEquals(2, modified.size());
        Set<Map<String, Object>> setValues = new HashSet<>(modified);
        assertTrue(setValues.contains(new PropertyValueMap("green", visU)));
        assertTrue(setValues.contains(new PropertyValueMap("red", visS)));
    }

    @Test
    public void testDeleteRemoveCorrectVisibility() {
        List<Map<String, Object>> oldValues = new ArrayList<>();

        oldValues.add(new PropertyValueMap("green", visU));
        oldValues.add(new PropertyValueMap("green", visS));

        List<Map<String, Object>> newValues =
                Collections.<Map<String, Object>>singletonList(new PropertyValueMap("green", visS, true));

        PropertyFilter filter = new PropertyFilter(ctxS);
        List<Map<String, Object>> modified = filter.modify(oldValues, newValues);

        assertEquals(1, modified.size());
        assertEquals(new PropertyValueMap("green", visU), modified.get(0));
    }
}
