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

package ezbake.data.graph.blueprints.util;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;

import ezbake.data.graph.blueprints.visibility.PropertyFilter;

/**
 * Assertions to help with testing visibility graph implementation.
 */
public class Assert {

    /**
     * Assert that the given iterable of elements matches the set of given IDs.
     *
     * @param ids set of ids that should match the IDs of the given elements
     * @param elements iterable of elements that should match the set of IDs
     */
    public static <T extends Element> void assertElementIds(Set<?> ids, Iterable<T> elements) {
        HashSet<Object> idsHashSet = new HashSet<>(ids);
        HashSet<Object> s = new HashSet<>();

        for (T e : elements) {
            s.add(e.getId());
        }

        assertEquals(idsHashSet, s);
    }

    /**
     * Assert that the given iterable of edges matches the set of given labels.
     *
     * @param labels set of labels that should match the labels of the given edges
     * @param edges iterable of edges that should match the set of labels
     */
    public static <T extends Edge> void assertEdgeLabels(Set<String> labels, Iterable<T> edges) {
        HashSet<String> labelsHashSet = new HashSet<>(labels);
        HashSet<String> s = new HashSet<>();

        for (T e : edges) {
            s.add(e.getLabel());
        }

        assertEquals(labelsHashSet, s);
    }

    /**
     * Assert that the values in a list of values maps matches a set of expected values.
     *
     * @param expected set of expected values
     * @param values list of values maps
     */
    public static void assertValues(Iterable expected, Iterable<Map<String, Object>> values) {

        HashSet<Object> x = new HashSet<>();
        for (Object o : expected) {
            x.add(o);
        }

        HashSet<Object> y = new HashSet<>();
        for (Map<String, Object> m : values) {
            y.add(m.get(PropertyFilter.VALUE_KEY));
        }

        assertEquals(x, y);
    }
}
