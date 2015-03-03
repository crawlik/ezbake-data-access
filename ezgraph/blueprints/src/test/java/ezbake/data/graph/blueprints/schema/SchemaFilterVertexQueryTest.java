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

package ezbake.data.graph.blueprints.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;

import ezbake.data.graph.blueprints.stub.PropertySchemaManagerStub;
import ezbake.data.graph.blueprints.stub.VertexQueryStub;

/**
 * Tests {@link SchemaFilterVertexQuery} which is a wrapper for {@link com.tinkerpop.blueprints.VertexQuery} designed to
 * work with the schema filter classes.
 */
public class SchemaFilterVertexQueryTest {

    //member variables retained for posterity.
    private static final String KEY = "key";
    private static final String VALUE = "value";

    /**
     * Wrapped {@link com.tinkerpop.blueprints.VertexQuery}.
     */
    private VertexQueryStub queryStub;

    /**
     * Implementation of {@link com.tinkerpop.blueprints.VertexQuery} under test.
     */
    private SchemaFilterVertexQuery query;

    /**
     * Initialize {@code queryStub} and {@code query}.
     */
    @Before
    public void setUp() {
        queryStub = new VertexQueryStub();
        query = new SchemaFilterVertexQuery(
                queryStub, new DefaultSchemaContext(new PropertySchemaManagerStub()));
    }

    /**
     * {@code vertices()} delegates to wrapped query and returns resulting iterable of {@link
     * com.tinkerpop.blueprints.Vertex} as an iterable of {@link SchemaFilterVertex}.
     */
    @Test
    public void testVertices() {
        assertSame(query.vertices().iterator().next().getClass(), SchemaFilterVertex.class);
        assertTrue(queryStub.verticesCalled);
    }

    /**
     * {@code edges()} delegates to wrapped query and returns resulting iterable of {@link
     * com.tinkerpop.blueprints.Edge} as an iterable of {@link SchemaFilterEdge}.
     */
    @Test
    public void testEdges() {
        assertSame(query.edges().iterator().next().getClass(), SchemaFilterEdge.class);
        assertTrue(queryStub.edgesCalled);
    }

    //// Tests retained for posterity ////

    @Test
    public void testDirection() {
        final Direction direction = Direction.OUT;
        assertSame(query, query.direction(direction));
        assertTrue(queryStub.directionCalled);
        assertEquals(direction, queryStub.directionDirection);
    }

    @Test
    public void testLabels() {
        final String[] labels = {"a", "b", "c"};
        assertSame(query, query.labels(labels));
        assertTrue(queryStub.labelsCalled);
        assertEquals(Lists.newArrayList(labels), Lists.newArrayList(queryStub.labelsLabels));
    }

    @Test
    public void testCount() {
        assertEquals(43, query.count());
        assertTrue(queryStub.countCalled);
    }

    @Test
    public void testVertexIds() {
        assertEquals(5, query.vertexIds());
        assertTrue(queryStub.vertexIdsCalled);
    }

    @Test
    public void testHasK() {
        assertSame(query, query.has(KEY));
        assertTrue(queryStub.hasKCalled);
        assertEquals(KEY, queryStub.hasKKey);
    }

    @Test
    public void testHasNotK() {
        assertSame(query, query.hasNot(KEY));
        assertTrue(queryStub.hasNotKCalled);
        assertEquals(KEY, queryStub.hasNotKKey);
    }

    @Test
    public void testHasKV() {
        assertSame(query, query.has(KEY, VALUE));
        assertTrue(queryStub.hasKVCalled);
        assertEquals(KEY, queryStub.hasKVKey);
        assertEquals(VALUE, queryStub.hasKVValue);
    }

    @Test
    public void testHasNotKV() {
        assertSame(query, query.hasNot(KEY, VALUE));
        assertTrue(queryStub.hasNotKVCalled);
        assertEquals(KEY, queryStub.hasNotKVKey);
        assertEquals(VALUE, queryStub.hasNotKVValue);
    }

    @Test
    public void testHasKPV() {
        final Predicate predicate = Compare.EQUAL;
        assertSame(query, query.has(KEY, predicate, VALUE));
        assertTrue(queryStub.hasKPVCalled);
        assertEquals(KEY, queryStub.hasKPVKey);
        assertEquals(predicate, queryStub.hasKPVPredicate);
        assertEquals(VALUE, queryStub.hasKPVValue);
    }

    @Test
    public void testInterval() {
        final String startValue = "a";
        final String endValue = "b";
        assertSame(query, query.interval(KEY, startValue, endValue));
        assertTrue(queryStub.intervalCalled);
        assertEquals(KEY, queryStub.intervalKey);
        assertEquals(startValue, queryStub.intervalStartValue);
        assertEquals(endValue, queryStub.intervalEndValue);
    }

    @Test
    public void testLimit() {
        final int limit = 100;
        assertSame(query, query.limit(limit));
        assertTrue(queryStub.limitCalled);
        assertEquals(limit, queryStub.limitLimit);
    }
}
