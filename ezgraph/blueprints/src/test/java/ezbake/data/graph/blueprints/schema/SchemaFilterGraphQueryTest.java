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

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Predicate;

import ezbake.data.graph.blueprints.stub.GraphQueryStub;
import ezbake.data.graph.blueprints.stub.PropertySchemaManagerStub;

/**
 * Tests for {@link SchemaFilterGraphQuery}.
 */
public class SchemaFilterGraphQueryTest {

    // member variables retained for posterity
    private static final String KEY = "key";
    private static final String VALUE = "value";

    /**
     * System under test.
     */
    private SchemaFilterGraphQuery query;

    /**
     * {@link com.tinkerpop.blueprints.Query} wrapped by {@link SchemaFilterGraphQuery}.
     */
    private GraphQueryStub queryStub;

    /**
     * Initialize the wrapped  {@link com.tinkerpop.blueprints.GraphQuery} and give it to the {@link
     * SchemaFilterGraphQuery} under test.
     */
    @Before
    public void setUp() {
        queryStub = new GraphQueryStub();
        query = new SchemaFilterGraphQuery(
                queryStub, new DefaultSchemaContext(new PropertySchemaManagerStub()));
    }

    //// Tests left in for posterity. ////

    /**
     * {@code edges()} delegates to the wrapped {@link com.tinkerpop.blueprints.GraphQuery} but also converts retrieved
     * edges to a list of {@link SchemaFilterEdge}.
     */
    @Test
    public void testEdges() {
        assertSame(query.edges().iterator().next().getClass(), SchemaFilterEdge.class);
        assertTrue(queryStub.edgesCalled);
    }

    /**
     * {@code vertices()} delegates to the wrapped {@link com.tinkerpop.blueprints.GraphQuery} but also converts
     * retrieved vertices to an iterable of {@link SchemaFilterVertex}.
     */
    @Test
    public void testVertices() {
        assertSame(query.vertices().iterator().next().getClass(), SchemaFilterVertex.class);
        assertTrue(queryStub.verticesCalled);
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
