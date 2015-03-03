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

package ezbake.data.graph.blueprints.stub;

import java.util.ArrayList;
import java.util.List;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * Stubbed {@link com.tinkerpop.blueprints.VertexQuery} used for testing {@link ezbake.data.graph.blueprints.schema.SchemaFilterVertexQuery}.
 */
public class VertexQueryStub implements VertexQuery {

    //member variables for validating input.
    public boolean directionCalled;
    public Direction directionDirection;

    public boolean labelsCalled;
    public String[] labelsLabels;

    public boolean countCalled;

    public boolean vertexIdsCalled;

    public boolean hasKCalled;
    public String hasKKey;

    public boolean hasKVCalled;
    public String hasKVKey;
    public Object hasKVValue;

    public boolean hasKPVCalled;
    public String hasKPVKey;
    public Predicate hasKPVPredicate;
    public Object hasKPVValue;

    public boolean hasNotKCalled;
    public String hasNotKKey;

    public boolean hasNotKVCalled;
    public String hasNotKVKey;
    public Object hasNotKVValue;

    public boolean intervalCalled;
    public String intervalKey;
    public Comparable intervalStartValue;
    public Comparable intervalEndValue;

    public boolean limitCalled;
    public int limitLimit;

    public boolean edgesCalled;

    public boolean verticesCalled;

    @Override
    public VertexQuery direction(Direction direction) {
        directionCalled = true;
        directionDirection = direction;

        return this;
    }

    @Override
    public VertexQuery labels(String... labels) {
        labelsCalled = true;
        labelsLabels = labels;

        return this;
    }

    @Override
    public long count() {
        countCalled = true;

        return 43;
    }

    @Override
    public Object vertexIds() {
        vertexIdsCalled = true;

        return 5;
    }

    @Override
    public VertexQuery has(String key) {
        hasKCalled = true;
        hasKKey = key;

        return this;
    }

    @Override
    public VertexQuery hasNot(String key) {
        hasNotKCalled = true;
        hasNotKKey = key;

        return this;
    }

    @Override
    public VertexQuery has(String key, Object value) {
        hasKVCalled = true;
        hasKVKey = key;
        hasKVValue = value;

        return null;
    }

    @Override
    public VertexQuery hasNot(String key, Object value) {
        hasNotKVCalled = true;
        hasNotKVKey = key;
        hasNotKVValue = value;

        return this;
    }

    @Override
    public VertexQuery has(
            String key, Predicate predicate, Object value) {
        hasKPVCalled = true;
        hasKPVKey = key;
        hasKPVPredicate = predicate;
        hasKPVValue = value;

        return this;
    }

    @Override
    public <T extends Comparable<T>> VertexQuery has(
            String key, T value, Compare compare) {
        // deprecated
        return this;
    }

    @Override
    public <T extends Comparable<?>> VertexQuery interval(String key, T startValue, T endValue) {
        intervalCalled = true;
        intervalKey = key;
        intervalStartValue = startValue;
        intervalEndValue = endValue;

        return this;
    }

    @Override
    public VertexQuery limit(int limit) {
        limitCalled = true;
        limitLimit = limit;
        return this;
    }

    @Override
    public Iterable<Edge> edges() {
        edgesCalled = true;
        final List<Edge> edges = new ArrayList<>();
        edges.add(new EdgeStub());

        return edges;
    }

    @Override
    public Iterable<Vertex> vertices() {
        verticesCalled = true;
        final List<Vertex> vertices = new ArrayList<>();
        vertices.add(new VertexStub());

        return vertices;
    }
}
