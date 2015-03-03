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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;

/**
 * Stub for GraphQuery.  Helps validate input.
 */
public class GraphQueryStub implements GraphQuery {

    // variables used for input validation
    public boolean hasKCalled;
    public String hasKKey;

    public boolean hasNotKCalled;
    public String hasNotKKey;

    public boolean hasKVCalled;
    public String hasKVKey;
    public Object hasKVValue;

    public boolean hasNotKVCalled;
    public String hasNotKVKey;
    public Object hasNotKVValue;

    public boolean hasKPVCalled;
    public String hasKPVKey;
    public Predicate hasKPVPredicate;
    public Object hasKPVValue;

    public boolean intervalCalled;
    public String intervalKey;
    public Comparable intervalStartValue;
    public Comparable intervalEndValue;

    public boolean limitCalled;
    public int limitLimit;

    public boolean edgesCalled;
    public boolean verticesCalled;

    @Override
    public GraphQuery has(String key) {
        hasKCalled = true;
        hasKKey = key;

        return this;
    }

    @Override
    public GraphQuery hasNot(String key) {
        hasNotKCalled = true;
        hasNotKKey = key;

        return this;
    }

    @Override
    public GraphQuery has(String key, Object value) {
        hasKVCalled = true;
        hasKVKey = key;
        hasKVValue = value;

        return this;
    }

    @Override
    public GraphQuery hasNot(String key, Object value) {
        hasNotKVCalled = true;
        hasNotKVKey = key;
        hasNotKVValue = value;

        return this;
    }

    @Override
    public GraphQuery has(
            String key, Predicate predicate, Object value) {
        hasKPVCalled = true;
        hasKPVKey = key;
        hasKPVPredicate = predicate;
        hasKPVValue = value;

        return this;
    }

    @Override
    public <T extends Comparable<T>> GraphQuery has(String key, T value, Compare compare) {

        return this;
    }

    @Override
    public <T extends Comparable<?>> GraphQuery interval(String key, T startValue, T endValue) {
        intervalCalled = true;
        intervalKey = key;
        intervalStartValue = startValue;
        intervalEndValue = endValue;

        return this;
    }

    @Override
    public GraphQuery limit(int limit) {
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
