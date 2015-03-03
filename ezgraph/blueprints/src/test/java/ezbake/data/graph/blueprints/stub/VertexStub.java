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
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * Stub for {@link com.tinkerpop.blueprints.Vertex} used for testing {@link ezbake.data.graph.blueprints.schema
 * .SchemaFilterVertex} and anywhere that requires an unintelligent Vertex. If a {@code Vertex} implementation under
 * test relies on its superclass {@link com.tinkerpop.blueprints.Element} methods rather than implementing themselves,
 * this class should generally not be used to test those methods on the implementation of {@code Element}.
 */
public class VertexStub implements Vertex {
    public static final String VERTEX_STUB_ID = "vertexStubId";

    // member variables for input validation
    public boolean getVerticesCalled;
    public Direction getVerticesDirection;
    public String[] getVerticesLabels;

    public boolean getEdgesCalled;
    public Direction getEdgesDirection;
    public String[] getEdgesLabels;

    public boolean addEdgeCalled;
    public String addEdgeLabel;
    public Vertex addEdgeVertex;

    public boolean getPropertyCalled;
    public String getPropertyKey;

    public boolean getPropertyKeysCalled;

    public boolean setPropertyCalled;
    public String setPropertyKey;
    public Object setPropertyValue;

    public boolean removePropertyCalled;
    public String removePropertykey;

    public boolean removeCalled;

    public boolean getIdCalled;

    public Set<String> propertyKeysToReturn;

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        getEdgesCalled = true;
        getEdgesDirection = direction;
        getEdgesLabels = labels;
        final List<Edge> edges = new ArrayList<>();
        edges.add(new EdgeStub());
        return edges;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        getVerticesCalled = true;
        getVerticesDirection = direction;
        getVerticesLabels = labels;
        final List<Vertex> verts = new ArrayList<>();
        verts.add(new VertexStub());
        return verts;
    }

    @Override
    public VertexQuery query() {
        throw new UnsupportedOperationException("Not used");
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        addEdgeCalled = true;
        addEdgeLabel = label;
        addEdgeVertex = inVertex;
        return new EdgeStub();
    }

    @Override
    public <T> T getProperty(String key) {
        getPropertyCalled = true;
        getPropertyKey = key;
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        getPropertyKeysCalled = true;
        return propertyKeysToReturn;
    }

    @Override
    public void setProperty(String key, Object value) {
        setPropertyCalled = true;
        setPropertyKey = key;
        setPropertyValue = value;
    }

    @Override
    public <T> T removeProperty(String key) {
        removePropertyCalled = true;
        removePropertykey = key;
        return null;
    }

    @Override
    public void remove() {
        removeCalled = true;
    }

    @Override
    public Object getId() {
        getIdCalled = true;
        return VERTEX_STUB_ID;
    }
}
