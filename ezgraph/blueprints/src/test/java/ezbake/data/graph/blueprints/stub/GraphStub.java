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
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

/**
 * Stub to help with validation of input. Some canned responses.
 */
public class GraphStub implements Graph {

    public static final String STUB_FOR_VERIFICATION = "stubForVerification";
    // variables used for validation of input
    public final Features features = new Features();

    public boolean getFeaturesCalled;

    public boolean addVertexCalled;
    public Object addVertexId;

    public boolean getVertexCalled;
    public Object getVertexId;

    public boolean removeVertexCalled;
    public Vertex removeVertexVertex;

    public boolean getVerticesCalled;

    public boolean getVerticesArgsCalled;
    public String getVerticesArgsKey;
    public Object getVerticesArgsValue;

    public boolean addEdgeCalled;
    public Object addEdgeId;
    public Vertex addEdgeOutVertex;
    public Vertex addEdgeInVertex;
    public String addEdgeLabel;

    public boolean getEdgeCalled;
    public Object getEdgeId;

    public boolean removeEdgeCalled;
    public Edge removeEdgeEdge;

    public boolean getEdgesCalled;

    public boolean getEdgesArgsCalled;
    public String getEdgesArgsKey;
    public Object getEdgesArgsValue;

    public boolean shutdownCalled;

    public VertexStub verifyableVertexStub = new VertexStub();

    @Override
    public Features getFeatures() {
        getFeaturesCalled = true;

        return features;
    }

    @Override
    public Vertex addVertex(Object id) {
        addVertexCalled = true;
        addVertexId = id;

        return new VertexStub();
    }

    @Override
    public Vertex getVertex(Object id) {
        getVertexCalled = true;
        getVertexId = id;

        if(id == STUB_FOR_VERIFICATION){
            return verifyableVertexStub;
        }

        return new VertexStub();
    }

    @Override
    public void removeVertex(Vertex vertex) {
        removeVertexCalled = true;
        removeVertexVertex = vertex;
    }

    @Override
    public Iterable<Vertex> getVertices() {
        getVerticesCalled = true;
        final List<Vertex> vertices = new ArrayList<>();
        vertices.add(new VertexStub());

        return vertices;
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        getVerticesArgsCalled = true;
        getVerticesArgsKey = key;
        getVerticesArgsValue = value;
        final List<Vertex> vertices = new ArrayList<>();
        vertices.add(new VertexStub());

        return vertices;
    }

    @Override
    public Edge addEdge(
            Object id, Vertex outVertex, Vertex inVertex, String label) {
        addEdgeCalled = true;
        addEdgeId = id;
        addEdgeOutVertex = outVertex;
        addEdgeInVertex = inVertex;
        addEdgeLabel = label;

        return new EdgeStub();
    }

    @Override
    public Edge getEdge(Object id) {
        getEdgeCalled = true;
        getEdgeId = id;

        return new EdgeStub();
    }

    @Override
    public void removeEdge(Edge edge) {
        removeEdgeCalled = true;
        removeEdgeEdge = edge;
    }

    @Override
    public Iterable<Edge> getEdges() {
        getEdgesCalled = true;
        final List<Edge> edges = new ArrayList<>();
        edges.add(new EdgeStub());

        return edges;
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        getEdgesArgsCalled = true;
        getEdgesArgsKey = key;
        getEdgesArgsValue = value;
        final List<Edge> edges = new ArrayList<>();
        edges.add(new EdgeStub());

        return edges;
    }

    @Override
    public GraphQuery query() {
        return null;
    }

    @Override
    public void shutdown() {
        shutdownCalled = true;
    }
}
