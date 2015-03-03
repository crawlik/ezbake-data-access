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

import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import ezbake.data.graph.blueprints.stub.VertexStub;

/**
 * Stub for validation of input. Contains canned response for {@code getVertex(id)} where it returns a {@link
 * ezbake.data.graph.blueprints.stub.VertexStub}.
 */
public class EdgeStub implements Edge {

    /**
     * Error message for trying to use the stub in a way it was not intended.
     */
    public static final String STUB_ELEMENT_MESSAGE = "Tested in SchemaFilterElementTest.";

    // variables used for validation of input.
    public boolean getVertexCalled;
    public Direction getVertexDirection;
    public boolean getLabelCalled;

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        getVertexCalled = true;
        getVertexDirection = direction;
        return new VertexStub();
    }

    @Override
    public String getLabel() {
        getLabelCalled = true;
        return null;
    }

    @Override
    public <T> T getProperty(String key) {
        throw new UnsupportedOperationException(STUB_ELEMENT_MESSAGE);
    }

    @Override
    public Set<String> getPropertyKeys() {
        throw new UnsupportedOperationException(STUB_ELEMENT_MESSAGE);
    }

    @Override
    public void setProperty(String key, Object value) {
        throw new UnsupportedOperationException(STUB_ELEMENT_MESSAGE);
    }

    @Override
    public <T> T removeProperty(String key) {
        throw new UnsupportedOperationException(STUB_ELEMENT_MESSAGE);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(STUB_ELEMENT_MESSAGE);
    }

    @Override
    public Object getId() {
        throw new UnsupportedOperationException(STUB_ELEMENT_MESSAGE);
    }
}