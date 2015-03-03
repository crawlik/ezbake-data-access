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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * Wrapper for Edge that works with schema filter classes.
 */
public class SchemaFilterEdge extends SchemaFilterElement implements Edge {

    /**
     * Edge wrapped by this.
     */
    private final Edge baseEdge;

    /**
     * Constructs a new SchemaFilterEdge with the given Edge and SchemaContext.
     *
     * @param baseEdge edge to wrap
     * @param context context in which this SchemaFilterEdge is used
     */
    public SchemaFilterEdge(
            Edge baseEdge, SchemaContext context) {
        super(baseEdge, context);
        this.baseEdge = baseEdge;
    }

    /**
     * Gets the edge this class wraps.
     *
     * @return the unwrapped edge
     */
    public Edge getBaseEdge() {
        return baseEdge;
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        return getContext().asSchemaFilterVertex(baseEdge.getVertex(direction));
    }

    @Override
    public String getLabel() {
        return baseEdge.getLabel();
    }
}
