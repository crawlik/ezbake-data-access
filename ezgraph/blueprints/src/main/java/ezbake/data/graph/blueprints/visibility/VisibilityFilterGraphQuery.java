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

import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;

/**
 * Wrapper for graph queries that implements visibility controls.
 */
public class VisibilityFilterGraphQuery extends VisibilityFilterQuery implements GraphQuery {

    /**
     * Create a new graph query wrapper.
     *
     * @param query graph query to wrap
     * @param ctx permissions context
     */
    public VisibilityFilterGraphQuery(GraphQuery query, PermissionContext ctx) {
        super(query, ctx);
    }

    @Override
    public GraphQuery has(String key) {
        super.has(key);

        return this;
    }

    @Override
    public GraphQuery hasNot(String key) {
        super.hasNot(key);

        return this;
    }

    @Override
    public GraphQuery has(String key, Object value) {
        super.has(key, value);

        return this;
    }

    @Override
    public GraphQuery hasNot(String key, Object value) {
        super.hasNot(key, value);

        return this;
    }

    @Override
    public GraphQuery has(String key, Predicate predicate, Object value) {
        super.has(key, predicate, value);

        return this;
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T extends Comparable<T>> GraphQuery has(String s, T t, Compare compare) {
        super.has(s, t, compare);

        return this;
    }

    @Override
    public <T extends Comparable<?>> GraphQuery interval(String key, T startValue, T endValue) {
        super.interval(key, startValue, endValue);

        return this;
    }

    @Override
    public GraphQuery limit(int i) {
        super.limit(i);

        return this;
    }
}
