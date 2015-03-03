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

import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import ezbake.base.thrift.Visibility;

/**
 * Wrapper around visibility deserializer that caches results.
 */
public class CachingVisibilityDeserializer implements VisibilityDeserializer {

    /**
     * Wrapped deserializer
     */
    private final VisibilityDeserializer baseDeserializer;

    /**
     * Cache for deserialized visibilities
     */
    private LoadingCache<Object, Visibility> visibilityCache;

    /**
     * Default maximum cache size
     */
    private static final int DEFAULT_MAXIMUM_CACHE_SIZE = 1000;

    /**
     * Construct a new deserializer that wraps another deserializer, but caches
     * its results.
     *
     * @param deserializer deserializer to wrap
     */
    public CachingVisibilityDeserializer(VisibilityDeserializer deserializer) {
        this.baseDeserializer = deserializer;
        this.visibilityCache = CacheBuilder.newBuilder().maximumSize(DEFAULT_MAXIMUM_CACHE_SIZE).build(
                new CacheLoader<Object, Visibility>() {
                    @Override
                    public Visibility load(Object o) throws Exception {
                        return baseDeserializer.deserialize(o);
                    }
                });
    }

    @Override
    public Visibility deserialize(Object object) {
        try {
            return visibilityCache.get(object);
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw VisibilityFilterExceptionFactory.visibilityMalformed();
        }
    }
}
