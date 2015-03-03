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

/**
 * Further standard exceptions. Like {@link com.tinkerpop.blueprints.util.ExceptionFactory},
 * provides consistent look-and-feel and terminology for exception.
 */
public final class VisibilityFilterExceptionFactory {

    private VisibilityFilterExceptionFactory() {

    }

    /**
     * Throw if a visibility exists, but is not a string containing a base64-encoded thrift visibility object.
     *
     * @return new illegal argument exception
     */
    public static IllegalArgumentException visibilityMalformed() {
        return new IllegalArgumentException("Visibility must be a base64-encoded serialized thrift object");
    }

    /**
     * Throw if a visibility object is null.
     *
     * @return new illegal argument exception
     */
    public static IllegalArgumentException visibilityCanNotBeNull() {
        return new IllegalArgumentException("Visibility cannot be null");
    }

    /**
     * Throw if an operation attempts to remove a visibility.
     *
     * @return new illegal argument exception
     */
    public static IllegalArgumentException visibilityCanNotBeRemoved() {
        return new IllegalArgumentException("Visibility cannot be removed from element");
    }

    /**
     * Throw if an operation is denied because the context lacks permission to perform the operation.
     *
     * @return new illegal argument exception
     */
    public static IllegalArgumentException permissionDenied() {
        return new IllegalArgumentException("Permission denied");
    }
}
