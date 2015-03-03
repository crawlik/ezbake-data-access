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

import org.apache.thrift.TException;

import ezbake.base.thrift.Visibility;
import ezbake.thrift.ThriftUtils;

/**
 * Visibility deserializer that deserializes base64-encoded Thrift objects
 * stored in Strings.
 */
public class DefaultVisibilityDeserializer implements VisibilityDeserializer {
    @Override
    public Visibility deserialize(Object object) {
        if (object == null) {
            throw VisibilityFilterExceptionFactory.visibilityCanNotBeNull();
        }

        if (!(object instanceof String)) {
            throw VisibilityFilterExceptionFactory.visibilityMalformed();
        }

        try {
            return ThriftUtils.deserializeFromBase64(Visibility.class, (String) object);
        } catch (TException e) {
            throw VisibilityFilterExceptionFactory.visibilityMalformed();
        }
    }
}
