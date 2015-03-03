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

package ezbake.data.graph.rexster;

import java.util.Properties;

import org.apache.commons.configuration.XMLConfiguration;

import com.tinkerpop.rexster.server.RexsterProperties;

/**
 * Utility methods to work with Rexster and EzBake properties.
 */
public class RexsterPropertiesUtils {

    /**
     * Combine Rexster and EzBake properties into a single RexsterProperties.
     *
     * @param rexsterProperties Rexster properties
     * @param ezBakeProperties EzBake properties
     * @return combined properties
     */
    public static RexsterProperties combineRexsterEzBakeProperties(
            RexsterProperties rexsterProperties, Properties ezBakeProperties) {
        XMLConfiguration configuration = new XMLConfigurationAdapter(
                rexsterProperties.getConfiguration(), ezBakeProperties);
        return new RexsterProperties(configuration);
    }
}
