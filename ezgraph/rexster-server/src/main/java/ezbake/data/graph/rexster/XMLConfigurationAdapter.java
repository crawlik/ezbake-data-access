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

/**
 * Adapter to get EzBake configuration into XMLConfiguration.
 *
 * We use this to be able to pass EzBake configuration properties through
 * interfaces that require an XMLConfiguration. We graft EzBake properties onto
 * getProperties() method under the key "ezbake.properties".
 */
class XMLConfigurationAdapter extends XMLConfiguration {

    /**
     * Property key name
     */
    public static final String EZBAKE_PROPERTY_KEY = "ezbake.properties";

    /**
     * EzBake properties
     */
    protected final Properties properties;

    /**
     * Construct a new adapter around an existing XMLConfiguration and EzBake properties.
     *
     * @param configuration existing (Rexster) configuration
     * @param properties EzBake properties
     */
    public XMLConfigurationAdapter(XMLConfiguration configuration, Properties properties) {
        super(configuration);
        this.properties = properties;
    }

    @Override
    public Properties getProperties(String key) {
        if (key.equals(EZBAKE_PROPERTY_KEY)) {
            return properties;
        } else {
            return super.getProperties(key);
        }
    }
}
