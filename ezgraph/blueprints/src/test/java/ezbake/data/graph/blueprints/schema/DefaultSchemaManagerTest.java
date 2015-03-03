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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

/**
 * Tests methods {@link DefaultPropertySchemaManager}.
 */
public class DefaultSchemaManagerTest {

    /**
     * A URI used as an identifier for schemas added to the tested manager.
     */
    private static final String SCHEMA_IDENTIFIER_URI = "http://myschema.org";

    /**
     * Property name to be given to a definition in a schema.
     */
    private static final String MY_PROP_NAME = "myProp";

    /**
     * System under test.
     */
    private PropertySchemaManager schemaManager;

    @Before
    public void setUp() {
        schemaManager = new DefaultPropertySchemaManager();
    }

    @Test
    public void testGetSchemas() {
        assertEquals(new HashSet<String>(), schemaManager.getSchemas());
    }

    @Test
    public void testAddSchema() throws SchemaViolationException {
        addSchemaWithProperty();
        final Set<String> expected = Sets.newHashSet(SCHEMA_IDENTIFIER_URI);

        assertEquals(expected, schemaManager.getSchemas());
    }

    @Test(expected = SchemaViolationException.class)
    public void testAddExistingSchema() throws SchemaViolationException {
        schemaManager.addSchema(new RdfPropertySchema(SCHEMA_IDENTIFIER_URI, null));
        schemaManager.addSchema(new RdfPropertySchema(SCHEMA_IDENTIFIER_URI, null));
    }

    @Test
    public void testGetSchema() {
        assertNull(schemaManager.getSchema(SCHEMA_IDENTIFIER_URI));
        schemaManager.addSchema(new RdfPropertySchema(SCHEMA_IDENTIFIER_URI, null));
        assertNotNull(schemaManager.getSchema(SCHEMA_IDENTIFIER_URI));
    }

    @Test
    public void testValidateSchema() throws SchemaViolationException {
        try {
            schemaManager.validateSchemaExists(SCHEMA_IDENTIFIER_URI);
            fail();
        } catch (final SchemaViolationException e) {
            //expected - eat exception.
        }
        schemaManager.addSchema(new RdfPropertySchema(SCHEMA_IDENTIFIER_URI, null));
        schemaManager.validateSchemaExists(SCHEMA_IDENTIFIER_URI);
    }

    private void addSchemaWithProperty() throws SchemaViolationException {
        final Map<String, RDFDatatype> properties = new HashMap<>();
        properties.put(MY_PROP_NAME, XSDDatatype.XSDdouble);
        schemaManager.addSchema(new RdfPropertySchema(SCHEMA_IDENTIFIER_URI, properties));
    }
}
