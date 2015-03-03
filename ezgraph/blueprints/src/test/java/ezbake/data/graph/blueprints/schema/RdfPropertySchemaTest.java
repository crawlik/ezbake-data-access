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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

/**
 * Tests {@link RdfPropertySchema}. Note that URIs are used for schema identifiers, which is what is intended for any
 * instance of RdfPropertySchema.
 */
public class RdfPropertySchemaTest {

    /**
     * A valid key defined in {@code properties} given a type of {@link XSDDatatype#XSDdouble}.
     */
    private static final String VALID_KEY = "myProp";

    /**
     * A valid value for the property defined in {@code properties} with key {@code VALID_PROP}.
     */
    private static final double VALID_PROP_VALUE = 34.93493D;

    /**
     * An invalid value for the properties defined in {@code properties}.
     */
    private static final String INVALID_VALUE = "not-a-double";

    /**
     * An invalid key for the properties defined in {@code properties}.
     */
    private static final String INVALID_KEY = "not-a-prop";

    /**
     * An identifier that can be assigned to a schema.
     */
    private static final String A_SCHEMA_IDENTIFIER_URI = "aUri";

    /**
     * Holds the system under test.
     */
    private PropertySchema schema;

    /**
     * Property definitions for a schema object.
     */
    private Map<String, RDFDatatype> properties;

    /**
     * Initialize {@code schema} and {@code properties}.
     */
    @Before
    public void setUp() {
        properties = new HashMap<>();
        properties.put(VALID_KEY, XSDDatatype.XSDdouble);
        schema = new RdfPropertySchema(A_SCHEMA_IDENTIFIER_URI, properties);
    }

    @Test
    public void testConstructorCreatesEmptyPropertiesIfNullPassedIn() {
        schema = new RdfPropertySchema(A_SCHEMA_IDENTIFIER_URI, null);
        assertNotNull(schema.getPropertyDefinitions());
    }

    /**
     * Tests that {@code getProperties()} returns given properties in a {@code Map<String,String>}
     */
    @Test
    public void testGetProperties() {
        final Map<String, String> expectedStringProperties = new HashMap<>();
        for (final Map.Entry<String, RDFDatatype> entry : properties.entrySet()) {
            expectedStringProperties.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        assertEquals(expectedStringProperties, schema.getPropertyDefinitions());
    }

    /**
     * Tests that {@code getIdentifier()} returns the given URL.
     */
    @Test
    public void testGetUri() {
        assertEquals(A_SCHEMA_IDENTIFIER_URI, schema.getIdentifier());
    }

    /**
     * Tests use of {@code isValidKeyValuePair(...)} against various properties, some valid some not.
     */
    @Test
    public void testIsValidKeyValue() {
        assertTrue(schema.isValidKeyValuePair(VALID_KEY, VALID_PROP_VALUE));
        assertFalse(schema.isValidKeyValuePair(VALID_KEY, INVALID_VALUE));
        assertFalse(schema.isValidKeyValuePair(INVALID_KEY, VALID_PROP_VALUE));
        assertFalse(schema.isValidKeyValuePair(VALID_KEY, null));
    }

    /**
     * Tests that {@code validateKeyValuePair(...)} throws an exception in the same cases that {@code
     * isValidKeyValuePair(...)} would return false.
     *
     * @throws SchemaViolationException if validation fails when it is not expected to
     */
    @Test
    public void testValidateKeyValue() throws SchemaViolationException {
        try {
            schema.validateKeyValuePair(VALID_KEY, null);
            fail();
        } catch (final SchemaViolationException e) {
            //Ignore exception - expected path.
        }

        try {
            schema.validateKeyValuePair(VALID_KEY, INVALID_VALUE);
            fail();
        } catch (final SchemaViolationException e) {
            //Ignore exception - expected path.
        }

        try {
            schema.validateKeyValuePair(INVALID_KEY, VALID_PROP_VALUE);
            fail();
        } catch (final SchemaViolationException e) {
            //Ignore exception - expected path.
        }

        schema.validateKeyValuePair(VALID_KEY, VALID_PROP_VALUE);
    }

    @Test
    public void testIsValidKey() {
        assertTrue(schema.isValidKey(VALID_KEY));
        assertFalse(schema.isValidKey(INVALID_KEY));
    }

    @Test
    public void testValidateKey() {
        schema.validateKey(VALID_KEY);
        try {
            schema.validateKey(INVALID_KEY);
            fail();
        } catch (SchemaViolationException e) {
            //Ignore exception - expected path.
        }
    }
}
