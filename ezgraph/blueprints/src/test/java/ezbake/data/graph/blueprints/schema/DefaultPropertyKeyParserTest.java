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
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import ezbake.data.graph.blueprints.stub.PropertySchemaManagerStub;

/**
 * Tests for {@link PropertyKeyParser}.
 */
public class DefaultPropertyKeyParserTest {

    /**
     * System under test.
     */
    private PropertyKeyParser parser;

    @Before
    public void setUp() {
        final DefaultSchemaContext context = new DefaultSchemaContext(new PropertySchemaManagerStub());
        parser = context.getPropertyKeyParser();
    }

    /**
     * Cannot parse a null key.
     */
    @Test
    public void testParseNullKey() {
        try {
            parser.getPropertyName(null);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow.  Eat exception.
        }

        try {
            parser.getSchemaIdentifier(null);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow.  Eat exception.
        }
    }

    /**
     * Cannot parse an empty key.
     */
    @Test
    public void testParseEmptyKey() throws SchemaViolationException {
        try {
            parser.getPropertyName("");
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow.  Eat exception.
        }

        try {
            parser.getSchemaIdentifier("");
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow.  Eat exception.
        }
    }

    /**
     * A valid key, based on syntax, is required when schema validation is on.
     *
     * @throws SchemaViolationException if the key is not properly formatted for schemas
     */
    @Test
    public void testConstructorRequiresSchemas() throws SchemaViolationException {
        final String multipleHashes = "too#many#hashes";
        final String noFragmentPropertyKey = "I_has_no_hash";

        try {
            parser.getPropertyName(noFragmentPropertyKey);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow.  Eat exception.
        }

        try {
            parser.getSchemaIdentifier(noFragmentPropertyKey);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow.  Eat exception.
        }

        try {
            parser.getPropertyName(multipleHashes);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow. Eat exception.
        }

        try {
            parser.getSchemaIdentifier(multipleHashes);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow. Eat exception.
        }

        //Happy path
        final String schemaIdentifier = "schemaIdentifier";
        final String propertyName = "propertyName";
        final String key = String.format("%s#%s", schemaIdentifier, propertyName);

        assertEquals(schemaIdentifier, parser.getSchemaIdentifier(key));
        assertEquals(propertyName, parser.getPropertyName(key));
    }

    /**
     * Hashes cannot go on the beginning or end of a key.
     */
    @Test
    public void testParserSchemaWithMissingPart() {
        final String identifierWithEmptyFragment = "PropertyNameRequired!#";
        final String fragmentOnly = "#SchemaIdentifierRequired!!";

        try {
            parser.getPropertyName(identifierWithEmptyFragment);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow. Eat exception.
        }

        try {
            parser.getSchemaIdentifier(identifierWithEmptyFragment);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow. Eat exception.
        }

        try {
            parser.getPropertyName(fragmentOnly);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow. Eat exception.
        }

        try {
            parser.getSchemaIdentifier(fragmentOnly);
            fail();
        } catch (final SchemaViolationException e) {
            // Correct flow. Eat exception.
        }
    }
}
