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

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import ezbake.base.thrift.Visibility;
import ezbake.data.graph.blueprints.util.SchemaTestHelpers;
import ezbake.data.graph.blueprints.visibility.PropertyFilter;
import ezbake.thrift.ThriftUtils;

/**
 * Tests {@link ezbake.data.graph.blueprints.schema.DefaultSchemaContext}.
 */
public class DefaultSchemaContextTest {

    /**
     * Property name used in tests.
     */
    private static final String SCHEMA_PROPERTY = "property";

    /**
     * System under test.
     */
    private DefaultSchemaContext context;

    @Before
    public void setUp() {
        final PropertySchemaManager schemaManager = new DefaultPropertySchemaManager();
        final Map<String, RDFDatatype> properties = new HashMap<>();
        properties.put(SCHEMA_PROPERTY, XSDDatatype.XSDdouble);
        final PropertySchema schema = new RdfPropertySchema(SchemaTestHelpers.ELEMENT_STARTING_SCHEMA, properties);
        schemaManager.addSchema(schema);
        context = new DefaultSchemaContext(schemaManager);
    }

    @Test
    public void testValidateWithValue() throws SchemaViolationException {
        final Double validSchemaPropValue = 11.0;
        final String invalidSchemaPropValue = "invalidValue";

        context.validateSchemaKeyValue(SchemaTestHelpers.ELEMENT_STARTING_SCHEMA, SCHEMA_PROPERTY, validSchemaPropValue);

        try {
            context.validateSchemaKeyValue(
                    SchemaTestHelpers.ELEMENT_STARTING_SCHEMA, "non-existent-property", validSchemaPropValue);
            fail();
        } catch (final SchemaViolationException e) {
            //expected. eat.
        }

        try {
            context.validateSchemaKeyValue("non-existent-schema", SCHEMA_PROPERTY, validSchemaPropValue);
            fail();
        } catch (final SchemaViolationException e) {
            //expected. eat.
        }

        try {
            context.validateSchemaKeyValue(
                    SchemaTestHelpers.ELEMENT_STARTING_SCHEMA, SCHEMA_PROPERTY, invalidSchemaPropValue);
            fail();
        } catch (final SchemaViolationException e) {
            //expected. eat.
        }
    }

    @Test
    public void testValidateKeyOnly() throws SchemaViolationException {
        context.validateSchemaKey(SchemaTestHelpers.ELEMENT_STARTING_SCHEMA, SCHEMA_PROPERTY);

        try {
            context.validateSchemaKey(SchemaTestHelpers.ELEMENT_STARTING_SCHEMA, "non-existent-property");
            fail();
        } catch (final SchemaViolationException e) {
            //expected. eat.
        }

        try {
            context.validateSchemaKey("non-existent-schema", SCHEMA_PROPERTY);
            fail();
        } catch (final SchemaViolationException e) {
            //expected. eat.
        }
    }

    @Test(expected = SchemaViolationException.class)
    public void testValidateSchemaUpdateDoesntAllowNullSchemaValue() {
        final List<Map<String, Object>> schemaValue = SchemaTestHelpers.getStartingSchemaValue();
        final Map<String, Object> schemaValWithNullValue = new HashMap<>();
        schemaValWithNullValue.put(PropertyFilter.VALUE_KEY, null);
        schemaValWithNullValue.put(PropertyFilter.VISIBILITY_KEY, SchemaTestHelpers.getEncodedVisibility());
        schemaValue.add(schemaValWithNullValue);

        context.validateSchemaUpdate(schemaValue);
    }

    @Test(expected = SchemaViolationException.class)
    public void testValidateSchemaUpdateDoesntAllowEmptySchemaValue() {
        final List<Map<String, Object>> schemaValue = SchemaTestHelpers.getStartingSchemaValue();
        final Map<String, Object> schemaValWithBlankValue = new HashMap<>();
        schemaValWithBlankValue.put(PropertyFilter.VALUE_KEY, "");
        schemaValWithBlankValue.put(PropertyFilter.VISIBILITY_KEY, SchemaTestHelpers.getEncodedVisibility());
        schemaValue.add(schemaValWithBlankValue);

        context.validateSchemaUpdate(schemaValue);
    }

    @Test(expected = SchemaViolationException.class)
    public void testValidateSchemaUpdateDoesntAllowNonStringSchemaValue() {
        final List<Map<String, Object>> schemaValue = SchemaTestHelpers.getStartingSchemaValue();
        final Map<String, Object> nonStringSchemaValue = new HashMap<>();
        nonStringSchemaValue.put(PropertyFilter.VALUE_KEY, new Object());
        nonStringSchemaValue.put(PropertyFilter.VISIBILITY_KEY, SchemaTestHelpers.getEncodedVisibility());
        schemaValue.add(nonStringSchemaValue);

        context.validateSchemaUpdate(schemaValue);
    }

    @Test(expected = SchemaViolationException.class)
    public void testValidateSchemaUpdateDoesntAllowUnFoundSchemas() {
        final List<Map<String, Object>> schemaValue = SchemaTestHelpers.getStartingSchemaValue();
        final Map<String, Object> missingSchema = new HashMap<>();
        missingSchema.put(PropertyFilter.VALUE_KEY, "missingSchema");
        missingSchema.put(PropertyFilter.VISIBILITY_KEY, SchemaTestHelpers.getEncodedVisibility());
        schemaValue.add(missingSchema);

        context.validateSchemaUpdate(schemaValue);
    }

    @Test(expected = SchemaViolationException.class)
    public void testValidateSchemaUpdateDoesntAllowNonEmptyVisibility() throws TException {
        final List<Map<String, Object>> schemaValue = SchemaTestHelpers.getStartingSchemaValue();
        final Visibility v = new Visibility();
        v.setFormalVisibility("not&empty");
        final String nonEmptyVis = ThriftUtils.serializeToBase64(v);
        schemaValue.get(0).put(PropertyFilter.VISIBILITY_KEY, nonEmptyVis);

        context.validateSchemaUpdate(schemaValue);
    }

    @Test(expected = SchemaViolationException.class)
    public void testValidateSchemaUpdateDoesntAllowDeleteFlag() {
        final List<Map<String, Object>> schemaValue = SchemaTestHelpers.getStartingSchemaValue();
        schemaValue.get(0).put(PropertyFilter.DELETE_KEY, true);

        context.validateSchemaUpdate(schemaValue);
    }
}
