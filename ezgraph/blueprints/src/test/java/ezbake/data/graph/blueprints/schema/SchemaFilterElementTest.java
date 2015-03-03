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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
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
import ezbake.data.graph.blueprints.stub.ElementStub;
import ezbake.data.graph.blueprints.util.SchemaTestHelpers;
import ezbake.data.graph.blueprints.visibility.PropertyFilter;
import ezbake.thrift.ThriftUtils;

/**
 * Test for {@link SchemaFilterElement}.
 */
public class SchemaFilterElementTest {

    /**
     * A property name used in these tests.
     */
    private static final String PROPERTY_NAME = "height";

    /**
     * Schema manager to get available schemas from.
     */
    private PropertySchemaManager schemaManager;

    /**
     * Context which knows the schema manager, {@link SchemaFilterElement} interacts with this to get schema decisions.
     */
    private SchemaContext context;

    /**
     * The element the {@link SchemaFilterElement} wraps. We use this to see how SchemaFilterElement interacts with it.
     */
    private ElementStub wrappedElementStub;

    /**
     * System under test.
     */
    private SchemaFilterElement element;

    /**
     * Initialize collaborators for {@link SchemaFilterElement}  including a wrapped {@link
     * com.tinkerpop.blueprints.Element} and {@link SchemaContext} which is used to make schema decisions.
     *
     * @throws SchemaViolationException if an error occurs initializing the {@link SchemaContext}, and the schemas on
     * the element.
     */
    @Before
    public void setUp() throws SchemaViolationException {
        schemaManager = new DefaultPropertySchemaManager();
        final Map<String, RDFDatatype> properties = new HashMap<>();
        properties.put(PROPERTY_NAME, XSDDatatype.XSDdouble);

        final PropertySchema schema = new RdfPropertySchema(SchemaTestHelpers.ELEMENT_STARTING_SCHEMA, properties);
        schemaManager.addSchema(schema);

        context = new DefaultSchemaContext(schemaManager);
        wrappedElementStub = new ElementStub();
        element = new SchemaFilterElement(wrappedElementStub, context);
    }

    /**
     * {@code getProperty(key)} delegates call to wrapped element.
     */
    @Test
    public void testGetProperty() {
        final String testKey = "aKey";
        element.getProperty(testKey);
        assertTrue(wrappedElementStub.getPropertyCalled);
        assertEquals(testKey, wrappedElementStub.getPropertyKey);
    }

    /**
     * {@code getPropertyKeys()} delegates call to wrapped element.
     */
    @Test
    public void testGetPropertyKeys() {
        element.getPropertyKeys();
        assertTrue(wrappedElementStub.getPropertyKeysCalled);
    }

    @Test
    public void testSetPropertyAllowsAnyVisiblityValue() {
        Object value = new Object();
        element.setProperty(SchemaFilterElement.VISIBILITY_PROPERTY_KEY, value);
        assertTrue(wrappedElementStub.setPropertyCalled);
        assertEquals(wrappedElementStub.setPropertyKey, SchemaFilterElement.VISIBILITY_PROPERTY_KEY);
        assertEquals(wrappedElementStub.setPropertyValue, value);
    }

    /**
     * Right now the visibility key is a reserved key; this behavior might be better suited somewhere else, but for now
     * this can be an effective solution.
     */
    @Test
    public void testSetPropertyWithVisibilityKey() {
        //exercise setProperty
        final Object testObject = SchemaTestHelpers.getObjectPropertyValue(new Object());
        element.setProperty(SchemaFilterElement.VISIBILITY_PROPERTY_KEY, testObject);

        //verify call went through.
        assertTrue(wrappedElementStub.setPropertyCalled);
        assertEquals(SchemaFilterElement.VISIBILITY_PROPERTY_KEY, wrappedElementStub.setPropertyKey);
        assertSame(testObject, wrappedElementStub.setPropertyValue);
    }

    /**
     * The structure of the value of any property passing through this filter needs to be validated. Confirm this
     * occurs.
     */
    @Test
    public void testStructureValidated() {
        //todo: Check that structure validation is taking place, currently handled by PropertyFilter @blame Charles
        //todo: ideally we're just checking that the validator is being called, rather than duplicating tests
    }

    /**
     * Test cases that should throw exceptions when setProperty is called on the  {@linkplain
     * SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property}.
     *
     * @throws org.apache.thrift.TException if a problem occurs serializing a visibility object
     */
    @Test
    public void testSetPropertyWithSchemaKeyErrorCase() throws TException {
        //Deletes not allowed.
        try {
            final List<Map<String, Object>> valueWithDeletes = SchemaTestHelpers.getStartingSchemaValue();
            valueWithDeletes.get(0).put(PropertyFilter.DELETE_KEY, new Boolean(true));
            element.setProperty(SchemaFilterElement.SCHEMA_PROPERTY_KEY, valueWithDeletes);
            fail();
        } catch (final SchemaViolationException e) {
            // correct flow - eat exception.
        }

        // Can't add schemas that aren't in the schema context.
        try {
            element.setProperty(
                    SchemaFilterElement.SCHEMA_PROPERTY_KEY, SchemaTestHelpers.getNewSchemaValue("notInSchemaContext"));
            fail();
        } catch (final SchemaViolationException e) {
            // correct flow - eat exception.
        }

        assertFalse(wrappedElementStub.setPropertyCalled);
        assertNull(wrappedElementStub.setPropertyKey);
        assertNull(wrappedElementStub.setPropertyValue);

        //Visibility on schema property value must be empty visibility.
        final Visibility badVis = new Visibility();
        badVis.setFormalVisibility("elephants&cheetahs");
        final String badVisAsString = ThriftUtils.serializeToBase64(badVis);
        // Can't add schemas that aren't in the schema context.
        try {
            final List<Map<String, Object>> schemaWithBadVis = SchemaTestHelpers.getStartingSchemaValue();
            schemaWithBadVis.get(0).put(PropertyFilter.VISIBILITY_KEY, badVisAsString);
            element.setProperty(SchemaFilterElement.SCHEMA_PROPERTY_KEY, schemaWithBadVis);
            fail();
        } catch (final SchemaViolationException e) {
            // correct flow - eat exception.
        }

        assertFalse(wrappedElementStub.setPropertyCalled);
        assertNull(wrappedElementStub.setPropertyKey);
        assertNull(wrappedElementStub.setPropertyValue);
    }

    /**
     * Test happy path adding a schema to the  {@linkplain SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property}.
     *
     * @throws SchemaViolationException if validation does not succeed as expected
     */
    @Test
    public void testSetPropertySchemaProperty() throws SchemaViolationException {
        String newSchema = "aNewSchema";
        // Add the schema into the schema context.
        schemaManager.addSchema(new RdfPropertySchema(newSchema, new HashMap<String, RDFDatatype>()));

        //Get a new schema value (with new schema)
        final List<Map<String, Object>> schemaValue = SchemaTestHelpers.getNewSchemaValue(newSchema);

        // Exercise by called setProperty with newly build schema value.
        element.setProperty(SchemaFilterElement.SCHEMA_PROPERTY_KEY, schemaValue);

        //verify setProperty was called on the wrapped element with expected values.
        assertTrue(wrappedElementStub.setPropertyCalled);
        assertEquals(SchemaFilterElement.SCHEMA_PROPERTY_KEY, wrappedElementStub.setPropertyKey);
        assertEquals(schemaValue, wrappedElementStub.setPropertyValue);
    }

    /**
     * Should be no issue with setting a property when all schema validation is off, but when it is on there will be an
     * exception. For more tests of property key validation see {@link DefaultPropertyKeyParserTest}.
     */
    @Test
    public void testSetPropertyNormalPropertySchemaless() {
        final String noSchemaKey = "NoSchema";
        final Object value = SchemaTestHelpers.getObjectPropertyValue(new Object());

        //Invalid schema syntax when validate schemas is on.
        try {
            element.setProperty(noSchemaKey, SchemaTestHelpers.getObjectPropertyValue(new Object()));
            fail();
        } catch (final SchemaViolationException e) {
            // correct flow - eat exception.
        }

        try {
            element.setProperty(noSchemaKey, value);
            fail();
        } catch (final SchemaViolationException e) {
            // correct flow - eat exception.
        }
    }

    /**
     * Test {@code setProperty(...)} with a key with valid syntax. When schemas are on, properties that can be validated
     * by the {@link SchemaContext} should be allowed, and values that cannot will result in a thrown exception.
     */
    @Test
    public void testSetPropertyKeyHasSchema() {
        final String notFoundKey = String.format("%s#notFoundPropName", SchemaTestHelpers.ELEMENT_STARTING_SCHEMA);
        final Object objectValue = SchemaTestHelpers.getObjectPropertyValue(new Object());

        //Schema-property combination must exist.
        try {
            element.setProperty(notFoundKey, objectValue);
            fail();
        } catch (final SchemaViolationException e) {
            // correct flow - eat exception.
        }

        final String propertyKey = String.format("%s#%s", SchemaTestHelpers.ELEMENT_STARTING_SCHEMA, PROPERTY_NAME);
        //Valid property key must have correct value.
        try {
            element.setProperty(propertyKey, objectValue);
            fail();
        } catch (final SchemaViolationException e) {
            // correct flow - eat exception.
        }

        final Object doubleValue = SchemaTestHelpers.getObjectPropertyValue(11.0);
        // happy path.
        element.setProperty(propertyKey, doubleValue);

        //verify call went through.
        assertTrue(wrappedElementStub.setPropertyCalled);
        assertEquals(propertyKey, wrappedElementStub.setPropertyKey);
        assertEquals(doubleValue, wrappedElementStub.setPropertyValue);
    }

    /**
     * {@code removeProperty(key)} just delegates to the wrapped {@link com.tinkerpop.blueprints.Element}.
     */
    @Test
    public void testRemoveProperty() {
        final String key = "bob";
        element.removeProperty(key);
        assertTrue(wrappedElementStub.removePropertyCalled);
        assertEquals(key, wrappedElementStub.removePropertyKey);
    }

    /**
     * {@code remove()} just delegates to the wrapped {@link com.tinkerpop.blueprints.Element}.
     */
    @Test
    public void testRemove() {
        element.remove();
        assertTrue(wrappedElementStub.removeCalled);
    }

    /**
     * {@code getId()} just delegates to the wrapped {@link com.tinkerpop.blueprints.Element}.
     */
    @Test
    public void testGetId() {
        //check that it is canned response from wrapped element.
        assertEquals("id", element.getId());
        assertTrue(wrappedElementStub.getIdCalled);
    }

    @Test
    public void testGetContext() {
        assertEquals(context, element.getContext());
    }
}
