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

import static ezbake.data.graph.blueprints.util.SchemaTestHelpers.getEncodedVisibility;
import static ezbake.data.graph.blueprints.util.SchemaTestHelpers.getObjectPropertyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.data.graph.blueprints.util.Assert;
import ezbake.data.graph.blueprints.visibility.NullVisibilityDeserializer;
import ezbake.data.graph.blueprints.visibility.PermissionContext;
import ezbake.data.graph.blueprints.visibility.PropertyFilter;
import ezbake.data.graph.blueprints.visibility.VisibilityDeserializer;
import ezbake.data.graph.blueprints.visibility.VisibilityFilterGraph;
import ezbake.security.permissions.PermissionUtils;

/**
 * Tests operation of the schema filter with an actual backend and visibility filter. Note that, in these tests, 'URI'
 * is used interchangeably with 'schema identifier' as the {@link DefaultSchemaContext} and {@link
 * DefaultPropertySchemaManager} are intended to use URIs for schema identifiers.
 */
public class SchemaIntegrationTest {

    //Schema URIs
    private static final String HERBIVORE_ZEBRA_SCHEMA = "http://herbivores.org/zebra";
    private static final String CARNIVORE_ZEBRA_SCHEMA = "http://carnivores.net/zebra";
    private static final String CARNIVORE_LION_SCHEMA = "http://carnivores.net/lion";

    //description property and key
    private static final String DESCRIPTION_PROPERTY_NAME = "description";
    private static final String SCHEMA_PROPERTY_KEY_FORMAT = "%s#%s";
    private static final String HERBIVORE_ZEBRA_DESCRIPTION_KEY =
            String.format(SCHEMA_PROPERTY_KEY_FORMAT, HERBIVORE_ZEBRA_SCHEMA, DESCRIPTION_PROPERTY_NAME);

    private static final String CARNIVORE_ZEBRA_DESCRIPTION_KEY =
            String.format(SCHEMA_PROPERTY_KEY_FORMAT, CARNIVORE_ZEBRA_SCHEMA, DESCRIPTION_PROPERTY_NAME);

    private static final String CARNIVORE_LION_DESCRIPTION_KEY =
            String.format(SCHEMA_PROPERTY_KEY_FORMAT, CARNIVORE_LION_SCHEMA, DESCRIPTION_PROPERTY_NAME);

    //speed property and key
    private static final String SPEED_PROPERTY_NAME = "speed";
    private static final String CARNIVORE_ZEBRA_SPEED_KEY =
            String.format(SCHEMA_PROPERTY_KEY_FORMAT, CARNIVORE_ZEBRA_SCHEMA, SPEED_PROPERTY_NAME);

    /**
     * A URI that, in these examples,  is meant to be a way of agreeing upon the identity of a vertex across diverse
     * groups/perspectives.
     */
    private static final String IDENTIFIER_SCHEMA = "http://letsworktogether.com/identifiers";

    /**
     * Something that might be expected to be a unique value.
     */
    private static final String EXAMPLE_IDENTIFIER = "social-security-number";

    /**
     * Schema URI + property name for an example identifier.
     */
    private static final String IDENTIFIER_EXAMPLE_KEY = String.format(SCHEMA_PROPERTY_KEY_FORMAT, IDENTIFIER_SCHEMA, EXAMPLE_IDENTIFIER);

    /**
     * A schema on which no properties are defined. Assigning the schema to a vertex means it can be used for queries of
     * elements with that schema, even if it has no properties.
     */
    private static final String UNLIKELY_FRIENDS_SCHEMA = "http://unlikelyFriends.com";

    /**
     * An id for a frequently referred to {@link com.tinkerpop.blueprints.Vertex} in these tests.
     */
    private static final String VERTEX_ID_1 = "zebra1";

    /**
     * A possible value for a zebra description property.
     */
    private static final String START_ZEBRA_VERTEX_DESCRIPTION = "Another friendly herbivore.";

    /**
     * SchemaManager which keeps track of the Schemas available to the context.
     */
    private PropertySchemaManager schemaManager;

    /**
     * SchemaContext which keeps track of the Schemas available to the elements.
     */
    private SchemaContext schemaContext;

    /**
     * The schema filter. Wraps the visibility filter ({@link ezbake.data.graph.blueprints.visibility
     * .VisibilityFilterGraph} which wraps a {@link com.tinkerpop.blueprints.impls.tg.TinkerGraph}.
     */
    private Graph wrapperGraph;

    /**
     * Gets a VisibilityFilterGraph that is permissive in terms of visibility, but still validates the data structure of
     * things passing through.
     *
     * @return a VisibilityFilterGraph that is all-permissive, but still validates data structure.
     */
    private static VisibilityFilterGraph getPermissiveVisibilityFilterGraph() {
        return new VisibilityFilterGraph(
                new TinkerGraph(), new PermissionContext() {
            @Override
            public Set<Permission> getPermissions(Visibility visibility) {
                return PermissionUtils.ALL_PERMS;
            }

            @Override
            public VisibilityDeserializer getElementVisibilityDeserializer() {
                return new NullVisibilityDeserializer();
            }

            @Override
            public VisibilityDeserializer getPropertyVisibilityDeserializer() {
                return new NullVisibilityDeserializer();
            }
        });
    }

    /**
     * Gets a value that is appropriate for the {@linkplain SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property}.
     * Note that the visibility MUST be empty on all schema property values.
     *
     * @return a value that is appropriate for the schema property
     */
    private static List<Map<String, Object>> getSchemaValue(String... schemaURIs) {

        final List<Map<String, Object>> schemaValue = new ArrayList<>();

        for (String schemaURI : schemaURIs) {
            final Map<String, Object> newSchemaValue = new HashMap<>();
            newSchemaValue.put(PropertyFilter.VALUE_KEY, schemaURI);
            newSchemaValue.put(PropertyFilter.VISIBILITY_KEY, getEncodedVisibility());
            schemaValue.add(newSchemaValue);
        }

        return schemaValue;
    }

    /**
     * Sets up the filters, {@link PropertySchema}s and {@link SchemaContext}, with the filters wrapped around a {@link
     * com.tinkerpop.blueprints.impls.tg.TinkerGraph}.
     *
     * @throws SchemaViolationException if an exception occurs while initializing the schemas
     */
    @Before
    public void setUp() throws SchemaViolationException {
        setUpSchemas();
        setUpContext();
        setUpGraph();
    }

    /**
     * Tests basic {@link com.tinkerpop.blueprints.GraphQuery} on system. Gets vertices with the property given on
     * setup.
     */
    @Test
    public void testGraphQuery() {
        addStartVertex();
        final GraphQuery query = wrapperGraph.query();
        final Iterable<Vertex> it = query.has(IDENTIFIER_EXAMPLE_KEY, 123456789).vertices();
        Assert.assertElementIds(Sets.newHashSet(VERTEX_ID_1), it);
    }

    /**
     * Tests a basic {@link com.tinkerpop.blueprints.VertexQuery}. In this example, different organizations are able to
     * use the same property names, but are able to assign and refer to values specific to their differing
     * perspectives.
     */
    @Test
    public void testVertexQuery() {
        addStartVertex();

        //query the vertex added in 'addStartVertex()'
        Vertex startVertex = wrapperGraph.query().has(IDENTIFIER_EXAMPLE_KEY, 123456789).vertices().iterator().next();

        //set properties on the 'start vertex'
        startVertex.setProperty(CARNIVORE_ZEBRA_SPEED_KEY, getObjectPropertyValue(100));
        startVertex.setProperty(
                CARNIVORE_ZEBRA_DESCRIPTION_KEY, getObjectPropertyValue("Edible, but probably too fast to catch."));

        // make a 'friend' vertex that will be connected to the start vertex
        final Vertex friendOfAZebra = wrapperGraph.addVertex("zebraFriend");
        friendOfAZebra.setProperty(
                SchemaFilterElement.SCHEMA_PROPERTY_KEY, getSchemaValue(
                        HERBIVORE_ZEBRA_SCHEMA, CARNIVORE_ZEBRA_SCHEMA, IDENTIFIER_SCHEMA));

        friendOfAZebra.setProperty(IDENTIFIER_EXAMPLE_KEY, getObjectPropertyValue(new Integer(111111111)));
        friendOfAZebra.setProperty(
                HERBIVORE_ZEBRA_DESCRIPTION_KEY, getObjectPropertyValue("A zebra who loves berries; lots of berries."));

        friendOfAZebra.setProperty(CARNIVORE_ZEBRA_DESCRIPTION_KEY, getObjectPropertyValue("looks delicious"));
        final int slow = 5;
        friendOfAZebra.setProperty(CARNIVORE_ZEBRA_SPEED_KEY, getObjectPropertyValue(slow));

        //connect the 'friend' vertex to the start vertex.
        wrapperGraph.addEdge("edgeId", startVertex, friendOfAZebra, "friends_with");

        final Vertex uLion = wrapperGraph.addVertex("unidentifiedLion");

        uLion.setProperty(
                SchemaFilterElement.SCHEMA_PROPERTY_KEY, getSchemaValue(
                        CARNIVORE_LION_SCHEMA));

        uLion.setProperty(
                CARNIVORE_LION_DESCRIPTION_KEY, getObjectPropertyValue("A dangerous predator looking for food"));

        Edge hunted = wrapperGraph.addEdge(
                "edgeId2", uLion, wrapperGraph.query().interval(CARNIVORE_ZEBRA_SPEED_KEY, 0, 20)
                        .has(CARNIVORE_ZEBRA_DESCRIPTION_KEY, "looks delicious").vertices().iterator().next(),
                "is_hunting");

        hunted.setProperty(SchemaFilterElement.SCHEMA_PROPERTY_KEY, getSchemaValue(UNLIKELY_FRIENDS_SCHEMA));

        Iterable<Vertex> likelyLions =
                friendOfAZebra.query().has(CARNIVORE_LION_DESCRIPTION_KEY, "A dangerous predator looking for food")
                        .vertices();

        Assert.assertElementIds(Sets.newHashSet(uLion.getId()), likelyLions);

        Iterable zebraFriends =
                friendOfAZebra.query().has(HERBIVORE_ZEBRA_DESCRIPTION_KEY, START_ZEBRA_VERTEX_DESCRIPTION).vertices();

        Assert.assertElementIds(Sets.newHashSet(VERTEX_ID_1), zebraFriends);
    }

    /**
     * Adding an invalid schema value does not work.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetBadSchemaValue() {
        wrapperGraph.addVertex('x')
                .setProperty(SchemaFilterElement.SCHEMA_PROPERTY_KEY, getSchemaValue("invalidSchema"));
    }

    /**
     * Setting a property that is not found in any of the schemas on an element should fail.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetPropNotOnElement() {
        wrapperGraph.addVertex('x').setProperty(HERBIVORE_ZEBRA_DESCRIPTION_KEY, getObjectPropertyValue(44.0D));
    }

    /**
     * Setting a property with a value that is not of the correct data type should fail.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetInvalidDatatype() {
        addStartVertex();
        wrapperGraph.getVertex(VERTEX_ID_1).setProperty(IDENTIFIER_EXAMPLE_KEY, getObjectPropertyValue("abc"));
    }

    /**
     * Prepares a {@link PropertySchemaManager} with an usable {@link PropertySchema}.
     *
     * @throws SchemaViolationException if an exception is thrown due to invalid schema values.
     */
    private void setUpSchemas() throws SchemaViolationException {
        final Map<String, RDFDatatype> schemaProps = new HashMap<>();
        schemaProps.put(DESCRIPTION_PROPERTY_NAME, XSDDatatype.XSDstring);
        schemaProps.put(SPEED_PROPERTY_NAME, XSDDatatype.XSDinteger);

        final Map<String, RDFDatatype> identifierSchemaProps = new HashMap<>();
        identifierSchemaProps.put(EXAMPLE_IDENTIFIER, XSDDatatype.XSDpositiveInteger);

        final PropertySchema herbZebraSchema = new RdfPropertySchema(HERBIVORE_ZEBRA_SCHEMA, schemaProps);
        final PropertySchema carniZebraSchema = new RdfPropertySchema(CARNIVORE_ZEBRA_SCHEMA, schemaProps);
        final PropertySchema carniLionSchema = new RdfPropertySchema(CARNIVORE_LION_SCHEMA, schemaProps);

        final PropertySchema indentifierSchema = new RdfPropertySchema(IDENTIFIER_SCHEMA, identifierSchemaProps);
        final PropertySchema noPropertiesSchema = new RdfPropertySchema(UNLIKELY_FRIENDS_SCHEMA, null);

        schemaManager = new DefaultPropertySchemaManager();
        schemaManager.addSchema(herbZebraSchema);
        schemaManager.addSchema(carniZebraSchema);
        schemaManager.addSchema(carniLionSchema);
        schemaManager.addSchema(indentifierSchema);
        schemaManager.addSchema(noPropertiesSchema);
    }

    /**
     * Initializes a {@link SchemaContext} with a schema policy that validates schemas and the schema manager created in
     * {@code setUp}.
     */
    private void setUpContext() {
        schemaContext = new DefaultSchemaContext(schemaManager);
    }

    /**
     * Prepares the {@link com.tinkerpop.blueprints.impls.tg.TinkerGraph} wrapped in a visibility filter, wrapped in the
     * schema filter.
     */
    private void setUpGraph() {
        //set up a graph around a visibility filter that is all permissive.
        wrapperGraph = new SchemaFilterGraph(
                getPermissiveVisibilityFilterGraph(), schemaContext);
    }

    /**
     * Adds a vertex to the graph used; a good place to start for building tests.
     */
    private void addStartVertex() {
        final Vertex v1 = wrapperGraph.addVertex(VERTEX_ID_1);
        v1.setProperty(
                SchemaFilterElement.SCHEMA_PROPERTY_KEY,
                getSchemaValue(HERBIVORE_ZEBRA_SCHEMA, CARNIVORE_ZEBRA_SCHEMA, IDENTIFIER_SCHEMA));
        v1.setProperty(IDENTIFIER_EXAMPLE_KEY, getObjectPropertyValue(new Integer(123456789)));
        v1.setProperty(HERBIVORE_ZEBRA_DESCRIPTION_KEY, getObjectPropertyValue(START_ZEBRA_VERTEX_DESCRIPTION));
    }
}
