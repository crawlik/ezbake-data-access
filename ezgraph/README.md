# EzBake Graph

EzBake Graph (EzGraph) provides a set of filters that modify the behavior of [TinkerPop
Blueprints](https://github.com/tinkerpop/blueprints/wiki) graphs to add support for EzBake access controls and schemas.
It also provides a network interface to such graphs through a [Rexster](https://github.com/tinkerpop/rexster) server can
handle EzBake security credentials.

## Blueprints Usage
 
Because EzGraph filters run server-side and do not modify the Blueprints API, clients typically do not need to include
any EzGraph-specific libraries into their applications. However, for development and test, it may be helpful to run a
Blueprints graph locally; in which case, the EzGraph Blueprints filters may be included in a project using Maven:

``` 
<dependency> 
  <groupId>ezbake.data</groupId> 
  <artifactId>ezbake-blueprints</artifactId>
  <version>2.1-SNAPSHOT</version> 
  <scope>test</scope>
</dependency> 
```

While the in-process filter interface is convenient for unit testing and debugging, interfacing with Rexster may involve
some type coercion so we recommend that developers integration test with Rexster running the visibility filter when
possible.

## Visibility Filter
 
The visibility filter controls access to both elements (vertices and edges) and individual properties in those elements
using EzBake access controls. Elements containing a special property, `ezbake_visibility`, check callers' authorizations
before allowing access to their contents. Because individual properties may be stored at different formal visibilities,
all properties are multi-valued. That is, a single property key is *always* mapped to a list of maps, where each map
contains the fields "value" (corresponding to the value of the property in a single-valued Blueprints element) and
"visibility" (containing that property value's EzBake visibility). In all cases, visibilities are stored as
Base64-encoded serialized Thrift Visibility objects. For additional details on the visibility model, see
<https://docs.google.com/a/42six.com/document/d/1gpumgOOCIfSbxqBGAl_ipbDjZAPUvv2n6cllhKqIk4c>.

It may be helpful to develop and test EzGraph-using applications running a local graph with a local visibility filter.
To do so, include the `ezbake-blueprints` artifact into the application project. Whenever the graph would be accessed
over Rexster, instead wrap the visibility filter around a local storage graph of choice (`TinkerGraph` used in the
examples):

```java 
EzSecurityToken token = // Obtain a security token from somewhere; 
Graph graph = new VisibilityFilterGraph(new TinkerGraph(), new DefaultPermissionContext(token));
```

To store a visibility on an element,

```java
// Create visibilities as desired.
Visibility visU = new Visibility();
visU.setFormalVisibility("U");

Vertex vertex = graph.addVertex(0); 
vertex.setProperty("ezbake_visibility", ThriftUtils.serializeToBase64(visU)); 
```

In addition to formal visibilities, EzGraph supports advanced markings and platform object visibilities.

The major difference between EzGraph and vanilla Blueprints graphs is that all properties are stored as lists of maps
where each map has a "value" field and a "visibility" field. This is required to support having property values at
different visibilities so is required for all properties on all elements. For example, to store the color green with
visibility "U" and color red with visibility "S":

```java
Map<String, Object> valGreen = new HashMap<>();
valGreen.put("value", "green");
valGreen.put("visibililty", ThriftUtils.serializeToBase64(visU));

Visibility visS = new Visibility();
visS.setFormatVisibility("S");

Map<string, Object> valRed = new HashMap<>();
valRed.put("value", "red");
valRed.put("visibility", ThriftUtils.serializeToBase64(visS));

vertex.setProperty("color", Lists.newArrayList(valGreen, valRed));
```

Calls to `setProperty` merge lists rather than overwrite, so the following calls are equivalent to the one above:

```java
// Set properties serially
vertex.setProperty("color", Collections.singletonList(valGreen)); 
vertex.setProperty("color", Collections.singletonList(valRed)); 
```

To delete a value from a property, the property value map may contain a field called "delete" with a boolean value. If
true, the map from the list of maps with both value and visibility is deleted. For example:

```java
Map<string, Object> valDel = new HashMap<>();
valDel.put("delete", true);

// Does nothing because no value "red" with visibility U
valDel.put("value", "red");
valDel.put("visibility", ThriftUtils.serializeToBase64(visU));
vertex.setProperty("color", Collections.singletonList(valDel));

// Does nothing because no value "green" with visibility S
valDel.put("value", "green");
valDel.put("visibility", ThriftUtils.serializeToBase64(visS));
vertex.setProperty("color", Collections.singletonList(valDel));

// Deletes value "red" with visibility S
valDel.put("value", "red");
valDel.put("visibility", ThriftUtils.serializeToBase64(visU));
vertex.setProperty("color", Collections.singletonList(valDel));
```

If the caller has permission to write every value on a property, the caller may delete all of them at once using
`myElement.removeProperty()`.

If the caller has permission to remove every value on every property of an edge (from the previous statement) as well as
the edge itself, the caller may remove an edge using `myEdge.remove()` or `myGraph.removeEdge()`.

If the caller has permission to remove every edge incident to a vertex (from the previous statement) and the vertex
itself, the caller may remove the vertex using `myVertex.remove()` or `myGraph.removeVertex()`.

## Schema Filter

The schema filter provides a way for users to define what properties they believe should be on a graph element and
namespace those properties to both protect them from misuse and to faciliate cooperation between teams.

The schema filter is initialized with a single `SchemaContext` which knows a single `PropertySchemaManager`.
This manager provides methods for accessing and creating PropertySchemas. The known set of schemas is
used to determine what schemas may be added to an element via the `SchemaContext`.

### Property Schemas

A property schema, whose behavior is defined by `PropertySchema`, is intended to be a namespaced set of property
definitions that can be 'assigned' to a graph element.  These property definitions provide a map of property names to
their acceptable datatypes that may be used on an element with the given schema. The `PropertySchema` also provides
methods for validating that a given `propertyName:value` combination is valid with the given property definitions.

### PropertySchemaManager
A `PropertySchemaManager` knows a set of PropertySchemas  and provides methods for creating new schemas and
accessing already existing ones. It also has methods for validating that it knows of a given schema (by identifier).

### SchemaContext
The `SchemaContext` is an abstraction  for handling schemas and provides a set of methods for validating them.  
The `SchemaContext` is initialized with a `PropertySchemaManager` and is given to a `SchemaFilterGraph` for use.
The `SchemaContext` generally uses its `PropertySchemaManager` to determine the validity of schemas and schema properties.


### SchemaFilterGraph
The SchemaFilterGraph wraps a `VisibilityFilterGraph and` provides 'schema validation'. Before setting a property
on an element, it is verified that the element has a schema that defines that property.  The Schema filter, is thus
composed as such: 
 
 - A `VisibilityFilterGraph` is wrapped by the `SchemaFilterGraph`
 - The `SchemaFilterGraph` has a  `SchemaContext`
 - The `SchemaContext` has a `PropertySchemaManager` 
 - The `PropertySchemaManager` has many `PropertySchema`s
 - A given element in the wrapped graph can have any number of `PropertySchema`s known to this system added to it

The properties that can be set on an element are determined by the 
`PropertySchema`s that have been added to the given element.

### RDF Implementation
An implementation based loosely on the RDF model is provided. Schemas are assigned to elements via a special property
with key `ezbake_schema`. Classes include `RdfPropertySchema`, `DefaultPropertySchemaManager`, and
`DefaultSchemaContext`. Notably, this implementation relies on the [core Jena RDF API](http://jena.apache.org/documentation/rdf/)
 to validate the data types of an `RdfPropertySchema`'s properties and it is expected that schema
 identifiers are assigned URI String values. Deleting schemas from elements is not allowed, 
nor is deleting or updating schemas in the schema manager. Empty visibilities on schemas are required.

### Using the RDF Implementation
Without a schema on an element, no properties can be set on that element. With the RDF implementation, we can take
advantage of schemas in a few easy steps. For the complete example see:
`ezbake.data.graph.blueprints.schema.SchemaIntegrationTest`.

#### Set up an RdfPropertySchema
These steps are assuming that the graph has not yet been set up.  In constrast to these directions, one might first set
up the `PropertySchemaManager`, `DefaultSchemaContext`, and `SchemaFilterGraph` before adding any schemas to
the `PropertySchemaManager`.

Define a set of schema properties:

```java
final Map<String, RDFDatatype> schemaProps = new HashMap<>(); 
schemaProps.put(DESCRIPTION_PROPERTY_NAME, XSDDatatype.XSDstring); 
schemaProps.put(SPEED_PROPERTY_NAME, XSDDatatype.XSDinteger);
```

Create a schema with the new property definitions:

```java
final PropertySchema carniZebraSchema = new RdfPropertySchema(CARNIVORE_ZEBRA_SCHEMA, schemaProps); 
//Where 'CARNIVORE_ZEBRA_SCHEMA' is the schema's URI/Identifier.
```

Create a `PropertySchemaManager` and add the new schema to it:

```java
schemaManager = new DefaultPropertySchemaManager(); 
schemaManager.addSchema(carniZebraSchema); 
```

#### Set up the `SchemaContext`:

This step might be done first, followed by adding the schemas (explained above). In that case,  the
`PropertySchemaManager` would also be initialized in this section.

```java
schemaContext = new DefaultSchemaContext(schemaManager);
```

#### Set up the graph: 

```java
//set up a graph around a visibility filter that is all permissive.
wrapperGraph = new SchemaFilterGraph(getPermissiveVisibilityFilterGraph(), schemaContext); 
```

In this case the visibility filter is set up to authorize all CRUD operations and it is a wrapper around Blueprints'
Tinkergraph. See the `SchemaIntegrationTest` for more info.


#### Add a new element, set its `ezbake_schema` property and add a property defined in any schema it is given.

Add the new element:

```java
final Vertex v1 = wrapperGraph.addVertex(VERTEX_ID_1);
```

Create the value for the schema property, which includes the schema created in the schema setup section:

```java
final List<Map<String, Object>> schemaValue = new ArrayList<>(); 
final Map<String, Object> newSchemaValue = new HashMap<>(); 
newSchemaValue.put(PropertyFilter.VALUE_KEY, CARNIVORE_ZEBRA_SCHEMA);
//Note that we add identifier 'CARNIVORE_ZEBRA_SCHEMA' as the value, not the schema itself.

newSchemaValue.put(PropertyFilter.VISIBILITY_KEY, getEncodedVisibility()); 
// getEncodedVisibility() returns a base64 encoded empty visibility schemaValue.add(newSchemaValue); 
```

Set the schema property on the newly created vertex: 

```java
v1.setProperty(SchemaFilterElement.SCHEMA_PROPERTY_KEY,schemaValue);
```

Set a property included in the schema we just added to the vertex:

```java
v1.setProperty(CARNIVORE_ZEBRA_DESCRIPTION_KEY, getObjectPropertyValue(START_ZEBRA_VERTEX_DESCRIPTION)); 
```

Note that `CARNIVORE_ZEBRA_DESCRIPTION_KEY` is composed like this:  `<SCHEMA_IDENTIFIER>#<PROPERTY_NAME>`. In
this case:

```java
CARNIVORE_ZEBRA_DESCRIPTION_KEY = CARNIVORE_ZEBRA_SCHEMA + '#' + DESCRIPTION_PROPERTY_NAME
```

Like the above example, schemas can continue to be added to the `PropertySchemaManager` and any of the schemas known
by the manager can be added to elements in the graph.

# Setting up Rexster With EzBake Visiblities and In-Memory Backend

EzGraph uses Rexster to communicate with an 'EzBake-Visibility wrapped' Blueprints backend.

Rexster provides us with its' own in-memory Blueprints graph implementation that can be used for testing.

Below are the steps needed to stand up a Rexster server backed by an in-memory EzBake-Visiblility wrapped Blueprints backend.

## Prepare the server

1. Run  `mvn package`

2. Enter the `rexster-server` directory.

3. Put the properties below somewhere EzConfiguration will pick them up. E.g. in /etc/sysconfig/ezgraph or by setting the `EZCONFIGURATION_DIR` to a location of your choosing and putting them there. Currently, the script looks in rexster-server/src/test/resources/ for ezconfig files which conveniently includes these properties.

```
ezbake.security.client.use.mock=true
ezbake.security.client.mode=MOCK
graph.store.class=ezbake.data.graph.rexster.graphstore.RexsterXmlConfigurationGraphStore
```

4. Run the following to start the Rexster server:

```
src/test/scripts/rexster-server-maven.sh
```

## Run some tests

### cURL
Note: Older versions of curl may not work with these commands. If you experience issues, please update your version of curl to at least 7.40.0 and retry.

An easy way to hit the server to see if its up is a simple curl like:

```bash
curl -u <base64 encoded ezsecurity token>:nopassword http://localhost:8182/graphs
```

for example, a curl with base64 encoded security token with formal visibility set to "A&B" might look like:

```bash
curl -u DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA=:nopass http://localhost:8182/graphs
```

Just querying the available graphs won't necessarily exercise the Visibility features, however.  Once some data has been put in the graph, the token sent above will be used to evaluate the visibilities of data and determine what actually gets returned.

The entire [Rexster Rest API](https://github.com/tinkerpop/rexster/wiki/Basic-REST-API) is available via curl, however, for more complex queries, the java (or another language) bindings for Rexster might be preferable to this approach.

### rexreq.py
`src/test/scripts` contains a python script `rexreq.py` that can be used in place of cURL in any of the examples in this doc.
Requests can be made to Rexster in the following format:

```bash
rexreq.py [-b|--baseurl] COMMAND PATH TOKEN 
```

The default value for `baseurl` is `http://localhost:8182`. Below is the equivalent to performing the cURL mentioned above:

```bash
rexreq.py get "/graphs" DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA
```

### Rexster Rest API via RexsterGraph

[RexsterGraph](https://github.com/tinkerpop/blueprints/wiki/Rexster-Implementation) is a Blueprints wrapper around Rexster's Rest API. It can be uses like any other Blueprints Graph but also provides methods for executing Gremlin queries.

RexsterGraph returns JSON encoded results in the form of JSONObject, JSONArray and others, so these objects can be used to parse the desired values from returned results.

For an example of how to use RexsterGraph, see the test class `ezbake.data.graph.blueprints.VisibilityFilterGraphWrapperIntegrationTest.java` in the rexster-server module of ezgraph.

It may be preferable to use a [GraphSONReader](https://github.com/tinkerpop/blueprints/wiki/GraphSON-Reader-and-Writer-Library), which helps load properties into a blueprints graph type of your choosing, make the data an easy format for you to work with.

### RexPro

[RexPro](https://github.com/tinkerpop/rexster/wiki/RexPro) is Rexster's binary protocol. Requests are made to RexPro via gremlin scripts. A session request is made to RexPro before requests for graph data can be made. In this session request, the user name is set to the user's base64 encoded ez security token.  The password field is unused.

 see the test class `ezbake.data.graph.blueprints.VisibilityFilterGraphWrapperIntegrationTest.java` in the rexster-server module of ezgraph for examples.

### Run canned java code against an EzVisibility enabled Rexster server

Class `ezbake.data.graph.rexster.ITRexsterScript.java` has a main method that can be run against a live Rexster instance with the Rest and Rexpro servers by default at localhost:8182 and localhost:8184, respectively.

### RexsterGraph Creation

Graph creation with Rexster can be tested with an in memory store of graphs. See `ezbake.data.graph.blueprints.graphmgmt.TinkerGraphManager` and `ezbake.data.graph.rexster.graphstore.ManagedGraphStore`.

NOTE: As per cURL section above, the long base64 encoded String is a base64-serialized `EzSecurityToken`

Expected results should all be return with a status `200 OK`.

#### Follow the steps above to prepare and start the server, instead using the below configuration. (See heading `Prepare the server` above for these instructions)

```
ezbake.security.client.use.mock=true
ezbake.security.client.mode=MOCK
graph.store.class=ezbake.data.graph.rexster.graphstore.ManagedGraphStore
graph.manager.class=ezbake.data.graph.blueprints.graphmgmt.TinkerGraphManager
```

#### Get graph management graph (NOTE: auth header is left out here for brevity/readability, see cURLs above for header)

Verify that the graph management graph exists.  This should report a graph called "manage".

```bash
curl -u DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA=:nopass http://localhost:8182/graphs
```
Expected result is (times may vary):

```json
{
    "graphs": [
        "manage"
    ],
    "name": "Rexster: A Graph Server",
    "queryTime": 0.125952,
    "upTime": "0[d]:00[h]:00[m]:45[s]",
    "version": "2.6.0"
}
```

If the graph cannot be found, there is likely an error with the configuration from step 1.

#### Add a vertex to the graph management graph, resulting in the creation of a new graph:

```bash
curl -H “Content-type:application/vnd.rexster-typed-v1+json” -X POST -u DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA=:nopass http://localhost:8182/graphs/manage/vertices/myNewGraph
```
Expected result is (times may vary):

```json
{
    "queryTime": 201.675776,
    "results": {
        "_id": "myNewGraph",
        "_type": "vertex"
    },
    "version": "2.6.0"
}
```

#### cURLing graphs should now yield two graphs, including the one you just created.

```bash
curl -u DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA=:nopass http://localhost:8182/graphs
```

Expected result is (times may vary):

```json
{
    "graphs": [
        "myNewGraph",
        "manage"
    ],
    "name": "Rexster: A Graph Server",
    "queryTime": 0.137984,
    "upTime": "0[d]:00[h]:01[m]:43[s]",
    "version": "2.6.0"
}
```


#### Add a vertex to the new graph, and verify its existence:

Add vertex:

```bash
curl -H “Content-type:application/vnd.rexster-typed-v1+json” -X POST -u DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA=:nopass http://localhost:8182/graphs/myNewGraph/vertices/myNewVertex
```

Expected result is (times may vary):

```json
{
    "queryTime": 0.65792,
    "results": {
        "_id": "myNewVertex",
        "_type": "vertex"
    },
    "version": "2.6.0"
}
```

Verify vertex added:

```bash
curl -u DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA=:nopass http://localhost:8182/graphs/myNewGraph/vertices
```

Expected result is (times may vary):

```json
{
    "queryTime": 2.025984,
    "results": [
        {
            "_id": "myNewVertex",
            "_type": "vertex"
        }
    ],
    "totalSize": 1,
    "version": "2.6.0"
}
```

#### A 'graph vertex' on the graph management graph can also have its ezbake_visibility set (element level visibility).  This will prevent users from opening or removing graphs that do not have read auths.  Currently, managing discover visibility is not supported.

For example:

```bash
curl -H “Content-type:application/vnd.rexster-typed-v1+json” -X POST -u DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA=:nopass http://localhost:8182/graphs/manage/vertices/addVisibility?ezbake_visibility=CwABAAAAAUEA
```
Expected result is (times may vary):

```json
{
    "queryTime": 22.473216,
    "results": {
        "_id": "addVisibility",
        "_type": "vertex",
        "ezbake_visibility": "CwABAAAAAUEA"
    },
    "version": "2.6.0"
}
```

It is also possible to set the "ezbake_visibility" property via another Rexster method.

#### Destroy a graph:

```bash
curl -H “Content-type:application/vnd.rexster-typed-v1+json” -X DELETE -u DAABCwABAAAACVVzZXJVdGlscwsAAgAAAAxtb2NrQXBwU2VjSWQLAAMAAAAHc29tZXNpZAoABgAAAUrt2DVhCwAHAAAADXNvbWVUZXN0VG9rZW4ACAACAAAAAQwAAwsAAQAAAAlwcmluY2lwYWwMAAILAAEAAAAJVXNlclV0aWxzCwACAAAADG1vY2tBcHBTZWNJZAsAAwAAAAdzb21lc2lkCgAGAAABSu3YNWELAAcAAAANc29tZVRlc3RUb2tlbgAADAALDgABCwAAAAIAAAABQQAAAAFCAAA=:nopass http://localhost:8182/graphs/manage/vertices/myNewGraph
```

Expected result is (times may vary):

```json
{
    "queryTime": 0.440832,
    "version": "2.6.0"
}
```

And querying available graphs should no longer include 'myNewGraph'.

For this example any graph manager could be used in place of `TinkerGraphManager` so long as it provides a graph management graph that can be accessed via the name "manage" as done in `TinkerGraphManager` on initialization.
