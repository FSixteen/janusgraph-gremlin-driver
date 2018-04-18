package com.xyshzh.gremlin.execute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xyshzh.gremlin.client.Connection;

/**
 * 
 * @author Shengjun Liu
 * @version 2018-01-31
 *
 */
public class Execute {

  private static final Logger log = LoggerFactory.getLogger(Execute.class);

  private Connection connection = null;

  public Execute(Connection connection) {
    this.connection = connection;
  }

  public List<Result> getResults(String query, Map<String, Object> args) {
    log.info("getResults::Query::" + query + "::args.size=" + ((null == args) ? 0 : args.size()));
    List<Result> results = new ArrayList<>();
    Long start = System.currentTimeMillis();
    try {
      Client client = connection.getClient();
      if (null == args || 1 > args.size()) {
        results = client.submit(query).all().get();
      } else {
        results = client.submit(query, args).all().get();
      }
      connection.releaseClient(client);
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    log.info(this.getClass().getPackage().getName() + " :: getResults :: Time :: "
        + ((System.currentTimeMillis() - start) + " ms ."));
    return results;
  }

  public Result getResult(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    if (null != results && 1 >= results.size()) {
      return results.get(0);
    } else {
      return null;
    }
  }

  public String getString(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getString() : null;
  }

  public int getInt(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getInt() : -1;
  }

  public byte getByte(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getByte() : -1;
  }

  public short getShort(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getShort() : -1;
  }

  public long getLong(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getLong() : -1L;
  }

  public float getFloat(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getFloat() : -1.0f;
  }

  public double getDouble(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getDouble() : -1.0;
  }

  public boolean getBoolean(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getBoolean() : false;
  }

  public boolean isNull(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.isNull() : true;
  }

  public Vertex getVertex(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getVertex() : null;
  }

  public Edge getEdge(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getEdge() : null;
  }

  public Element getElement(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getElement() : null;
  }

  public Map<String, Object> getElementMap(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    if (null != result) {
      HashMap<String, Object> elementMap = new HashMap<>();
      Element element = result.getElement();
      if (element.id() instanceof RelationIdentifier) {
        RelationIdentifier id = (RelationIdentifier) element.id();
        elementMap.put("id", id.getRelationId());
        elementMap.put("outVertexId", id.getOutVertexId());
        elementMap.put("inVertexId", id.getInVertexId());
        elementMap.put("relationId", id.getRelationId());
      } else {
        elementMap.put("id", element.id());
      }
      elementMap.put("label", element.label());
      element.keys().forEach(key -> {
        elementMap.put(key, element.value(key));
      });
      return elementMap;
    } else {
      return null;
    }
  }

  public Path getPath(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getPath() : null;
  }

  @SuppressWarnings("unchecked")
  public <V> Property<V> getProperty(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? (Property<V>) result.getObject() : null;
  }

  @SuppressWarnings("unchecked")
  public <V> VertexProperty<V> getVertexProperty(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? (VertexProperty<V>) result.getObject() : null;
  }

  public <T> T get(String query, Map<String, Object> args, final Class<? extends T> clazz) {
    Result result = getResult(query, args);
    return (null != result) ? clazz.cast(result.getObject()) : null;
  }

  public <K, V> Map<K, V> getMap(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.get(Map.class) : null;
  }

  public Object getObject(String query, Map<String, Object> args) {
    Result result = getResult(query, args);
    return (null != result) ? result.getObject() : null;
  }

  public Vertex[] getVertices(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    Vertex[] vertices = new Vertex[results.size()];
    for (int i = results.size() - 1; i >= 0; i--) {
      vertices[i] = results.get(i).getVertex();
    }
    return vertices;
  }

  public Edge[] getEdges(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    Edge[] edges = new Edge[results.size()];
    for (int i = results.size() - 1; i >= 0; i--) {
      edges[i] = results.get(i).getEdge();
    }
    return edges;
  }

  public Element[] getElements(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    Element[] elements = new Element[results.size()];
    for (int i = results.size() - 1; i >= 0; i--) {
      elements[i] = results.get(i).getElement();
    }
    return elements;
  }

  public List<Map<String, Object>> getElementsMap(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    List<Map<String, Object>> elementsMap = new ArrayList<>();
    for (Result result : results) {
      HashMap<String, Object> elementMap = new HashMap<String, Object>();
      Element element = result.getElement();
      if (element.id() instanceof RelationIdentifier) {
        RelationIdentifier id = (RelationIdentifier) element.id();
        elementMap.put("id", id.getRelationId());
        elementMap.put("outVertexId", id.getOutVertexId());
        elementMap.put("inVertexId", id.getInVertexId());
        elementMap.put("relationId", id.getRelationId());
      } else {
        elementMap.put("id", element.id());
      }
      elementMap.put("label", element.label());
      element.keys().forEach(key -> {
        elementMap.put(key, element.value(key));
      });
      elementsMap.add(elementMap);
    }
    return elementsMap;
  }

  public Path[] getPaths(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    Path[] paths = new Path[results.size()];
    for (int i = results.size() - 1; i >= 0; i--) {
      paths[i] = results.get(i).getPath();
    }
    return paths;
  }

  public Object[] getObjects(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    Object[] objects = new Object[results.size()];
    for (int i = results.size() - 1; i >= 0; i--) {
      objects[i] = results.get(i).getObject();
    }
    return objects;
  }

  public <K, V> Map<K, V>[] getMaps(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    Map<K, V>[] maps = new LinkedHashMap[results.size()];
    for (int i = results.size() - 1; i >= 0; i--) {
      maps[i] = results.get(i).get(Map.class);
    }
    return maps;
  }

  public void getAllClass(String query, Map<String, Object> args) {
    List<Result> results = getResults(query, args);
    results.forEach((result) -> {
      System.out.println(result.getObject().getClass());
    });
  }

}
