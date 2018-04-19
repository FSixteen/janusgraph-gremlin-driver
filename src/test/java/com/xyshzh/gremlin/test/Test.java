package com.xyshzh.gremlin.test;

import java.util.List;
import java.util.Map;

import com.xyshzh.gremlin.client.Connection;
import com.xyshzh.gremlin.execute.Execute;

public class Test {

  @org.junit.Test
  public void getLong() {
    Connection connection = new Connection.ClientConnection("src/resource/gremlin-driver.yaml", false);
    Execute execute = new Execute(connection);
    Long count = execute.getLong("c.V().count()", null);
    println(count);
    connection.close();
  }

  @org.junit.Test
  public void getElementMap() {
    Connection connection = new Connection.ClientConnection("src/resource/gremlin-driver.yaml", false);
    Execute execute = new Execute(connection);
    Map<String, Object> vertex = execute.getElementMap("c.V().limit(1)", null);
    vertex.forEach((k, v) -> {
      println(k + "  ::  " + v);
    });
    Map<String, Object> edge = execute.getElementMap("c.E().limit(1)", null);
    edge.forEach((k, v) -> {
      println(k + "  ::  " + v);
    });
    connection.close();
  }

  @org.junit.Test
  public void getElementsMap() {
    Connection connection = new Connection.ClientConnection("src/resource/gremlin-driver.yaml", false);
    Execute execute = new Execute(connection);
    List<Map<String, Object>> vertex = execute.getElementsMap("c.V().limit(10)", null);
    vertex.forEach(e -> {
      e.forEach((k, v) -> {
        println(k + "  ::  " + v);
      });
    });
    List<Map<String, Object>> edge = execute.getElementsMap("c.E().limit(10)", null);
    edge.forEach(e -> {
      e.forEach((k, v) -> {
        println(k + "  ::  " + v);
      });
    });
    connection.close();
  }

  @org.junit.Test
  public void getGroupMap() {
    Connection connection = new Connection.ClientConnection("src/resource/gremlin-driver.yaml", false);
    Execute execute = new Execute(connection);
    String query = "g.V(1).bothE().as('r').otherV().hasId(2).select('r').group().by('f').by(group().by('t').by(groupCount().by(label)))";
    Map<String, Map<String, Map<String, Number>>> result = execute.getMap(query, null);
    if (null != result && 0 < result.size()) {
      result.forEach((f, v1) -> {
        v1.forEach((t, v2) -> {
          v2.forEach((label, count) -> {
            println(f + "-->" + t + "-->" + label + "-->" + count);
          });
        });
      });
    }
    connection.close();
  }

  private void println(Object k) {
    System.out.println(k);
  }
}
