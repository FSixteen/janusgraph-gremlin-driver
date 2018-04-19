# janusgraph-gremlin-driver

此包中对janusgraph-gremlin-driver作了简单包装, 仅供学习参考使用.

# demo:
* 查询总数
```Java
  @org.junit.Test
  public void getLong() {
    Connection connection = new Connection.ClientConnection("src/resource/gremlin-driver.yaml", false);
    Execute execute = new Execute(connection);
    Long count = execute.getLong("c.V().count()", null);
    println(count);
    connection.close();
  }
```
* 查询某个点或边
```Java
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
```
* 查询某些点或边
```Java
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
```
* 获取分组Map
```Java
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
```