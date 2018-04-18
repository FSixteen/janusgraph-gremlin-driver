package com.xyshzh.gremlin.test;

import com.xyshzh.gremlin.client.Connection;
import com.xyshzh.gremlin.execute.Execute;

public class Test {
  public static void main(String[] args) {
    Connection connection = new Connection.ClientConnection("src/resource/gremlin-driver.yaml", false);
    Execute execute = new Execute(connection);
    System.out.println(execute.getElements("c.V(256)", null));;
    connection.close();
  }
}
