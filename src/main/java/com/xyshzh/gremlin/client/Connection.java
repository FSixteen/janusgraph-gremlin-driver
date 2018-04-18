package com.xyshzh.gremlin.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

/**
 * @author Shengjun Liu
 * @version 2018-01-30
 */
public interface Connection {
  Client getClient();

  void releaseClient(Client client);

  void close();

  public final static class ClientConnection implements Connection {

    private Cluster.Builder builder;
    private Integer clientMinSize = 10;
    private Integer clientMaxSize = clientMinSize * 4;
    private Integer clientInitBatchSize = clientMinSize;
    private Cluster cluster;
    private Vector<Client> clients = new Vector<Client>();

    public ClientConnection(String resourcePath, boolean autoCreate) {
      File file = null;
      try {
        file = new File(resourcePath);
        if (!file.exists() && autoCreate) {
          file.createNewFile();
          InputStream inputStream = null;
          OutputStream outputStream;
          inputStream = ClientConnection.class.getResourceAsStream("/gremlin-driver.yaml");
          outputStream = new FileOutputStream(file);
          int bytesRead = 0;
          byte[] buffer = new byte[8192];
          while ((bytesRead = inputStream.read(buffer, 0, 8192)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
          }
          outputStream.close();
          inputStream.close();
        }
        builder = Cluster.build(file);
        cluster = builder.create();
      } catch (NullPointerException | IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public Client getClient() {
      if (2 < clients.size()) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            getClients();
          }
        }).start();
        return clients.remove(0);
      } else {
        synchronized (this.getClass()) {
          if (1 < clients.size()) {
            return clients.remove(0);
          } else {
            getClients();
            return clients.remove(0);
          }
        }
      }
    }

    @Override
    public void releaseClient(Client client) {
      if (null == client || client.isClosing()) {
        return;
      } else if (clientMaxSize < clients.size() && !client.isClosing()) {
        client.close();
      } else {
        clients.addElement(client);
      }
    }

    private void getClients() {
      synchronized (this.getClass()) {
        if (clientMinSize > clients.size()) {
          for (int i = clientInitBatchSize; 0 < i; i--) {
            clients.addElement(cluster.connect());
          }
        }
      }
    }

    @Override
    public void close() {
      clients.forEach(_1 -> _1.close());
      cluster.close();
    }

  }
}
