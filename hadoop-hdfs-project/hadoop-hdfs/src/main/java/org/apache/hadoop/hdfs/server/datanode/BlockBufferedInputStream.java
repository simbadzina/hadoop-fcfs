package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;

public class BlockBufferedInputStream extends FilterInputStream {
 BufferedInputStream bf ;
 
  protected BlockBufferedInputStream(InputStream in) {
    super(in);
    // TODO Auto-generated constructor stub
  }

}
