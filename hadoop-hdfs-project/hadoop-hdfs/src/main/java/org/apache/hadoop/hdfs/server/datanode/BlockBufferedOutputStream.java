package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

class BlockBufferedOutputStream extends OutputStream{
  public final ByteBuffer buf;
  
  BlockBufferedOutputStream(ByteBuffer _buf){
    buf = _buf;
  }

  @Override
  public void write(int b) throws IOException {
    if (buf.remaining() == 0)
      throw new IOException("buffer is full");
      buf.put((byte) (b & 0xff));
  }
  
  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    if(buf.remaining()<len)
      throw new IOException("buffer is full");
    buf.put(b,off,len);
  }

  public int getCount() {
    return buf.position();
  }
  
}
