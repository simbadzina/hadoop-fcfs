package org.apache.hadoop.hdfs.server.datanode;


import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.io.RandomAccessFile;

import org.mortbay.log.Log;

/**
 * The class implements a buffered output stream. By setting up such
 * an output stream, an application can write bytes to the underlying
 * output stream without necessarily causing a call to the underlying
 * system for each byte written.
 *
 * @author  Arthur van Hoff
 * @since   JDK1.0
 */
public class BlockBufferedOutputStream extends FilterOutputStream{
  /**
   * The internal buffer where data is stored.
   */  
  MappedByteBuffer buf;
  FileChannel channel;
  RandomAccessFile outFile;
  /**
   * Creates a new buffered output stream to write data to the
   * specified underlying output stream.
   *
   * @param   out   the underlying output stream.
   */
  public BlockBufferedOutputStream(OutputStream out) throws IOException{
    this(out, 8192,null);
  }
  private int count = 0;
  /**
   * Creates a new buffered output stream to write data to the
   * specified underlying output stream with the specified buffer
   * size.
   *
   * @param   out    the underlying output stream.
   * @param   size   the buffer size.
   * @exception IllegalArgumentException if size &lt;= 0.
   */
  public BlockBufferedOutputStream(OutputStream out, int size, RandomAccessFile _outFile) throws IOException{
    super(out);
    if (size <= 0) {
      throw new IllegalArgumentException("Buffer size <= 0");
    }
    outFile = _outFile;
    channel = outFile.getChannel();  
    buf = channel.map(MapMode.READ_WRITE, 0, size);
    buf.order(ByteOrder.nativeOrder());

  }




  /** Flush the internal buffer */
  private void flushBuffer() throws IOException {
    buf.force();
  }

  
  public void close() throws IOException {
    try {
      flush();
    } catch (IOException ignored) {
    }
  }


  public int getCount(){
    return count;
  }

  public int getLength(){
    return buf.capacity();
  }


  /**
   * Writes the specified byte to this buffered output stream.
   *
   * @param      b   the byte to be written.
   * @exception  IOException  if an I/O error occurs.
   */
  public synchronized void write(int b) throws IOException {
    buf.put((byte)b);
    count++;

  }


  /**
   * Writes <code>len</code> bytes from the specified byte array
   * starting at offset <code>off</code> to this buffered output stream.
   *
   * <p> Ordinarily this method stores bytes from the given array into this
   * stream's buffer, flushing the buffer to the underlying output stream as
   * needed.  If the requested length is at least as large as this stream's
   * buffer, however, then this method will flush the buffer and write the
   * bytes directly to the underlying output stream.  Thus redundant
   * <code>BufferedOutputStream</code>s will not copy data unnecessarily.
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  public synchronized void write(byte b[], int off, int len) throws IOException {
    buf.put(b, off, len);
    count += len;
  }

  /**
   * Flushes this buffered output stream. This forces any buffered
   * output bytes to be written out to the underlying output stream.
   *
   * @exception  IOException  if an I/O error occurs.
   * @see        java.io.FilterOutputStream#out
   */
  public synchronized void flush() throws IOException {
    Log.info("BFLUSH,"+count);
    flushBuffer();
    out.flush();
  }

  public ByteBuffer getBuf(){
    return buf;
  }

  public void rewind(){
    buf.rewind();
  }



}