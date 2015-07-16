package org.apache.hadoop.hdfs.server.datanode;

public class BTime {
  public long start;
  public long end;
  
  public BTime(long _start,long _end){
    start=_start;
    end = _end;
  }
}
