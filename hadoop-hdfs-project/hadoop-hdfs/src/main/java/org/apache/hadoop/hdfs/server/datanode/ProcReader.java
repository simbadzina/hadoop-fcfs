package org.apache.hadoop.hdfs.server.datanode;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;

public class ProcReader {
  private final static String DISK_FILE = "/proc/diskstats";
  private final static String MEM_FILE = "/proc/meminfo";
  private final static String DIRTY = "Dirty";
  private String disk = "sdc";
  private int wait;
  private Stats prevStats;
  private Stats currStats;
  private RandomAccessFile rFile;
  private RandomAccessFile mFile;
  private DStats prevDStats;
  private DStats currDStats;
  private DStats initDStats;
  private MStats currMemStats;
  private MStats prevMemStats;
  private long prevTime;
  private long currTime;

  public ProcReader(String storageDevice) throws FileNotFoundException,IOException{
    prevStats = null;
    currStats = null;
    rFile = new RandomAccessFile(DISK_FILE,"r");
    mFile = new RandomAccessFile(MEM_FILE,"r");
    prevDStats = null;
    currDStats = null;
    
    currMemStats = null;
    prevMemStats = null;
    
    prevTime = 0;
    currTime = 1000;
    disk = storageDevice;
    updateInfo();
    updateInfo();
    initDStats = currDStats;
  }

  public void updateInfo() throws IOException{
    currTime = System.currentTimeMillis();
    if(currTime-prevTime < 100){
     return; 
    }
    prevTime = currTime;
    //BufferedReader br = new BufferedReader(new FileReader(DISK_FILE));
    String line;
    rFile.seek(0);
    while((line = rFile.readLine()) != null){
      if(line.indexOf(disk) > 0){
        //the line with the stats for this disk
        line = line.substring(line.indexOf(disk));
        String[] numbers = line.split("\\s+");
        prevStats = currStats;        
        currStats = new Stats(Long.valueOf(numbers[4]).longValue(),Long.valueOf(numbers[8]).longValue(),
            Long.valueOf(numbers[1]).longValue(),Long.valueOf(numbers[5]).longValue());
        
        prevDStats = currDStats;        
        currDStats = new DStats(Long.valueOf(numbers[3]).longValue(),Long.valueOf(numbers[7]).longValue());
        if(prevStats ==null){
          wait = 0;
        }else{
          wait = Stats.getWait(prevStats,currStats);
        }           
        break;
      }

    }
    
    mFile.seek(0);
    while ((line = mFile.readLine()) != null) {
      if (line.startsWith(DIRTY)) {
        prevMemStats = currMemStats;
        currMemStats = new MStats(Long.parseLong(line.replaceAll("[\\D]", ""))*1024);
        break;
      }
    }
  }

  public int getWait(){
    return (wait>0)?wait:0;
  }
  
  public void close()
  {
    try{
    rFile.close();
    mFile.close();
    }catch(IOException e){
      System.out.println("Failed to close file in ProcReader");
    }
  }
  
  public int getReadThroughput(){
    return DStats.getReadThroughput(prevDStats,currDStats);
  }
  
  public int getWriteThroughput(){
    return DStats.getWriteThroughput(prevDStats,currDStats);
  }
  
  
  public long getReadTotal(){
    return DStats.getTotalRead(initDStats,currDStats);
  }
  
  public long getWriteTotal(){
    return DStats.getTotalWrite(initDStats,currDStats);
  }
  
  public int getMemThroughput(){
    return MStats.getMemThroughput(prevMemStats, currMemStats);
  }
  


  public static void main(String [] args){
    try{
    ProcReader rd = new ProcReader("sdc");
      while(true){
        System.out.println(rd.getWait());
        Thread.sleep(1000);
      }
    }catch(IOException e){
      System.out.println(e);
    }catch(Exception e){
      System.out.println(e);
    }   
  }


}


class Stats{

  public Stats( long readT, long writeT, long  readO, long writeO){
    readOps = readO;
    writeOps = writeO;
    readTime = readT;
    writeTime = writeT;
  }
  public long readOps;
  public long writeOps;
  public long readTime;
  public long writeTime;

  public static int getWait(Stats prev, Stats curr){
    long numOps = (curr.readOps + curr.writeOps) - (prev.readOps + prev.writeOps);
    long totTime = (curr.readTime + curr.writeTime) - (prev.readTime + prev.writeTime);
    return (int)((numOps!=0)?(totTime/numOps):0);
  }

}

class DStats{

  public DStats( long secsRead, long secsWritten){
    bytesRead = secsRead*512;
    bytesWritten = secsWritten * 512;
    time = System.currentTimeMillis();
  }
  public long bytesRead;
  public long bytesWritten;
  public long time;


  public static int getReadThroughput(DStats prev, DStats curr){
    long bytesDiff = curr.bytesRead  - prev.bytesRead;
    long timeDiff = curr.time - prev.time;
    return (int)((bytesDiff/timeDiff)*1000);
  }
  
  public static int getWriteThroughput(DStats prev, DStats curr){
    long bytesDiff = curr.bytesWritten  - prev.bytesWritten;
    long timeDiff = curr.time - prev.time;
    return (int)((bytesDiff/timeDiff)*1000);
  }
  
  
  public static long getTotalWrite(DStats prev, DStats curr){
    return  curr.bytesWritten  - prev.bytesWritten;
  }
  
  public static long getTotalRead(DStats prev, DStats curr){
    return  curr.bytesRead  - prev.bytesRead;
  }

}


class MStats{
  
  MStats(long mUsed){
    memUsed = mUsed;
    time = System.currentTimeMillis();
  }
  public long time;
  public long memUsed;
  
  
  public static int getMemThroughput(MStats prev, MStats curr){
    long bytesDiff = curr.memUsed - prev.memUsed;
    long timeDiff = curr.time - prev.time;
    return (int)((bytesDiff/timeDiff)*1000);
  }
}

