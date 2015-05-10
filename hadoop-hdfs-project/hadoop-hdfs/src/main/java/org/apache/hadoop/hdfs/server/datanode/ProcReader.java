package org.apache.hadoop.hdfs.server.datanode;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;

public class ProcReader {
  private final static String DISK_FILE = "/proc/diskstats";
  private String disk = "sdc";
  private int wait;
  private Stats prevStats;
  private Stats currStats;
  private RandomAccessFile rFile;

  public ProcReader() throws FileNotFoundException{
    prevStats = null;
    currStats = null;
    rFile = new RandomAccessFile(DISK_FILE,"r");
  }

  public void updateInfo() throws IOException{
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
        if(prevStats ==null){
          wait = 0;
        }else{
          wait = Stats.getWait(prevStats,currStats);
        }           
        break;
      }

    }
  }

  public int getWait() throws IOException{
    updateInfo();
    return wait;
  }
  
  public void close()
  {
    try{
    rFile.close();
    }catch(IOException e){
      System.out.println("Failed to close file in ProcReader");
    }
  }


  public static void main(String [] args){
    try{
    ProcReader rd = new ProcReader();
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



