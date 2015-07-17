package org.apache.hadoop.hdfs.server.datanode;

import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class JobWFQ extends WeightedFairQueue {
Map<String, LeafWFQ> queues;
  LinkedList<BTime> times = new LinkedList<BTime>();
  
  JobWFQ(){
    queues = new ConcurrentHashMap<String, LeafWFQ>();
  }
  
  @Override
  void addReceive(PendingReceive receive) {
    times.addLast(new BTime(receive.pStart,receive.pEnd));
    LeafWFQ curr = queues.get(receive.flow);
    if(curr == null){
      curr = new LeafWFQ();
      queues.put(receive.flow, curr);
    }
      
    
    long startTime = this.getVirtualTime();
    if(!curr.isEmpty()){
      startTime = Math.max(startTime, curr.getFinishTime());
    }
    
   receive.jStart = startTime;
   receive.jEnd = startTime + (long)(receive.blockSize/receive.flowPriority);
   curr.addReceive(receive);
  }

  @Override
  PendingReceive getReceive() {
    String bestFlow = "";
    long bestTime = Long.MAX_VALUE;
    long temp;
    LeafWFQ bestQueue;

    for(Entry<String, LeafWFQ> entry : queues.entrySet()){
      if(!entry.getValue().isEmpty()){
        temp = entry.getValue().getStartTime();
        if(temp < bestTime){
          bestTime = temp;
          bestFlow = entry.getKey();
        }
      }
    }

    if(bestFlow.length() < 1){
      return null;
    }else{
      bestQueue = queues.get(bestFlow);
    }

    PendingReceive result = bestQueue.getReceive();
    result.setTimeStamp(bestTime);
    
    if(bestQueue.isEmpty()){
       queues.remove(bestFlow);
    }
    times.pollFirst();
    return result;
   
  }

  @Override
  boolean isEmpty() {
    for(Entry<String, LeafWFQ> entry : queues.entrySet()){
      if(!entry.getValue().isEmpty()){
        return false;
      }
    }
    return true;
  }

  @Override
  long getVirtualTime() {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long temp;
    for(Entry<String, LeafWFQ> entry : queues.entrySet()){
      //if queue for the priority level is not empty
      if(!entry.getValue().isEmpty()){
          temp = entry.getValue().getStartTime();
          if(temp > max){
            max = temp;
          }
          if(temp < min){
            min = temp;
          }
      }
    }
    return ((min+max)/2);
  }

  @Override
  PendingReceive remove(String blockID, String flow, String position) {
    LeafWFQ curr = queues.get(flow);
    PendingReceive removed = null;
    if(curr != null){
      removed = curr.remove(blockID,flow,position);
      if(removed !=null){
        if(curr.isEmpty()){
          queues.remove(flow);
        }  
      }
    }
    times.pollFirst();
    return removed;
  }

  @Override
  int getSize() {
    int size = 0;
    for(Entry<String, LeafWFQ> entry : queues.entrySet()){
      size += entry.getValue().getSize();
    }
    return size;
  }
  
  
  public long getStartTime(){
    if(!times.isEmpty()){
      return times.getFirst().start;
    }
    return 0;
  }
  
  public long getFinishTime(){
    if(!times.isEmpty()){
      return times.getLast().end;
    }
    return 0;
  }
}
