/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.mapreduce.task.MapContextImpl;


import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//FLORIN
import java.security.MessageDigest;
import org.apache.commons.codec.binary.Hex;
/** 
 * Maps input key/value pairs to a set of intermediate key/value pairs.  
 * 
 * <p>Maps are the individual tasks which transform input records into a 
 * intermediate records. The transformed intermediate records need not be of 
 * the same type as the input records. A given input pair may map to zero or 
 * many output pairs.</p> 
 * 
 * <p>The Hadoop Map-Reduce framework spawns one map task for each 
 * {@link InputSplit} generated by the {@link InputFormat} for the job.
 * <code>Mapper</code> implementations can access the {@link Configuration} for 
 * the job via the {@link JobContext#getConfiguration()}.
 * 
 * <p>The framework first calls 
 * {@link #setup(org.apache.hadoop.mapreduce.Mapper.Context)}, followed by
 * {@link #map(Object, Object, Context)} 
 * for each key/value pair in the <code>InputSplit</code>. Finally 
 * {@link #cleanup(Context)} is called.</p>
 * 
 * <p>All intermediate values associated with a given output key are 
 * subsequently grouped by the framework, and passed to a {@link Reducer} to  
 * determine the final output. Users can control the sorting and grouping by 
 * specifying two key {@link RawComparator} classes.</p>
 *
 * <p>The <code>Mapper</code> outputs are partitioned per 
 * <code>Reducer</code>. Users can control which keys (and hence records) go to 
 * which <code>Reducer</code> by implementing a custom {@link Partitioner}.
 * 
 * <p>Users can optionally specify a <code>combiner</code>, via 
 * {@link Job#setCombinerClass(Class)}, to perform local aggregation of the 
 * intermediate outputs, which helps to cut down the amount of data transferred 
 * from the <code>Mapper</code> to the <code>Reducer</code>.
 * 
 * <p>Applications can specify if and how the intermediate
 * outputs are to be compressed and which {@link CompressionCodec}s are to be
 * used via the <code>Configuration</code>.</p>
 *  
 * <p>If the job has zero
 * reduces then the output of the <code>Mapper</code> is directly written
 * to the {@link OutputFormat} without sorting by keys.</p>
 * 
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class TokenCounterMapper 
 *     extends Mapper&lt;Object, Text, Text, IntWritable&gt;{
 *    
 *   private final static IntWritable one = new IntWritable(1);
 *   private Text word = new Text();
 *   
 *   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 *     StringTokenizer itr = new StringTokenizer(value.toString());
 *     while (itr.hasMoreTokens()) {
 *       word.set(itr.nextToken());
 *       context.write(word, one);
 *     }
 *   }
 * }
 * </pre></blockquote></p>
 *
 * <p>Applications may override the {@link #run(Context)} method to exert 
 * greater control on map processing e.g. multi-threaded <code>Mapper</code>s 
 * etc.</p>
 * 
 * @see InputFormat
 * @see JobContext
 * @see Partitioner  
 * @see Reducer
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FlorinMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>{

  public static final Log LOG =
    LogFactory.getLog(FlorinMapper.class);

    public int seed_for_rnd=-1;	
    public Random random = null;
    public long sumAll=0; //fast correctness check 
    public byte [] sumHashes=new byte [16];
    public int nr_records=0; 
    public long start_tstamp=0; 
    public String task_id=null;
	
    protected void setup(Context context) throws IOException, InterruptedException {
		LOG.info("Start of mapper setup function");
 		task_id=context.getConfiguration().get("mapper.id.param");
		//seed_for_rnd= Integer.valueOf((task_id.split("_"))[4]);
		//LOG.info("Got seed: "+seed_for_rnd);		
		//random = new Random(seed_for_rnd);
		 //random = new Random(239473258);
//		 for(int i=0;i<16;i++)
//			 sumHashes[i]=0;
		start_tstamp=System.currentTimeMillis();
    }


    private void randomizeBytes(byte[] data, int offset, int length) {
      int sum=0;
      //for(int i=offset + length - 1; i >= offset; --i) {
      for(int i= offset; i <= offset + length -1 ; i++) {
	sum+=(int)data[i];
       	Random random = new Random(sum);
        data[i] = (byte) random.nextInt(255);
      }
    }

    private void computeSum(byte[] data, int offset, int length) {
      //for(int i=offset + length - 1; i >= offset; --i) {
      for(int i= offset; i <= offset + length -1 ; i++) {
	sumAll+=Math.abs((int)data[i]);
      }
    }


    private void computeHash(byte[] data, int offset, int length) {
	
	try{
	MessageDigest m = MessageDigest.getInstance("MD5");

        byte [] bytes_trunc = new byte [length];
        for(int i=0; i<length;i++){
  	 	bytes_trunc[i]=data[i];	      
        }

	m.reset();
	m.update(bytes_trunc);
	byte[] digest = m.digest();
	//System.out.print("Bytes hash: ");
	for (int i=0;i<digest.length;i++){
		sumHashes[i]+=digest[i];
//		System.out.print(digest[i]+".");
	}
//	System.out.println();

//	String hashtext = Hex.encodeHexString(digest);
	      
//	LOG.info("dig.len"+digest.length+"Hash of value at mapper is: "+hashtext);
	}catch(Exception e){
    		System.out.println("Exception computing MD5 in FlorinMapper");
	}

    }
  /**
   * Called once for each key/value pair in the input split. Most applications
   * should override this, but the default is the identity function.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void map(KEYIN key, VALUEIN value, 
                     Context context) throws IOException, InterruptedException {
			     
    if(key instanceof org.apache.hadoop.io.BytesWritable){
	randomizeBytes(((org.apache.hadoop.io.BytesWritable)key).getBytes(),0,((org.apache.hadoop.io.BytesWritable)key).getLength());
	computeSum(((org.apache.hadoop.io.BytesWritable)value).getBytes(),0,((org.apache.hadoop.io.BytesWritable)value).getLength());
	computeHash(((org.apache.hadoop.io.BytesWritable)value).getBytes(),0,((org.apache.hadoop.io.BytesWritable)value).getLength());
	nr_records++;
    }else
    	System.out.println("Not bytes writable");

    context.write((KEYOUT) key, (VALUEOUT) value);
  }

  
  protected void cleanup(Context context) throws IOException, InterruptedException {
	
	System.out.println("Processing: "+nr_records+" records took: "+(System.currentTimeMillis()-start_tstamp));		
	LOG.info("Processing: "+nr_records+" records took: "+(System.currentTimeMillis()-start_tstamp));		

	System.out.println("Fast hash check: "+sumAll+ " for task: "+task_id);
	System.out.print("Sum hash check: ");
	for (int i=0;i<sumHashes.length;i++){
		System.out.print(sumHashes[i]+":");
	}
	System.out.println(" for task: "+task_id);

  }
  


}
