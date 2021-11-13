package main;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import static java.lang.Math.min;

public class PeopleYouKnow {
	
	public static class PeopleYouKnowPairWritable implements Writable {
		private int _userId;
		private boolean	_hasCommonFriend;
		
		public PeopleYouKnowPairWritable() {
			this(-1, false);
		}
		
		public PeopleYouKnowPairWritable(int userId, boolean hasCommonFriend) {
			_userId = userId;
			_hasCommonFriend = hasCommonFriend;
		}
		
		public void write(DataOutput out) throws IOException {
	         out.writeInt(_userId);
	         out.writeBoolean(_hasCommonFriend);
	    }
	       
       public void readFields(DataInput in) throws IOException {
    	   _userId = in.readInt();
    	   _hasCommonFriend = in.readBoolean();
       }
       
       public int getUserId(){
    	   return _userId;
       }
       
       public boolean hasCommonFriend(){
    	   return _hasCommonFriend;
       }
	}
	
	public static class PeopleYouKnowMapper extends Mapper<LongWritable, Text, IntWritable, PeopleYouKnowPairWritable> {
	   
	 public void map(LongWritable key, Text value, Context context
	                 ) throws IOException, InterruptedException {
		 String line[] = value.toString().split("\t");
		 int userId = Integer.valueOf(line[0]);
		 List<Integer> friendsList = new ArrayList<Integer>();
		 
		 if (line.length == 1) {
			 context.write(new IntWritable(userId), new PeopleYouKnowPairWritable());
		 }
		 else {
			 String splittedFriends[] = line[1].split(",");
			 for (String friend : splittedFriends) {
				 friendsList.add(Integer.valueOf(friend)); 
			 }
			 
			 for (int i=0; i < friendsList.size(); i++) {
				 context.write(new IntWritable(userId), new PeopleYouKnowPairWritable(friendsList.get(i), false));
				 for(int j=i+1; j < friendsList.size();j++) {
					 context.write(new IntWritable(friendsList.get(i)), new PeopleYouKnowPairWritable(friendsList.get(j), true));
					 context.write(new IntWritable(friendsList.get(j)), new PeopleYouKnowPairWritable(friendsList.get(i), true));
				 }
			 }
		 }
	 }
	}
	
	public static class PeopleYouKnowReducer 
	    extends Reducer<IntWritable, PeopleYouKnowPairWritable, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterable<PeopleYouKnowPairWritable> values, 
	                    Context context
	                    ) throws IOException, InterruptedException {
			
			// The HashMap key is the recommended friend id and the value is the number of common friends;
			HashMap<Integer, Integer> recommendedFriendNbCommonFriendPair = new HashMap<Integer, Integer>();
			
			for (PeopleYouKnowPairWritable recommendedCommonFriendPair : values) {
				Integer recommendedFriend = recommendedCommonFriendPair.getUserId();
				Boolean hasCommonFriend = recommendedCommonFriendPair.hasCommonFriend();
				
				if (recommendedFriendNbCommonFriendPair.containsKey(recommendedFriend)) {
					 if(hasCommonFriend && recommendedFriendNbCommonFriendPair.get(recommendedFriend) != null) {
						// This Updates the number of common friends for a recommended friend
						 recommendedFriendNbCommonFriendPair.put(recommendedFriend, 
									recommendedFriendNbCommonFriendPair.get(recommendedFriend) + 1);
					 }
					 else {
						 recommendedFriendNbCommonFriendPair.put(recommendedFriend, null);
					 }
				}
				else {
					if (hasCommonFriend) {
						// This Initializes the key value pair of the hashmap if the recommended friend is not already there
						recommendedFriendNbCommonFriendPair.put(recommendedFriend, 1);
					}
					else {
						recommendedFriendNbCommonFriendPair.put(recommendedFriend, null);
					}
				}
			}
			
			List<Entry<Integer, Integer>> list = new LinkedList<Entry<Integer, Integer>>();
			for (Entry<Integer, Integer> mapEntry : recommendedFriendNbCommonFriendPair.entrySet()) {
				if (mapEntry.getValue() != null) {
					list.add(mapEntry);
				}
			}
			
			// Sort the HashMap through a list sort
			list.sort((entry1, entry2) -> entry1.getValue().compareTo(entry2.getValue()) == 0
					// if two values are equal we sort the recommended friend ID in ascending order
	                ? entry1.getKey().compareTo(entry2.getKey())
	                // else we sort the number of common friends in descending order
	                : entry2.getValue().compareTo(entry1.getValue())); 
			
			HashMap<Integer, Integer> sortedRecommendedFriendNbCommonFriendPair = list.stream()
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> b, LinkedHashMap::new));
			
			List<Entry<Integer, Integer>> entryList = List.copyOf(
					sortedRecommendedFriendNbCommonFriendPair.entrySet())
					.subList(0, min(sortedRecommendedFriendNbCommonFriendPair.size(),10));
			
			StringJoiner joiner = new StringJoiner(",");
			entryList.forEach((entry -> joiner.add(entry.getKey().toString())));
			
			context.write(key, new Text(joiner.toString()));
			 
		}
	}
	
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 if (otherArgs.length < 2) {
		   System.err.println("Usage: PeopleYouKnow <in> [<in>...] <out>");
		   System.exit(2);
		 }
		 Job job = Job.getInstance(conf, "PeopleYouMayKnow");
		 job.setJarByClass(PeopleYouKnow.class);
		 job.setMapperClass(PeopleYouKnowMapper.class);
		 job.setReducerClass(PeopleYouKnowReducer.class);
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(PeopleYouKnowPairWritable.class);
		 
		 for (int i = 0; i < otherArgs.length - 1; ++i) {
		   FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		 }
		 FileOutputFormat.setOutputPath(job,
		   new Path(otherArgs[otherArgs.length - 1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
