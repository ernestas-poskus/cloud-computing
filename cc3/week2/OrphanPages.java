import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
	
        Job job = Job.getInstance(conf, "Orphan Pages");
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(NullWritable.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setMapperClass(LinkCountMap.class);
	job.setReducerClass(OrphanPageReduce.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setJarByClass(OrphanPages.class);
	return job.waitForCompletion(true) ? 0 : 1;
    }
	
    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
	
	String delimiters;
        Map<Integer, Integer> parentCount;

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
	    this.delimiters = new String(": ");
	    this.parentCount = new HashMap<Integer, Integer>();
	}

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line, this.delimiters);
	   
	    if (tokenizer.hasMoreTokens()) {
		Integer fromPage = Integer.parseInt(tokenizer.nextToken());
		if (!parentCount.containsKey(fromPage)) {
		    parentCount.put(fromPage, new Integer(0));
		}

		while(tokenizer.hasMoreTokens()) {
		    Integer toPage = Integer.parseInt(tokenizer.nextToken());
		    if(!parentCount.containsKey(toPage)) {
			parentCount.put(toPage, new Integer(1));
		    }
		    else {
			Integer count = parentCount.remove(toPage);
			++count;
			parentCount.put(toPage, count);
		    }
		}
	    }
        }

	@Override 
	protected void cleanup(Context context) throws IOException,InterruptedException {
	    for(Map.Entry<Integer, Integer> item: parentCount.entrySet()) {
		Integer page = item.getKey();
		Integer count = item.getValue();
		context.write(new IntWritable(page), new IntWritable(count));
	    }
	}
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    Integer sum = new Integer(0);
	    for (IntWritable val: values) {
		sum += val.get();
	    }

	    if (sum == 0) {
		context.write(key, NullWritable.get());
	    }
        }
    }
}
