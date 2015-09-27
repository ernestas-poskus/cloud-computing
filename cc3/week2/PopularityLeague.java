import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "Link Count");
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(InDegreeCountMap.class);
		job.setReducerClass(PopularityLeagueReduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(PopularityLeague.class);
		return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }


    public static class InDegreeCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
	
		String delimiters;
		List<String> leagueMembers;

		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			this.delimiters = new String(": ");
			Configuration conf = context.getConfiguration();
			String leagueMembersPath = conf.get("league");
			this.leagueMembers = Arrays.asList(readHDFSFile(leagueMembersPath, conf).split("\n"));
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, this.delimiters);
			if (tokenizer.hasMoreTokens()) {
				String fromPage = tokenizer.nextToken();
				while (tokenizer.hasMoreTokens()) {
					String toPage = tokenizer.nextToken();
					if (this.leagueMembers.contains(toPage)) {
						Integer toPageInt = Integer.parseInt(toPage);
						context.write(new IntWritable(toPageInt), new IntWritable(1));
					}
				}
			}
		}
	}

	public static class PopularityLeagueReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private TreeSet<Pair<Integer, Integer>> inDegreeCounts = new TreeSet<Pair<Integer, Integer>>();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			Integer sum = new Integer(0);
			for (IntWritable i: values) {
				sum += i.get();
			}

			inDegreeCounts.add(new Pair<Integer, Integer>(sum, key.get()));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Integer rank = new Integer(0);
			Integer itemCount = new Integer(0);
			Integer previousInDegree = new Integer(-1);

			for (Pair<Integer, Integer> item: inDegreeCounts) {
				Integer inDegree = item.first;
				Integer pageId = item.second;
				if (!inDegree.equals(previousInDegree)) {
					rank = itemCount;
				}
								
				context.write(new IntWritable(pageId), new IntWritable(rank));
				//context.write(new IntWritable(pageId), new IntWritable(inDegree));
			        ++itemCount;
				previousInDegree = inDegree;
			}
		}
	}
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
		B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {

	public final A first;
	public final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>,
			B extends Comparable<? super B>>
	Pair<A, B> of(A first, B second) {
		return new Pair<A, B>(first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		int cmp = o == null ? 1 : (this.first).compareTo(o.first);
		return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair<?, ?>) obj).first)
				&& equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
}
// <<< Don't Change

