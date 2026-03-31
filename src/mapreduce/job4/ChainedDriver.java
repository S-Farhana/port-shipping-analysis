import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class ChainedDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Region Aggregation Chained");
        job.setJarByClass(ChainedDriver.class);
        job.setMapperClass(RegionAggregatorMapper.class);
        job.setReducerClass(RegionAggregatorReducer.class);
        job.setPartitionerClass(RegionPartitioner.class);
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        ControlledJob cJob = new ControlledJob(job.getConfiguration());
        cJob.setJob(job);
        JobControl control = new JobControl("Port Chain");
        control.addJob(cJob);
        Thread t = new Thread(control);
        t.start();
        while (!control.allFinished()) Thread.sleep(1000);
        System.exit(control.getFailedJobList().isEmpty() ? 0 : 1);
    }
}