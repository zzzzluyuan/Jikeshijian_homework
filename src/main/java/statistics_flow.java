import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class statistics_flow   {
    public static void main(String[] args) throws Exception{
        Configuration Configuration=new Configuration();
        Job job=Job.getInstance(Configuration);

        job.setJarByClass(statistics_flow.class);

        job.setMapperClass(flowmapper.class);
        job.setReducerClass(flowreducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(flowbean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowbean.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result=job.waitForCompletion(true);
        System.exit(result?0:1);

    }

}
class flowmapper extends Mapper<LongWritable, Text, Text, flowbean>{
    Text k = new Text();
    flowbean v = new flowbean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取每行的数据
        String line = value.toString();
        String[] split = line.split("\t");
        String telehpone = split[1];
        long upfllow = Long.parseLong(split[7]);
        long downfllow = Long.parseLong(split[8]);
        context.write(new Text(telehpone),new flowbean(upfllow,downfllow));
    }
}
class flowreducer extends Reducer<Text,flowbean,Text,flowbean>{
    @Override
    protected void reduce(Text key, Iterable<flowbean> values, Context context) throws IOException, InterruptedException {
        //累加器
        long sumupflow=0;
        long sumdownflow=0;
        //遍历value
        for(flowbean flow:values){
            //进行累加
            sumupflow +=flow.getUpflow();
            sumdownflow +=flow.getDownflow();
        }
       //封装对象
       flowbean result=new flowbean(sumupflow,sumdownflow);
        context.write(key,result);
    }
}
