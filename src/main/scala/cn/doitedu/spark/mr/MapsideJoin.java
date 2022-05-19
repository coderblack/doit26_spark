package cn.doitedu.spark.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapsideJoin {

    public static class XMapper extends Mapper<LongWritable, Text,Text,NullWritable>{

        Map<Integer,String> mp = new HashMap<Integer,String>();

        /**
         * 从缓存文件中加载小表数据
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new FileReader("./names.txt"));
            String line = null;
            while((line=br.readLine())!=null){
                String[] split = line.split(",");
                mp.put(Integer.parseInt(split[0]),split[1]);
            }
        }

        /**
         * maptask对大表数据进行map映射的时候，关联上已经加载好的小表数据
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String name = mp.get(Integer.parseInt(split[0]));
            context.write(new Text(value.toString()+","+name), NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(XMapper.class);
        FileInputFormat.setInputPaths(job,new Path("hdfs://doit01:8020/大表数据所在路径/"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://doit01:8020/关联结果输出路径/"));
        // map端join中，不再需要reduce任务
        job.setNumReduceTasks(0);

        /**
         * 添加缓存文件
         * 该文件将随同job资源一起被提交到yarn指定的资源目录，并被各个task容器下载到自己的工作目录
         */
        job.addCacheFile(new URI("hdfs://doit01:8020/abc/names.txt"));

        job.waitForCompletion(true);
    }

}
