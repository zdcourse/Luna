package word_kmeans;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

public class KMeans{
	public static int k = 3;

	public static final String dataPath2 = "/word_file";
	public static String CENTROID_FILE_NAME = "/centroid.txt";
	public static String OUTPUT_FILE_NAME="/part-00000";

	public static List<Cluster> clusters = new LinkedList<Cluster>();
	public static Set<WordInst> InstGrp = new HashSet();
	public static String JOB_NAME = "KMeans";

	public static class WordMap extends MapReduceBase implements
			Mapper<Object, Text, Text, Text> {

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                k = Integer.parseInt(job.get("COUNT"));
                if (cacheFiles != null && cacheFiles.length > 0) {
                    clusters.clear();
                    String line;
                    BufferedReader cacheReader1 = new BufferedReader(new FileReader(new File(cacheFiles[0].toString())));
                    BufferedReader cacheReader2 = null;

                    try {
                        //read the cluster file
                        while((line = cacheReader1.readLine()) != null) {
                            String[] temp = line.split(" ");
                            for (String t:temp)
                                clusters.add(new Cluster(new WordInst(t)));
                        }

                        if (InstGrp.size() == 0) {
                            cacheReader2 = new BufferedReader(new FileReader(new File("word"+ dataPath2)));
                            while((line = cacheReader2.readLine()) != null) {
                                String[] temp = line.split(" ");
                                for(String str:temp) {
                                    InstGrp.add(new WordInst(str));
                                }
                            }
                        }
                    } finally {
                        System.out.println("test");
                        cacheReader1.close();
                        cacheReader2.close();
                    }
                }

			} catch (Exception e) {
				System.err.println("Exception reading DistributedCache: " + e);
			}

		}

		@Override
		public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
		    for(WordInst inst: InstGrp) {
				int cp = 0;
				double dist = -1;
				for(int j = 0; j < k;j ++) {
					Cluster c = clusters.get(j);
                    for(int i=0;i<clusters.size(); i ++) {
                        WordInst ins = clusters.get(i).centr;
                        if(inst.word.equals(ins.word)) {
                            continue;
                        }
                    }
					double tdist = c.dist(inst);
					if(dist == -1) {
						dist = c.dist(inst);
						cp = j;
					} else if (tdist > dist) {
						cp = j;
						dist = tdist;
					}
				}
				String txt1 = clusters.get(cp).centr.word;
				String txt2 = inst.word;

				if (dist == 1) {
                    txt1 = txt2;
                }
				output.collect(new Text(txt1), new Text(txt2));
			}
		}
	}


	public static class WordReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {


		    String words = "";
		    String center = key.toString();


            int index;
            Cluster c = null;
            for(index=0;index < k; index ++) {
                c = clusters.get(index);
                if (center.equals(c.centr.word)) {
                    break;
                }
            }
            c.resetGroup();
		    while(values.hasNext()) {
		        String str = values.next().toString();
		        System.out.println(str);

                c.addInst(new WordInst(str));
		        if (words.equals("")) {
		            words = str;
		            continue;
                }
		        words = words + " " + str;
            }

            c.recalcCentr(true);
            String newCenter;
            newCenter = c.centr.word;
            output.collect(new Text(newCenter), new Text(words));
		}
	}

    public static class Cluster {
		public WordInst centr = null;
		public Set<WordInst> groups = null;
		public Cluster(WordInst initCentroid){
			this.centr = initCentroid;
			groups = new HashSet();
		}

		public double dist(WordInst inst) {
			return this.centr.distance(inst);
		}

		public void resetGroup(){
			groups.clear();
		}

		public boolean recalcCentr(boolean isReset) {
			WordInst uCentr = this.centr.centroid(groups);
			boolean rst = uCentr.isSamePoint(this.centr);
			if(uCentr.word.equals(null))
			    return true;
			this.centr = uCentr;
			if(isReset)
				groups.clear();
			return rst;
		}

		public void addInst(WordInst inst) {
			groups.add(inst);
		}
	}


	public static void run(String[] args) throws Exception{
	    String In = "word";
	    String Out = "output";

	    String input = In;
	    String output = Out + System.nanoTime();

	    int iteration = 0;

	    while(true) {
	        JobConf conf = new JobConf(KMeans.class);
            Path hdfsPath = new Path(input + CENTROID_FILE_NAME);
            DistributedCache.addCacheFile(hdfsPath.toUri(), conf);

            conf.setJobName(JOB_NAME);
	        conf.set("COUNT", "3");
            conf.setJarByClass(KMeans.class);
	        conf.setMapOutputKeyClass(Text.class);
	        conf.setMapOutputKeyClass(Text.class);

	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setMapperClass(WordMap.class);
	        conf.setReducerClass(WordReduce.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            conf.set("mapred.textoutputformat.separator", " ");

            FileInputFormat.setInputPaths(conf, new Path(input + CENTROID_FILE_NAME));

            Path output_file = new Path(output);
            FileOutputFormat.setOutputPath(conf, output_file);
            JobClient.runJob(conf);


            //write the center to the centroid file
            String centers=null;

            centers = Utils.readFromFile(output+OUTPUT_FILE_NAME, false);
            System.out.println("new centers:" + centers);
            String old_centers = Utils.readFromFile("word/centroid.txt", true);
            System.out.println("old centers:" + old_centers);
            if(centers.equals(old_centers)) {
                break;
            }
            Utils.writeToFile("word/centroid.txt", centers);

            ++ iteration;
            if(iteration > 10) {
                break;
            }
            output = Out + Long.toString(System.nanoTime());
        }
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }
}
