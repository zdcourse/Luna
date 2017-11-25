package word_kmeans;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Utils {

    public Map<TwoTuple<String, String>, Double> word_vec = new HashMap<TwoTuple<String, String>, Double>();

    public Utils(){
        File file = new File("word/word_vec_file");
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null){
                String[] s = line.split(" ");
                TwoTuple<String, String> temp = new TwoTuple<String, String>(s[0], s[1]);
                word_vec.put(temp, Double.parseDouble(s[2]));
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public double getDistance(WordInst a, WordInst b) {
        String word_a;
        String word_b;

        word_a = a.word;
        word_b = b.word;
        if(word_a == word_b) {
            return 1;
        }

        TwoTuple<String, String> temp = new TwoTuple<String, String>(word_a, word_b);
        try {
            Double dis = word_vec.get(temp);

            if (dis == null) {
                temp = new TwoTuple<String, String>(word_b, word_a);
                dis = word_vec.get(temp);
            }
            return dis;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return 1;
    }

    public static void writeToFile(String filename, String content) throws Exception{
        BufferedWriter bw = null;
        try {
            File file = new File(filename);
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file, false);
            bw = new BufferedWriter(fw);
            bw.write(content);
            System.out.println("File written Successfully");

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            bw.close();
        }
    }

    public static String readFromFile(String filename, boolean byline) throws Exception{
        BufferedReader OutputReader = new BufferedReader(new FileReader(new File(filename)));
        String centers=null;
        try {
            String line;

            while((line = OutputReader.readLine()) != null) {

                if(byline){
                    return line;
                }
                String[] temp = line.split(" ");

                if (centers == null) {
                    centers = temp[0];
                    continue;
                } else {
                    centers = centers + " " + temp[0];
                }
            }
        } catch (Exception e) {

        } finally {
            OutputReader.close();
        }
        return centers;
    }
}
