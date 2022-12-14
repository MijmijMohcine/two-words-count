package org.hadoop.wordcount;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WCountMap extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);

    // Map function
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // converte value to type string and lower case letters
        String line = value.toString();
        line = line.toLowerCase();

        // the words that does not have any meaningful information
        String stop_words = "than just more he their am there out her were into too him you 'd an it's your if other can mightn until you'll hadn't no under had haven won very isn't d the above shouldn for will down needn't being won't again ain hadn wasn't has mustn shan't nor now from ours this and m doesn't weren yourself they when between with below about our at she up t where further couldn by wouldn wouldn't you're own in couldn't only you hasn been have mustn't does during what didn't hers isn once hasn't doing having to on we such haven't shouldn't should theirs of why off whom over but before any so myself did she's ma some ll his re yourselves a o you've mightn't is each was i which ourselves do it all themselves don should've its s herself didn not needn that'll or who are himself itself shan then me because these weren't while how yours doesn few both them don't aftermy aren't same that most y be through aren wasn ve against here those as";
        if (line.charAt(0) == '5') { // 5 stars reviews
            line = line.split("\t")[2];
            String[] sentences = line.split("[-,!.?()\"]"); // split the review into sentences
            List<String> wordsList = new ArrayList<String>();
            for (String sentence : sentences) {
                List<String> l = Arrays.asList(sentence.split(" ")); // split each sentence into words
                List<String> words = new ArrayList<>();
                for (String word : l)
                    if (!stop_words.contains(word.trim()) && word.trim() != "" && word.trim() != " ")
                        words.add(word); // remove stop words
                //add sequences of two adjacent words to wordslist
                if (words.size() > 0) {
                    if (words.size() == 1) {
                        System.out.println("block if");
                        wordsList.add(words.get(0).trim());
                    } else {
                        System.out.println("block else");
                        for (int i = 0; i < words.size() - 1; i++) {
                            wordsList.add(words.get(i).trim() + " " + words.get(i + 1).trim());
                        }
                    }
                }
            }
            for (String a : wordsList) {
                Text word = new Text(a);
                context.write(word, ONE); // for each word in wordList we return the pair (key; value) made up of our word and 1, in Text format.
            }
        }

    }
}
