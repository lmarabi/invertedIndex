package org.gistic.invertedIndex;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

//import org.apache.lucene.analysis.standard.StandardAnalyzer;
public class KWIndexSearcher {
    
    

    public enum dataType {

        tweets, hashtags;
    }

    public KWIndexSearcher() {
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws UnsupportedEncodingException, IOException {
        String indexPath = "/export/scratch/louai/test/test2/index/";
        KWIndexSearcher searcher = new KWIndexSearcher();
        String query = "hello";
        searcher.search(indexPath, dataType.tweets, query, Integer.MAX_VALUE);
    }

    /**
     * This method search inside the inverted index by passing the following
     * Parameters
     *
     * @param index the path of the index
     * @param type [dataType.tweets,dataType.hashtags]
     * @param queryString the keywords
     * @param hits number of hits returned in the answer.
     * @return
     */
    public List<String> search(String index, dataType type, String queryString, int hits) {
        double starttime = System.currentTimeMillis();
        String field;
        if (type.equals(type.tweets)) {
            field = "tweetText";
        } else {
            field = "hashtagText";
        }
        List<String> outputResult = new ArrayList<String>();
        int repeat = 0;
        boolean raw = false;

        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(new File(index)));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
//        Analyzer analyzer = new ArabicAnalyzer(Version.LUCENE_46);

        int count = 0;
        QueryParser parser = new QueryParser(Version.LUCENE_46, field, analyzer);
        while (true) {
            try {
                String line = queryString;
                line = line.trim();
                if (line.length() == 0) {
                    break;
                }
                Query query = parser.parse(line);

                System.out.println("Searching for: " + query.toString(field));

                TopDocs results = searcher.search(query, hits);
                System.out.println("** " + results.totalHits);
                //PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter("SearchResultPointsArabicSimplified.csv", true)));
                for (int i = 0; i < results.scoreDocs.length; ++i) {
                    Document doc = searcher.doc(results.scoreDocs[i].doc);
                    /*                    System.out.println(doc.get("created_at") + " , " +
                     doc.get("tweetID") + " , " +
                     doc.get("userID") + " , " +
                     doc.get("screenName") + " , " +
                     doc.get("tweetText") + " , " +
                     doc.get("followersCount") + " , " +
                     doc.get("language") + " , " +
                     doc.get("osystem") + " , " +
                     doc.get("lat") + " , " +
                     doc.get("lon") 
                     );
                     */
                    //System.out.println(doc.get("tweet"));
                    if (type.equals(dataType.tweets)) {
                        outputResult.add(doc.get("tweet"));
                        System.out.println(doc.get("tweet"));
                    } else {
                        outputResult.add(doc.get("hashtag"));
                        System.out.println(doc.get("hashtag"));
                    }

                    count++;
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            break;
        }

        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        double endtime = System.currentTimeMillis();
        System.out.println("Execution time in milliseconds: " + (endtime - starttime));
        return outputResult;
    }

    

}
