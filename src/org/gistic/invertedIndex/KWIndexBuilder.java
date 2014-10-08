package org.gistic.invertedIndex;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class KWIndexBuilder {

    public enum dataType {

        tweets, hashtags;
    }

    public static void main(String[] args) throws ParseException {
        String dataFolders = "/Users/louai/microblogsDataset/test/data/";
        String indexPath = "/Users/louai/microblogsDataset/test/index/";
        KWIndexBuilder indexBuilder = new KWIndexBuilder();
        String[] files = indexBuilder.getFilesPaths(dataFolders);
        indexBuilder.buildIndex(files, indexPath, true, dataType.tweets);
    }

    public KWIndexBuilder() {
    }

    /**
     * This Create Inverted keyword index of the given list of files to the
     * index Path
     *
     * @param dataFiles
     * @param indexPath
     * @return true if building index success otherwise return false
     */
    public boolean buildIndex(List<File> dataFiles, String indexPath, dataType type) {
        String[] dataFolders = new String[dataFiles.size()];
        int counter = 0;
        for (File f : dataFiles) {
            dataFolders[counter] = f.getAbsolutePath();
            counter++;
        }
        try {
            this.buildIndex(dataFolders, indexPath, true, type);
            return true;
        } catch (ParseException ex) {
            Logger.getLogger(KWIndexBuilder.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    
    public boolean buildIndex(String[] dataFolders, String indexPath, boolean create, dataType type) throws ParseException {
        boolean result = false;
        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(new File(indexPath));
            //Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
            Analyzer analyzer = new ArabicAnalyzer(Version.LUCENE_46);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_46, analyzer);
            
            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }

            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer.  But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            iwc.setRAMBufferSizeMB(2048.0);

            IndexWriter writer = new IndexWriter(dir, iwc);
            result = index(writer, dataFolders,type);

            // NOTE: if you want to maximize search performance,
            // you can optionally call forceMerge here.  This can be
            // a terribly costly operation, so generally it's only
            // worth it when your index is relatively static (ie
            // you're done adding documents to it):
            //
            // writer.forceMerge(1);
            writer.close();

            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");

        } catch (IOException e) {
            System.err.println(" caught a " + e.getClass()
                    + "\n with message: " + e.getMessage());
            return false;
        }
        return result;
    }

    private boolean index(IndexWriter writer, String[] dataFolders, dataType type) throws ParseException {
        BufferedReader filesReader;
        try {
            System.out.println("indexing :" + dataFolders.length + " files");
            //filesReader = new TwitterFileStream(filesPaths , true , i);

            for (int j = 0; j < dataFolders.length; j++) {
                filesReader = new BufferedReader(new FileReader(new File(dataFolders[j])));
                System.out.println(dataFolders[j]);
                String line = "";
                while ((line = filesReader.readLine()) != null) {
                    if (!line.equals("")) {
                        try {
                            addDoc(writer, line, type);
                        } catch (Exception e) {
                            System.err.println(e.getLocalizedMessage());
                        }
                    }
                }
                filesReader.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    

    private void addDoc(IndexWriter w, String line, dataType type) throws IOException, ParseException {
        Document doc = new Document();
        if (type.equals(dataType.tweets)) {
            Tweet tweet = new Tweet(line);
            doc.add(new Field("tweetText", tweet.tweetText, Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
//            doc.add(new TextField("tweetText", tweet.tweetText, Store.YES));
            doc.add(new StringField("tweet", tweet.toString(), Store.YES));
            doc.add(new StringField("crated_at", tweet.created_at, Store.YES));
            doc.add(new StringField("lat", tweet.lat, Store.YES));
            doc.add(new StringField("lon", tweet.lon, Store.YES));
        }else{
            Hashtag hashtag = new Hashtag(line);
            doc.add(new StringField("hashtag", hashtag.toString(), Store.YES));
            doc.add(new StringField("lat", hashtag.lat, Store.YES));
            doc.add(new StringField("lon", hashtag.lon, Store.YES));
            doc.add(new TextField("hashtagText", hashtag.hashtagText, Store.YES));
            
        }

        w.addDocument(doc);
    }

    private String[] getFilesPaths(String root) {
        ArrayList<String> validFiles = new ArrayList<String>();
        File folder = new File(root);
        String[] filesNames = folder.list();
        for (int i = 0; i < filesNames.length; ++i) {
            //if(filesNames[i].endsWith(".txt"))
            validFiles.add(root + filesNames[i]);
        }
        String[] newFilesNames = new String[validFiles.size()];
        return validFiles.toArray(newFilesNames);
    }
}
