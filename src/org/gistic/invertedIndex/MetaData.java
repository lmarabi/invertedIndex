/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.invertedIndex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

/**
 *
 * @author louai
 */
public class MetaData {

	private String day;
	private List<KeyWord> keywords;
	final boolean ASC = true;
	final boolean DESC = false;

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public List<KeyWord> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<KeyWord> keywords) {
		this.keywords = keywords;
	}

	public void readMetaData(String path) throws FileNotFoundException,
			UnsupportedEncodingException, IOException {
		BufferedReader br;
		br = new BufferedReader(new InputStreamReader(new FileInputStream(path
				+ "/_inverted_metadata"), "UTF-8"));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] temp = line.split(",");
			this.day = temp[0];
			for (int i = 1; i < temp.length; i++) {
				String[] ky = temp[i].split("-");
				this.keywords.add(new KeyWord(ky[0], Integer.parseInt(ky[i])));
			}
		}
	}

	/**
	 * This method create the meta data for the inverted index
	 * 
	 * @param indexDir
	 *            the directory where the inverted index exist
	 * @param path
	 *            the folder path where the meta-data will be saved
	 * @param day
	 *            the day of the current inverted index
	 * @return
	 * @throws IOException
	 */
	public boolean buildMetaData(String indexDir, String path, String day,
			int threshold) throws IOException {
		File file = new File(new File(path) + "/_inverted_metadata");
		if (!file.exists()) {
			file.createNewFile();
		}
		Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(file, true), "UTF8"));

		double startTime = System.currentTimeMillis();
		String field;
		field = "tweetText";

		IndexReader reader = null;
		try {
			reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		// long threshold = (long) (reader.maxDoc() * 0.01);
		System.out.println("Threshold = " + threshold);

		HashMap<String, Integer> keywords = new HashMap<String, Integer>();
		Fields fields = MultiFields.getFields(reader);
		Terms terms = fields.terms(field);
		try {
			TermsEnum iterator = terms.iterator(TermsEnum.EMPTY);
			BytesRef byteRef = null;
			while ((byteRef = iterator.next()) != null) {
				String term = new String(byteRef.bytes, byteRef.offset,
						byteRef.length);
				keywords.put(term, iterator.docFreq());
			}

			Map<String, Integer> sortedList = sortByComparator(keywords, DESC);
			System.out.println(sortedList.size());
			keywords = (HashMap<String, Integer>) sortedList;
			writer.append(day);
			Iterator it = keywords.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry obj = (Map.Entry) it.next();
				// System.out.println("k: "+obj.getKey()+" v: "+obj.getValue());
				writer.append(",["
						+ (String) obj.getKey().toString().replace(',', ' ')
						+ "-" + obj.getValue() + "]");
				if (Integer.parseInt(obj.getValue().toString()) < threshold) {
					break;
				}
			}
			double endTime = System.currentTimeMillis();
			System.out.println("execution Time in MilliSeconds: "
					+ (endTime - startTime));
			writer.append("\n");
			writer.flush();
			writer.close();
			return true;
		} catch (Exception e2) {
			e2.printStackTrace();
			return false;
		}
	}

	/**
	 * This method return all the keywords in inverted index.
	 * 
	 * @param indexpath
	 * @return
	 * @throws IOException
	 */
	public List<String> getAllKeywordOfIndex(String indexpath)
			throws IOException {
		List<String> temp = new ArrayList<String>();
		String field;
		field = "tweetText";
		IndexReader reader = null;
		try {
			reader = DirectoryReader
					.open(FSDirectory.open(new File(indexpath)));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		HashMap<String, Integer> keywords = new HashMap<String, Integer>();
		Fields fields = MultiFields.getFields(reader);
		Terms terms = fields.terms(field);
		TermsEnum iterator = terms.iterator(TermsEnum.EMPTY);
		BytesRef byteRef = null;
		while ((byteRef = iterator.next()) != null) {
			String term = new String(byteRef.bytes, byteRef.offset,
					byteRef.length);
			keywords.put(term, iterator.docFreq());
		}

		Map<String, Integer> sortedList = sortByComparator(keywords, ASC);
		System.out.println(sortedList.size());
		keywords = (HashMap<String, Integer>) sortedList;
		Iterator it = keywords.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry obj = (Map.Entry) it.next();
			temp.add(obj.getKey().toString().replace(',', ' '));

		}

		return temp;
	}

	/**
	 * This method create the meta data for the inverted index
	 * 
	 * @param indexDir
	 *            the directory where the inverted index exist
	 * @param path
	 *            the folder path where the meta-data will be saved
	 * @param day
	 *            the day of the current inverted index
	 * @return
	 * @throws IOException
	 */
	public boolean getAllKeywordOfInvertedIndex(String indexDir, String path)
			throws IOException {
		File file = new File(new File(path) + "/_inverted_allKeywords");
		if (!file.exists()) {
			file.createNewFile();
		}
		Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(file, true), "UTF8"));

		double startTime = System.currentTimeMillis();
		String field;
		field = "tweetText";

		IndexReader reader = null;
		try {
			reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		HashMap<String, Integer> keywords = new HashMap<String, Integer>();
		Fields fields = MultiFields.getFields(reader);
		Terms terms = fields.terms(field);
		TermsEnum iterator = terms.iterator(TermsEnum.EMPTY);
		BytesRef byteRef = null;
		while ((byteRef = iterator.next()) != null) {
			String term = new String(byteRef.bytes, byteRef.offset,
					byteRef.length);
			keywords.put(term, iterator.docFreq());
		}

		Map<String, Integer> sortedList = sortByComparator(keywords, DESC);
		System.out.println(sortedList.size());
		keywords = (HashMap<String, Integer>) sortedList;
		writer.append(day);
		Iterator it = keywords.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry obj = (Map.Entry) it.next();
			// System.out.println("k: "+obj.getKey()+" v: "+obj.getValue());
			writer.append((String) obj.getKey().toString().replace(',', ' ')
					+ "\n");

		}
		double endTime = System.currentTimeMillis();
		System.out.println("execution Time in MilliSeconds: "
				+ (endTime - startTime));
		writer.append("\n");
		writer.flush();
		writer.close();
		return true;
	}

	private Map<String, Integer> sortByComparator(
			HashMap<String, Integer> unsortMap, final boolean order) {
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(
				unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}

		});
		// Maintaining insertion order with the help of LinkedList
		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;

	}

	private static class KeyWord {
		private String word;
		private int freq;

		public KeyWord(String word, int freq) {
			this.word = word;
			this.freq = freq;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public int getFreq() {
			return freq;
		}

		public void setFreq(int freq) {
			this.freq = freq;
		}

		public KeyWord() {
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws UnsupportedEncodingException,
			IOException {
		String indexPath = "/export/scratch/louai/test/index/invertedindex/tweets/Day/index.2014-05-02/";
		String storedPath = "/export/scratch/louai/test/index/invertedindex/";
		MetaData searcher = new MetaData();
		searcher.buildMetaData(indexPath, storedPath, "2014-05-03", 5000);
		// searcher.getAllKeywordOfInvertedIndex(indexPath, storedPath);
	}
}
