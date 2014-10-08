package server;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.io.*;
import java.net.*;


public class TwitterStreamServer implements Runnable{

	public static int tmpCount = 1;
	public static int servedRequestCount = 0;
	public static int qSize = 100000;
	public static ArrayBlockingQueue<QEntry> queue = new ArrayBlockingQueue<QEntry>(qSize);
	
	public void runServer(int port) {
		String clientRequest; 
		String tweetsStr;

		ServerSocket welcomeSocket;
		try {
			welcomeSocket = new ServerSocket(port);
			
			System.out.println("Twitter Stream Server Listening on port " + port + "...");
			while(true) {
				Socket connectionSocket = welcomeSocket.accept();

				System.out.println("A client connected on "+new Date().toString()+"...");
				
				BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
				//BufferedReader br           = new BufferedReader(new InputStreamReader(new FileInputStream("DirectionResponse.xml"), "UTF-8"));
				
				DataOutputStream  outToClient = new DataOutputStream(connectionSocket.getOutputStream());
				clientRequest = inFromClient.readLine();
				
				++servedRequestCount;
				System.out.println(servedRequestCount+") Client request: " + clientRequest);
				clientRequest = clientRequest.toLowerCase();
				
				if(clientRequest.startsWith("taghreed "))
				{
					try
					{
						String [] clientRequestFields = clientRequest.split(" ");

						int msgType = Integer.parseInt(clientRequestFields[1]);
						int sizeToGet = Integer.parseInt(clientRequestFields[2]);
						
						ArrayList<String> filter_kws = new ArrayList<String>();
						for(int i = 3; i < clientRequestFields.length; ++i)
						{
							clientRequestFields[i] = decode_unicode_str(clientRequestFields[i]);
							filter_kws.add(clientRequestFields[i]);
						}
						
						if(sizeToGet < 0) sizeToGet = queue.size();
						else sizeToGet = Math.min(sizeToGet, queue.size());
						
						int responseLength;
						String responseLengthStr;
						
						if(msgType == 1)
						{
							if(filter_kws.size() == 0)
								tweetsStr = getTweetsJSON(sizeToGet);
							else
								tweetsStr = getTweetsJSON(sizeToGet,filter_kws);
							
							responseLength = tweetsStr.length();
							responseLengthStr = String.format("%09d", responseLength);
							System.out.println("Sending " + responseLengthStr + " bytes");
							System.out.println(tweetsStr);
							
							outToClient.write(responseLengthStr.getBytes());
							outToClient.flush();
							outToClient.write(tweetsStr.getBytes());
							outToClient.flush();
						}
						else if(msgType == 2)
						{
							if(filter_kws.size() == 0)
								tweetsStr = getTweetsJSON_ByProvince(sizeToGet,null);
							else
								tweetsStr = getTweetsJSON_ByProvince(sizeToGet,filter_kws);
							
							responseLength = tweetsStr.length();
							responseLengthStr = String.format("%09d", responseLength);
							System.out.println("Sending " + responseLengthStr + " bytes");
							System.out.println(tweetsStr);
							
							outToClient.write(responseLengthStr.getBytes());
							outToClient.flush();
							outToClient.write(tweetsStr.getBytes());
							outToClient.flush();
						}
					} catch(NumberFormatException e) {
						e.printStackTrace();
					}
				}
				connectionSocket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String decode_unicode_str(String str) {
		int chars_count = str.length()/6;
		StringBuffer buf = new StringBuffer();
		
		int curr_index = 2;
		int pattern_length = 4;
		
		for(int i = 0; i < chars_count; ++i, curr_index += 6)
		{
			int pnt_code = Integer.parseInt(str.substring(curr_index,curr_index+pattern_length),16);
			buf.appendCodePoint(pnt_code);
		}
		return buf.toString();
	}

	private String getTweetsJSON(int sizeToGet) {
		int size = Math.min(sizeToGet, queue.size());
		String jsonStr = "{\"tweets\":[";
		for(int i = 0; i < size; ++i) {
			try {
				jsonStr += "";
				if(i < size-1) {
					jsonStr += ",";
				}
			}
            catch (Exception e) {
				e.printStackTrace();
			}
		}
		jsonStr += "]}";
		return jsonStr;
	}
	
	private String getTweetsJSON(int sizeToGet, ArrayList<String> filter_kws) {
		int size = Math.min(sizeToGet, queue.size());
		String jsonStr = "{\"tweets\":[";
		boolean putAtLeastOne = false;
		for(int i = 0; i < size; ++i) {
			try {
				
				QEntry currTweet = queue.take();
				boolean validTweet = contains(currTweet,filter_kws);
				if(validTweet)
				{
					if(putAtLeastOne) {
						jsonStr += ",";
					}
					jsonStr += "";
					putAtLeastOne = true;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		jsonStr += "]}";
		return jsonStr;
	}
	
	private String getTweetsJSON_ByProvince(int sizeToGet, ArrayList<String> filter_kws) {
		int size = Math.min(sizeToGet, queue.size());
		String jsonStr = "{\"tweets\":[";
		boolean putAtLeastOne = false;
		for(int i = 0; i < size; ++i) {
			try {
				QEntry currTweet = queue.take();
				TwitterPost currTweetPost =  currTweet.getPost();
				
				boolean validTweet = contains(currTweet,filter_kws);
				if(validTweet)
				{
					int province = insideKSA(currTweetPost.getLat(),currTweetPost.getLng()); 
					if(province != -1)
					{
						//currTweetPost.setProvince(province);
						if(putAtLeastOne) {
							jsonStr += ",";
						}
						jsonStr +="";
						putAtLeastOne = true;
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		jsonStr += "]}";
		return jsonStr;
	}
	
	private int insideKSA(double lat, double lng) {
		double [] southes = {19.46472866289902,20.556269626874602,23.251046729771254,26.609682371901762,18.742214334230987,16.921858339040128,25.76151176587871,24.691746947123256,28.820531521914063,28.134831845193883,17.88506269492949,19.44579193264303,16.409029384127088};
		double [] westes = {43.502490234375045,38.514697265625045,37.734667968750045,34.592578125000045,46.084277343750045,43.667285156250045,39.701220703125045,41.722705078125045,39.942919921875045,36.218554687500045,41.964404296875045,40.942675781250045,41.579882812500045};
		double [] northes = {25.653954558526685,23.216133434752635,26.550320323038758,28.622447770571846,29.02603490402045,19.32040754926309,28.642057106543966,27.153011094070056,31.74315570238898,30.426037408400035,20.704897891966215,20.72593714235712,17.82833587037734};
		double [] eastes = {47.486279296874955,43.443310546874955,41.982128906249955,38.345654296874955,50.716259765624955,47.673046874999955,43.827832031249955,44.651806640624955,44.333203124999955,41.070263671874955,44.300244140624955,41.938183593749955,43.410351562499955};

		for(int i = 0; i < southes.length; ++i)
		{
			if(lat >= southes[i] && lat <= northes[i] && lng >= westes[i] && lng <= eastes[i])
				return i;
		}
		return -1;
	}

	private boolean contains(QEntry currTweet, ArrayList<String> filter_kws) {
		if(filter_kws == null)
			return true;
		boolean validTweet = false;
		for(String kw : filter_kws)
		{
			//validTweet = currTweet.getText().indexOf(kw) >= 0;
			validTweet = currTweet.getText().contains(kw);
			
			if(validTweet)
				return true;
		}
		return false;
	}

	private void runVirtualCrawler(String[] args) {
		TwitterFileStream stream = null;
		try {
			stream = new TwitterFileStream(args);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		while(true) {
			String nextTweetStr;
			try {
				nextTweetStr = stream.getNextTweetJSON();
				TwitterPost tweet = new TwitterPost(nextTweetStr);
				//System.out.println(tweet.getText());
				queue.add(tweet.createQEntry());
				if(queue.size() >= qSize)
				{
					try {
						//throghout oldest tweets 
						queue.take();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void run() {
		//String[] args = getFilesNames("H:\\UmmAlQura\\Twitter_Data\\sample\\");
		//runVirtualCrawler(args);
		//String[] args = {};
		//TwitterStream.runLocationCrawler(args);
	}

	private String[] getFilesNames(String folderPath) {

		  ArrayList<String> files = new ArrayList<String>();
		  File folder = new File(folderPath);
		  File[] listOfFiles = folder.listFiles(); 
		 
		  for (int i = 0; i < listOfFiles.length; i++) 
		  {
			  if (listOfFiles[i].isFile())
				  files.add(listOfFiles[i].getAbsolutePath());
		  }
		  
		  String[] filesArr = new String[files.size()];
		  filesArr = files.toArray(filesArr);
		  return filesArr;
	}

	public static void main(String[] args) {
		//String tst = "������ �������� ���������� ���� �������� ������";
		//String tst2 = "\u0645\u0631\u062D\u0628\u0627";
		//String tst2 = "\u201c: \u0627\u0644\u0646\u0627\u0633 \u0641\u0640 \u0647\u0630\u0627 \u0627\u0644\u0632\u0645\u0646 \u0635\u0627\u0628\u0647\u0645 \u0634\u064a \n\u0645\u0627\u063a\u064a\u0631 \u0642\u0627\u0644 \u0641\u0644\u0627\u0646 \u0648 \u0641\u0644\u0627\u0646  \u0633\u0648\u0649\n\n\u0644\u0627\u064a\u0633\u0644\u0645 . \u0627\u0644\u0645\u064a\u062a \u0648\u0644\u0627\u064a\u0633\u0644\u0645  \u0627\u0644\u062d\u064a\n\u0647\u0630\u0627  \u0648\u0647\u0645  \u0630\u0631\u064a\u0629   \u0627\u062f\u0645  \u0648\u062d\u0648\u0649\u201d\u201d";
		//String tst2 = "\u0627\u0644\u0632\u0645\u0646";
		//System.out.println(tst.contains(tst2));
		//System.out.println(tst);
		//return;
		
		
		TwitterStreamServer server = new TwitterStreamServer();
		Thread t = new Thread(server);
        t.start();
        server.runServer(1150);
        
		//String [] files = {"I:\\Twitter_Data\\ArabSample\\2013-10-12\\2013-10-12_11-42-02.txt"};
		//createOfflineGoogleHeatmpData(files);
	}

	private static void createOfflineHeatmpData(String [] filenames) {
		TwitterFileStream stream = null;
		try {
			stream = new TwitterFileStream(filenames);
			/* Sample
			 * var testData={
            max: 46,
            data: [{lat: 33.5363, lng:-117.044, count: 1},{lat: 33.5608, lng:-117.24, count: 1},{lat: 38, lng:-97, count: 1},{lat: 38.9358, lng:-77.1621, count: 1}]
    		};
			 */
			String jsArray = "var testData = {max:100002,data:[";
			
			TwitterPost firsttweet = new TwitterPost(stream.getNextTweetJSON());
			jsArray += "{lat:"+firsttweet.getLat()+",lng:"+firsttweet.getLng()+",count:1}";
			
			for(int i = 0; i < 100000; ++i)
			{
				System.out.println(i);
				TwitterPost tweet = new TwitterPost(stream.getNextTweetJSON());	
				jsArray += ",{lat:"+tweet.getLat()+",lng:"+tweet.getLng()+",count:1}";
			}
			jsArray += "]};";
			
			PrintStream outputFile = null;
		    try {
				outputFile = new PrintStream(new File("heatmapData.js"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		    
		    outputFile.print(jsArray);
		    outputFile.flush();
		    outputFile.close();
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static void createOfflineGoogleHeatmpData(String [] filenames) {
		TwitterFileStream stream = null;
		try {
			stream = new TwitterFileStream(filenames);
			/* Sample
			 * var testData=[
			new google.maps.LatLng(37.782551, -122.445368),
  			new google.maps.LatLng(37.782745, -122.444586),
  			new google.maps.LatLng(37.782842, -122.443688),
  			new google.maps.LatLng(37.782919, -122.442815),
  			new google.maps.LatLng(37.782992, -122.442112)
			  ];
			 */
			String jsArray = "var testDataGoogle = [";
			
			TwitterPost firsttweet = new TwitterPost(stream.getNextTweetJSON());
			jsArray += "new google.maps.LatLng("+firsttweet.getLat()+","+firsttweet.getLng()+")";
			
			for(int i = 0; i < 100000; ++i)
			{
				System.out.println(i);
				TwitterPost tweet = new TwitterPost(stream.getNextTweetJSON());	
				jsArray += ",new google.maps.LatLng("+tweet.getLat()+","+tweet.getLng()+")";
				
			}
			jsArray += "];";
			
			PrintStream outputFile = null;
		    try {
				outputFile = new PrintStream(new File("heatmapDataGoogle.js"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		    
		    outputFile.print(jsArray);
		    outputFile.flush();
		    outputFile.close();
		    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
