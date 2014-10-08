package server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;


public class TwitterFileStream {
	BufferedReader file = null;
	String [] filePath;
	int index = 0;
	boolean singlePass;
	
	public TwitterFileStream(String [] filePaths) throws FileNotFoundException {
		this.filePath = Arrays.copyOf(filePaths, filePaths.length);
		file = new BufferedReader(new FileReader(new File(this.filePath[0])));
		index = (index+1) % filePath.length;
		singlePass = false;
	}

    public TwitterFileStream(String [] filePaths , int i) throws FileNotFoundException {
        this.filePath = Arrays.copyOf(filePaths, filePaths.length);
        file = new BufferedReader(new FileReader(new File(this.filePath[i])));
        index = (index+1) % filePath.length;
        singlePass = false;
    }
	
	public TwitterFileStream(String [] filePaths, boolean singlePass , int i) throws FileNotFoundException {
		this.filePath = Arrays.copyOf(filePaths, filePaths.length);
		file = new BufferedReader(new FileReader(new File(this.filePath[i])));
		index = (index+1) % filePath.length;
		this.singlePass = singlePass;
	}
	
	public String getNextTweetJSON() throws IOException {
		String line = file.readLine();
		if(line == null) {
            if (singlePass && index == 0)
                return null;//pass finished
            else {
                file.close();
                //System.err.println(filePath==null?"null":"not null");
                //System.err.println(filePath.length<=index?"index out of range":"not out of range");
                //System.err.println(filePath.length);

                //file = new BufferedReader(new FileReader(new File(this.filePath[index])));
                index = (index + 1) % filePath.length;
                line = file.readLine();

            }
        }
		
		return line;
	}
	public void close() throws IOException
	{
		file.close();
	}
}
