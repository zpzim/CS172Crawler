import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import twitter4j.JSONException;
import twitter4j.JSONObject;


public class FetchTitle implements Runnable{
		private String url;
		private JSONObject tweet;
		private RandomAccessFile f;
		ArrayList<String> pageTitles = new ArrayList<String>();
		Document doc = null;
		FetchTitle(String URL, JSONObject t) {
			this.url = URL;
			this.tweet = t;
		}
		public void run() {
			Stream.numThreads++;
			Document doc = null;
			boolean UHOST = false;
    		do{
				try {
    				Connection con = Jsoup.connect(url).userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21").timeout(10000);
					Connection.Response resp = con.execute();
					UHOST = false;
					if(resp.statusCode() == 200){
						doc = con.get();
						System.out.println(doc.title());
						pageTitles.add(doc.title());
					}
				}
					
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			
			}while(UHOST);
		try {
			if(pageTitles.size() != 0){
				tweet.put("PageTitles", pageTitles);
			}
			try {
				WriteThread.q.offer(tweet.toString(), 10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		Stream.numThreads--;
	}

}
