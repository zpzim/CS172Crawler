import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.jsoup.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;



public final class Stream {
	//public static PrintWriter writer;
	final static int __COUNT = 0;
	static int numThreads = 0;
	static private String path;
	static private int initCount;
	//public static File file = null; 
	public static void main(String[] args) throws TwitterException {
		if(args.length == 0 || args.length > 2)
		{
			System.err.println("Correct Usage: Stream.jar path/to/output/directory/ [initial file number]");
			return;
		}
		if(args.length == 1)
		{
			path = args[0];
			initCount = 0;
		}
		else if(args.length == 2)
		{
			path = args[0];
			initCount = Integer.parseInt(args[1]);
		}
    	//authenticate stream
		//int counter = __COUNT;
    	ConfigurationBuilder cb = new ConfigurationBuilder();
    	cb.setJSONStoreEnabled(true);
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey("htQrouJTZMdgRn6EvijRvfDkC");
		cb.setOAuthConsumerSecret("89TqaYw0uyvr0A0FaavR9WIvLASJkGR2T6kRMXvgRqM9PkbHnS");
		cb.setOAuthAccessToken("2887968636-nMSmcXxzuq6bLm5vl247H9EmMHuT4IjDCZ4KXko");
		cb.setOAuthAccessTokenSecret("IZONMOMRlgzqla4QTsQWChMPyLVmrbVtw24m8J823kmVA");
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        System.out.println("Init done");
        //file = new File("D:\\TweetData\\TweetData"+((Integer)counter).toString() + ".txt");
        /*try {
			writer = new PrintWriter(file, "UTF-8");
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (UnsupportedEncodingException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}*/
        
        StatusListener listener = new StatusListener() {
           // double fileSize = 0;
          //  int count = __COUNT;
            public void onStatus(Status status) {
            	User user = status.getUser();
            	if(user.isGeoEnabled()) {
            		
            	String tweetText = TwitterObjectFactory.getRawJSON(status);
            	JSONObject tweet = null;
					try {
						tweet = new JSONObject(tweetText);
					} catch (JSONException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
            		if(status.getURLEntities().length != 0){
            			//ArrayList<String> pageTitles = new ArrayList<String>();
            			URLEntity[] Urls = status.getURLEntities();
                		for(int i = 0; i < Urls.length; ++i){
                			//Document doc = null;
                			
                			//try {
                				//System.out.println(Urls[i].getExpandedURL());
                				//String title = TitleExtractor.getPageTitle(Urls[i].getExpandedURL());
                				
                				//System.out.println(title + '\n');
                				//if(title != null){
                				//	pageTitles.add(title);
                			//	}	
							//} catch (IOException e1) {
								//// TODO Auto-generated catch block
								//e1.printStackTrace();
							//}
                			
                			FetchTitle ft = new FetchTitle(Urls[i].getExpandedURL(), tweet);
                			
                			while(numThreads > 100){
                				try {
									Thread.sleep(5000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}	
                			}
                			Thread FTThread = new Thread(ft, "FetchTitle" + ((Integer)numThreads).toString());
                			System.out.println("Spawning Fetch Title Thread");
                			FTThread.start();
                		}
                			/*try {
                				Connection con = Jsoup.connect(Urls[i].getExpandedURL()).userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21").timeout(10000);
								Connection.Response resp = con.execute();
								if(resp.statusCode() == 200){
									doc = con.get();
									System.out.println(doc.title());
									pageTitles.add(doc.title());
									con.timeout(0);
								}
								
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                		}
                		try {
                			if(pageTitles.size() != 0)
							{
                				tweet.put("Page Titles", pageTitles);
							}
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
            		}
	            	//String username = status.getUser().getScreenName();
	                //System.out.println("@" + username);
	                //Date date = status.getCreatedAt();
	                //System.out.println(date);
	                //String profileLocation = user.getLocation();
	                //System.out.println(profileLocation);
	                //String content = status.getText();
	                
	                //URLEntity[] Urls = status.getURLEntities();
            		//for(int i = 0; i < Urls.length; ++i){
            		//	System.out.println(Urls[i].getExpandedURL());
            		//}
            		//System.out.println(content +"\n");
            		//System.out.println(tweet.toString());
            		*/
                	//fileSize += tweet.toString().getBytes().length;
            		//writer.println(tweet.toString());
            		//if(fileSize > 10485760){ //
            			/*fileSize = 0;
            			writer.close();
            			count++;
            			file = new File("D:\\TweetData\\TweetData"+((Integer)count).toString() + ".txt");
            			try {
							writer = new PrintWriter(file, "UTF-8");
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (UnsupportedEncodingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}*/
            			/*try {
							writer = new PrintWriter("C:\\Users\\Zach\\Google Drive\\TweetData\\TweetData"+((Integer)count).toString() + ".txt", "UTF-8");
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (UnsupportedEncodingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}*/
            		
            		}
                
                }
            	//}
            }
            

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                //System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        
        twitterStream.addListener(listener);
        
        WriteThread writer = new WriteThread(path, initCount);
       
        System.out.println("Start Sampling");
        double[][] loc = { {-160.1573700905,-11.5382811043},{-37.8477935791,57.3701052779} };
        FilterQuery fq = new FilterQuery(null);
        String[] lang = {"English"};
       // fq.language(lang);
        fq.locations(loc);
        twitterStream.filter(fq);
        System.out.println("Spanwing Writer thread");
        Thread WThread = new Thread(writer, "Writer");
        WThread.start();
    }
    String getMetaTag(Document document, String attr) {
        Elements elements = document.select("meta[name=" + attr + "]");
        for (Element element : elements) {
            final String s = element.attr("content");
            if (s != null) return s;
        }
        elements = document.select("meta[property=" + attr + "]");
        for (Element element : elements) {
            final String s = element.attr("content");
            if (s != null) return s;
        }
        return null;
    }
}
