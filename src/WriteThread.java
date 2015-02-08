import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;


public class WriteThread implements Runnable {
	private final static int INITCOUNT = 0;
	public static ArrayBlockingQueue<String> q = new ArrayBlockingQueue<String>(50, true);
	private static int fileSize = 0;
	private static int count = INITCOUNT;
	private String path;
	private static File file;
	private static PrintWriter writer;
	public WriteThread(String p)
	{
		path = p;
		if(System.getProperty("os.name").startsWith("Windows")){
				if(!path.endsWith("\\")) {
				path.concat("\\");
			}
		}
		else{
			if(!path.endsWith("/")){
				path.concat("/");
			}
		}
		
		count = 0;
		file = new File(path + "TweetData"+((Integer)count).toString() + ".txt");
	}
	public WriteThread(String p, int initialCount)
	{
		path = p;
		if(!path.endsWith("\\")) {
			path += "\\";
		}
		System.out.println(path);
		count = initialCount;
		file = new File(path + "TweetData"+((Integer)count).toString() + ".txt");
	}
	public void run() {
		
		try {
			
			writer = new PrintWriter(file, "UTF-8");
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (UnsupportedEncodingException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		while(true){
			while(!q.isEmpty())
			{
				
				String tweet = null;
				try {
					tweet = q.take();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				if(tweet != null){
					writer.println(tweet);
					fileSize += tweet.getBytes().length;
				}
				//writer.println(tweet.toString());
				if(fileSize > 10485760){ //
					fileSize = 0;
					writer.close();
					count++;
					file = new File(path + "TweetData"+((Integer)count).toString() + ".txt");
					try {
						writer = new PrintWriter(file, "UTF-8");
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}	
			}
			Thread.yield();
		}
	}
}