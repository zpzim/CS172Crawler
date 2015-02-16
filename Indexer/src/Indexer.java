import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import twitter4j.HashtagEntity;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;


public class Indexer {
	private Indexer() {}
	public static void main(String[] args) {
		String usage = "java org.apache.lucene.demo.IndexFiles"
				+ " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
				+ "This indexes the documents in DOCS_PATH, creating a Lucene index"
				+ "in INDEX_PATH that can be searched with SearchFiles";
		String indexPath = null;
		String docsPath = null;
		boolean create = true;
		boolean search = false;
		String query = "";
		for(int i=0;i<args.length;i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i+1];
				i++;
			} else if ("-docs".equals(args[i])) {
				docsPath = args[i+1];
				i++;
			} else if ("-update".equals(args[i])) {
				create = false;
			} else if ("-search".equals(args[i])){
				search = true;
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				try {
					query = br.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(1);
				}
			}
			
		}
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_3);
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);
		//If search parameter set...
		//Get Query and Perform search functionality
		if(search && indexPath != null){
			Query q = null;
			try {
				q = new QueryParser(Version.LUCENE_4_10_3, "hashtag", analyzer).parse(query);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(1);
			}
			int hitsPerPage = 10;
			Directory idxDir = null;
			try {
				idxDir = FSDirectory.open(new File(indexPath));
				IndexReader reader = IndexReader.open(idxDir);
				IndexSearcher searcher = new IndexSearcher(reader);
				TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
				searcher.search(q, collector);
				ScoreDoc[] hits = collector.topDocs().scoreDocs;
				for(int i = 0; i < hits.length; ++i){
					System.out.println(reader.document(hits[i].doc).get("body"));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.exit(0);
		}
		
		//Else perform indexing
		if(docsPath == null){
			System.out.println("Usage: " + usage);
			System.exit(1);
		}
		final File docDir = new File(docsPath);
		if(!docDir.exists()||!docDir.canRead()){
			System.out.println("Document directory '" +docDir.getAbsolutePath()+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}
		Date start = new Date();
		System.out.println("Indexing to directory '" + indexPath + "'...");
		Directory dir = null;
		try {
			dir = FSDirectory.open(new File(indexPath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
		if(create){
			iwc.setOpenMode(OpenMode.CREATE);
		}else{
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		}
		IndexWriter writer;
		try {
			writer = new IndexWriter(dir, iwc);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
		indexDocs(writer, docDir, docsPath);
		//writer.forceMerge(1);
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		Date end = new Date();
		System.out.println(end.getTime() - start.getTime() + " total milliseconds");
	}
	
	
	static void indexDocs(IndexWriter writer, File file, String dir){
		if(file.isDirectory()){
				String[] files = file.list();
				if(files != null){
					
					for(int i = 0; i < files.length; ++i){
						System.err.println("Indexing: " + files[i]);
						indexDocs(writer, new File(dir + "\\" + files[i]), dir + "\\" + files[i]);
					}
				}
		}else{
			System.out.println(file);
			FileInputStream infile;
			BufferedReader reader;
			try {
				//System.err.println(file);
				infile = new FileInputStream(file);
				reader = new BufferedReader(new InputStreamReader(infile, "UTF-8"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
			String JSONline;
			try {
				System.out.println("adding " + file + "...");
				while((JSONline = reader.readLine()) != null){
					if(writer.getConfig().getOpenMode() == OpenMode.CREATE){
						//System.out.println("adding " + file);
						addTweet(writer, JSONline);
					}
					else{
						System.out.println("updating " + file);
						//writer.updateDocument(new Term("path", file.getPath()), d);
					}
				}
				System.out.println("adding " + file + " done");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	static void addTweet(IndexWriter writer, String RAWjson){
		Document d = new Document();
		Status status = null;
		try {
			status = TwitterObjectFactory.createStatus(RAWjson);
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		HashtagEntity[] tags = status.getHashtagEntities();
		for(int i = 0; i < tags.length; ++i){
			d.add(new StringField("hashtag", tags[i].getText(), Field.Store.YES));
		}
		d.add(new TextField("body", status.getText(), Field.Store.YES));
		if(status.getURLEntities().length > 0){
			JSONObject j = null;
			try {
				j = new JSONObject(RAWjson);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String s = null;
			try {
				s = j.getJSONArray("PageTitles").getString(0);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
			//System.out.println(s);
			if(s != null){
				d.add(new TextField("PageTitle", s, Field.Store.YES));
			}
		}
		try {
			writer.addDocument(d);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
