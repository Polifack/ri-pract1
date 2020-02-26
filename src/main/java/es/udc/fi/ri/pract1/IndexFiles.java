package es.udc.fi.ri.pract1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntBinaryOperator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;

import es.udc.fi.ri.pract1.IndexFilesThreadPool.WorkerThread;

public class IndexFiles {
	
	static boolean create;
	

	private IndexFiles() {
	}
	interface TwoIntLambda {
	    public int operation(int a, int b);
	}
	
	public static class WorkerThread implements Runnable {

		private final Path folder; //Folder which contains the docs to index
		private final MMapDirectory dir; //Folder where to create the index (memory)

		public WorkerThread(final Path folder, final MMapDirectory dir) {
			this.folder = folder;
			this.dir = dir;
		}

		@Override
		public void run() {
			String ThreadName = Thread.currentThread().getName();
			System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
					ThreadName, folder));
			try {
				System.out.println(ThreadName+": Indexing to directory '" + dir + "'...");

				//Directory dir = FSDirectory.open(Paths.get(indexPath+"/"+ThreadName));
				
				Analyzer analyzer = new StandardAnalyzer();
				IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

				if (create) {
					iwc.setOpenMode(OpenMode.CREATE);
				} else {
					// Add new documents to an existing index:
					iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				}

				IndexWriter writer = new IndexWriter(dir, iwc);
				//Do indexDoc to every file of folder (como indexDocs)
				indexDocs(writer,folder,ThreadName);

				writer.close();

			} catch (IOException e) {
				System.out.println(ThreadName+": caught a " + e.getClass() + "\n with message: " + e.getMessage());
			}
		}

	}
	
	private void readConfig(String filePath) throws IOException {
		FileInputStream inputStream = new FileInputStream(filePath);
		Properties prop = new Properties();
		
		prop.load(inputStream);
		String docs_nt = prop.getProperty("DOCS");
		String partial_nt = prop.getProperty("PARTIAL_INDEXES");
		String only_nt = prop.getProperty("ONLY_FILES");
		
		String[] docs = docs_nt.split(" ");
		String[] partial = partial_nt.split(" ");
		String[] only = only_nt.split(" ");
	}
	
	private static String read5Lines(String fileName, int mode) throws IOException {
		String result = "";
        BufferedReader br = new BufferedReader(
        		new InputStreamReader(new FileInputStream(fileName)));
        LinkedList<String> lines = new LinkedList<String>();
		
		//Remove lines until only 5. If 0 remove last, if 1 remove first
		for(String tmp; (tmp = br.readLine()) != null;) 
		    if (lines.add(tmp) && lines.size() > 5) {
		    	if (mode == 0) lines.removeLast();
		    	if (mode == 1) lines.remove(0);
		    }
		
		for (String line : lines) {
			result += line+"\n";
		}
		
		br.close();
		return result;
	}

	static void indexDocs(final IndexWriter writer, Path path, String thread) throws IOException {
		Files.walkFileTree(path,new HashSet<>(),1, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (!Files.isDirectory(file)) {
					try {
						indexDoc(writer, file, attrs.lastModifiedTime().toMillis(),thread);
					} catch (IOException ignore) {
					}
				}
				return FileVisitResult.CONTINUE;
			}
		});
	}

	static void indexDoc(IndexWriter writer, Path file, long lastModified,String thread) throws IOException {
		try (InputStream stream = Files.newInputStream(file)) {
			Document doc = new Document();
			
			doc.add(new StringField("path", file.toString(), Field.Store.YES));
			doc.add(new LongPoint("modified", lastModified));
			doc.add(new TextField("contents",new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
			doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
			doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
			doc.add(new DoublePoint("sizeKb", (double) (new File(file.toString()).length() / 1024)));
			doc.add(new TextField("top5Lines", read5Lines(file.toString(),0), Field.Store.YES)) ;
			doc.add(new TextField("bottom5Lines", read5Lines(file.toString(),1), Field.Store.YES)) ;

			if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
				System.out.println(thread +": adding " + file);
				writer.addDocument(doc);
			} else {
				System.out.println(thread + ": updating " + file);
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
		}
	}

	public static void main(String[] args) {
		String usage = "java -jar IndexFiles" + " [-index INDEX_PATH] [-update]\n\n";
		
		String indexPath = "index";
		int nThreads = Runtime.getRuntime().availableProcessors();
		boolean partialIndex = false;
		boolean onlyFiles = false;
		boolean create = true;
		OpenMode openMode = null;
		
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-update".equals(args[i])) {
				create = false;
			} else if ("-numThreads".equals(args[i])) {
				nThreads = Integer.parseInt( args[i+1] );
				i++;
			} else if ("-openMode".equals(args[i])) {
				openMode = IndexWriterConfig.OpenMode.valueOf(args[i+1]);
				i++;
			} else if("partialIndexes".equals(args[i])) {
				partialIndex =true;
				i++;
			}else if("onlyFiles".equals(args[i])) {
				onlyFiles = true;
				i++;
			}
		}
		
		final int numCores = Runtime.getRuntime().availableProcessors();
		final ExecutorService executor = Executors.newFixedThreadPool(numCores);
		
		/* Los DOC DIR SE OBTIENEN A TRAVES DE LA CONFIG FILE
		final Path docDir = Paths.get(docsPath);
		if (!Files.isReadable(docDir)) {
			System.out.println("Document directory '" + docDir.toAbsolutePath()
					+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}*/
		final Path docDir = null;
		
		List<MMapDirectory> dirList = new ArrayList<MMapDirectory>();

		Date start = new Date();
		MMapDirectory mmapdir = null;
		int i = 0;
		
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir)) {
			mmapdir = new MMapDirectory(Paths.get("/tmp/LuceneIndex"));
			dirList.add(mmapdir);
			
			final Runnable mainWorker = new WorkerThread(docDir,mmapdir);
			executor.execute(mainWorker);
			for (final Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					mmapdir = new MMapDirectory(Paths.get("/tmp/LuceneIndex"+i++));
					dirList.add(mmapdir);
					
					final Runnable worker = new WorkerThread(path,mmapdir);
					/*
					 * Send the thread to the ThreadPool. It will be processed eventually.
					 */
					executor.execute(worker);
				}
			}

		} catch (final IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		
		/*
		 * Close the ThreadPool; no more jobs will be accepted, but all the previously
		 * submitted jobs will be processed.
		 */
		executor.shutdown();

		/* Wait up to 1 hour to finish all the previously submitted jobs */
		try {
			executor.awaitTermination(1, TimeUnit.HOURS);
		} catch (final InterruptedException e) {
			e.printStackTrace();
			System.exit(-2);
		} finally {
			//Merge directories
			System.out.println("Merging indexes into "+indexPath);
			IndexWriterConfig iconfig = new IndexWriterConfig(new StandardAnalyzer());
			
			if (create) {
				iconfig.setOpenMode(OpenMode.CREATE);
			} else {
				iconfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
			}
			
			IndexWriter ifusedwriter = null;
			try {
				Directory dir = FSDirectory.open(Paths.get(indexPath));
				ifusedwriter = new IndexWriter(dir, iconfig);

				
				for (MMapDirectory tmp : dirList) {
					ifusedwriter.addIndexes(tmp);
				}
				ifusedwriter.commit();
				ifusedwriter.close();
			} catch (IOException e) {
				System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
			}
		}

		
		
		Date end = new Date();
		System.out.println(end.getTime() - start.getTime() + " total milliseconds");
	}

}