package es.udc.fi.ri.pract1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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


public class IndexFiles {
	
	static String indexPath = "index";
	static int numCores = Runtime.getRuntime().availableProcessors();
	static int nThreads = numCores;
	static boolean partialIndex = false;
	static boolean onlyFiles = false;
	static boolean multithread = false;
	static OpenMode openMode = OpenMode.CREATE_OR_APPEND;
	
	static String[] partial;
	static List<String> fileTypes;
	static List<Path> docsDir;
	static boolean create;
	
	public static class WorkerThread implements Runnable {

		private final Path folder; 
		private final MMapDirectory dir; 
		
		public WorkerThread(final Path folder, final MMapDirectory dir) {
			
			this.folder = folder;
			this.dir = dir;
		}

		@Override
		public void run() {
			String ThreadName = Thread.currentThread().getName();
			System.out.println("[T-"+Thread.currentThread().getId()+"] Indexing folder "+folder
					+ " at "+dir);
			try {
				Analyzer analyzer = new StandardAnalyzer();
				IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
				iwc.setOpenMode(openMode);
				IndexWriter writer = new IndexWriter(dir, iwc);
				Files.walkFileTree(folder, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attr) throws IOException {   
						//Check if -onlyFiles is enabled
						if (onlyFiles) {
							//We check what type of file is this
							String fileType = getFileExtension(file.toFile());
							
							//If the file extension is included add it
							if (fileTypes.contains(fileType)) {
								try {
									indexDoc(writer, file, attr.lastModifiedTime().toMillis(), ThreadName);
								} 
								catch (IOException ignore) {
								}
							}
						}
						else {
							try {
								indexDoc(writer, file, attr.lastModifiedTime().toMillis(), ThreadName);
							} 
							catch (IOException ignore) {
							}
						}
						return FileVisitResult.CONTINUE;
					}
				});
				writer.close();

			} catch (IOException e) {
				System.out.println("[T-"+Thread.currentThread().getId()+"]"+": ERROR: " + e.getMessage());
			}
		}

	}
	
	private static void readConfig(String filePath) throws IOException {
		FileInputStream inputStream = new FileInputStream(filePath);
		Properties prop = new Properties();
		
		prop.load(inputStream);
		String docs_nt = prop.getProperty("DOCS");
		String partial_nt = prop.getProperty("PARTIAL_INDEXES");
		String only_nt = prop.getProperty("ONLY_FILES");
		
		String[] docs = docs_nt.split(" ");
		partial = partial_nt.split(" ");
		String[] only = only_nt.split(" ");
		
		docsDir = new ArrayList<Path>();
		for (int i = 0; i< docs.length; i++) {
			Path docDir = Paths.get(docs[i]);
			docsDir.add(docDir);
		}
		
		fileTypes = Arrays.asList(only);
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

	private static String getFileExtension(File file) {
	        String fileName = file.getName();
	        if(fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0)
	        return fileName.substring(fileName.lastIndexOf("."));
	        else return "";
	    }
	
	static void indexDocs(final IndexWriter writer, Path path) throws IOException {
		if (Files.isDirectory(path)) {
			//walkFileTree(path start, set<FileVisitOptions> options)
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attr) throws IOException {
			        
					//Check if -onlyFiles is enabled
					if (onlyFiles) {
						//We check what type of file is this
						String fileType = getFileExtension(file.toFile());
						
						//If the file extension is included add it
						if (fileTypes.contains(fileType)) {
							try {
								indexDoc(writer, file, attr.lastModifiedTime().toMillis(), "Main Thread");
							} 
							catch (IOException ignore) {
							}
						}
					}
					else {
						try {
							indexDoc(writer, file, attr.lastModifiedTime().toMillis(), "Main Thread");
						} 
						catch (IOException ignore) {
						}
					}

					

					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis(),"Main Thread");
		}
	}
	
	static void indexDoc(IndexWriter writer, Path file, long lastModified, String thread) throws IOException {
		try (InputStream stream = Files.newInputStream(file)) {
			//Create doc and add fields
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
				//System.out.println("[*] Adding " + file);
				writer.addDocument(doc);
			} else {
				//System.out.println("[*] Updating " + file);
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
		}
	}

	public static void main(String[] args) {
		String usage = 	"[*] USAGE: java -jar IndexFiles [-openmode <APPEND | CREATE | APPEND_OR_CREATE>]" + 
						"[-onlyFiles] [-index INDEX_PATH] [-update] [-partialIndexes] "+
						"[-numThreads NUM_THREADS]+ \n\n";
		System.out.println(usage);
		//Process arguments
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-numThreads".equals(args[i])) {
				multithread = true;
				nThreads = Integer.parseInt( args[i+1] );
				i++;
			} else if ("-openMode".equals(args[i])) {
				openMode = IndexWriterConfig.OpenMode.valueOf(args[i+1]);
				i++;
			} else if("-partialIndexes".equals(args[i])) {
				partialIndex =true;
			}else if("-onlyFiles".equals(args[i])) {
				onlyFiles = true;
			}
		}
		final ExecutorService executor = Executors.newFixedThreadPool(numCores);
		List<MMapDirectory> dirList = new ArrayList<MMapDirectory>();

		System.out.println("**************************************************");
		System.out.println("[*] Launching IndexFiles with "+nThreads+" threads.");
		System.out.println("[*] The indexPath is "+indexPath);
		System.out.println("[*] The indexMode is "+openMode);
		System.out.println("[*] Partial indexes enable value is "+partialIndex);
		System.out.println("[*] File extension filtering value is "+onlyFiles);
		System.out.println("**************************************************");
		
		try {
			readConfig("./IndexFiles.config");
		} catch (IOException e1) {
			System.out.println("[!] Cannot read ./IndexFiles.config");
			System.exit(1);
		}
		
		for (Path p : docsDir){
			if (!Files.isReadable(p)) {
				System.out.println("[!] Document directory '" + p.toAbsolutePath()
						+ "' does not exist or is not readable, please check the path");
				System.exit(1);
			}
		}
		Date start = new Date();
		try {
			System.out.println("[*] Indexing to directory '" + indexPath + "'...");

			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(openMode);

			IndexWriter writer;

			writer = new IndexWriter(dir, iwc);
			MMapDirectory mmapdir = null;
			
			//Para cada uno de los directorios en la config file
			for (Path p : docsDir){
				
				if (multithread) {
					int i = 0;
					//Creamos un worker que se encargue de indexarlo
					try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(p)) {
						mmapdir = new MMapDirectory(Paths.get("/tmp/LuceneIndex"));
						dirList.add(mmapdir);

						final Runnable mainWorker = new WorkerThread(p, mmapdir);
						executor.execute(mainWorker);

						for (final Path path : directoryStream) {
							if (Files.isDirectory(path)) {
								mmapdir = new MMapDirectory(Paths.get("/tmp/LuceneIndex" + i++));
								dirList.add(mmapdir);

								final Runnable worker = new WorkerThread(path, mmapdir);
								//Y enviamos el worker a la thread pool
								executor.execute(worker);
							}
						}
					} catch (final IOException e) {
						e.printStackTrace();
						System.exit(-1);
					} 

					//Cerramos el pool de threads
					executor.shutdown();
					//Le damos 1h para terminar la tarea
					try {
						executor.awaitTermination(1, TimeUnit.HOURS);
					} catch (final InterruptedException e) {
						System.out.println("[!] Timeout during index creation: "+e);
						System.exit(-2);
					} finally {
						//Fusionamos los indices temporales
						System.out.println("[*] Merging indexes into "+indexPath);
							for (MMapDirectory tmp : dirList) {
								writer.addIndexes(tmp);
							}
							writer.commit();
							writer.close();
					//Printeamos el tiempo
					Date end = new Date();
					System.out.println("[*] "+ (end.getTime() - start.getTime()) + " total milliseconds");
					}
				}
				else {
					indexDocs(writer, p);
					writer.commit();
					writer.close();

					Date end = new Date();
					System.out.println("[*] "+ (end.getTime() - start.getTime()) + " total milliseconds");
				}
			}
		} catch (IOException e1) {
			System.out.println("[!] Error during index creation: "+e1);
		}

		
		
		Date end = new Date();
		System.out.println(end.getTime() - start.getTime() + " total milliseconds");
	}
}