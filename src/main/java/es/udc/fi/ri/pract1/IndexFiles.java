package es.udc.fi.ri.pract1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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

public class IndexFiles {

	static String indexPath = "./index";
	static int numCores = Runtime.getRuntime().availableProcessors();
	static int nThreads = numCores;
	static boolean partialIndex = false;
	static boolean onlyFiles = false;
	static OpenMode openMode = OpenMode.CREATE_OR_APPEND;

	static List<String> fileTypes;
	static List<Path> docsDir;
	static List<Path> partialDir;

	static boolean create;

	static void debug(String s) {
		String ANSI_RESET = "\u001B[0m";
		String ANSI_GREEN = "\u001B[32m";
		System.out.println(ANSI_GREEN + "[*] -- " + ANSI_RESET + s);
	}

	static void error(String s) {
		String ANSI_RESET = "\u001B[0m";
		String ANSI_RED = "\u001B[31m";
		System.out.println(ANSI_RED + "[!] -- " + ANSI_RESET + s);
	}

	static void printfile(String fn, char mode){
		String ANSI_PURPLE = "\u001B[35m";
		String ANSI_RESET = "\u001B[0m";
		System.out.println(ANSI_PURPLE + "["+mode+"] -- " + ANSI_RESET +fn);
	}

	public static class WorkerThread implements Runnable {

		private final Path folder;
		private final FSDirectory dir;

		public WorkerThread(final Path folder, final FSDirectory dir) {
			this.folder = folder;
			this.dir = dir;
		}

		@Override
		public void run() {
			String ThreadName = Thread.currentThread().getName();
			try {
				//Creamos el writer
				Analyzer analyzer = new StandardAnalyzer();
				IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
				iwc.setOpenMode(openMode);
				IndexWriter writer = new IndexWriter(dir, iwc);

				//Navegamos el FileTree
				Files.walkFileTree(folder, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attr) throws IOException {
						// Check if -onlyFiles is enabled
						if (onlyFiles) {
							// We check what type of file is this
							String fileType = getFileExtension(file.toFile());

							// If the file extension is included add it
							if (fileTypes.contains(fileType)) {
								try {
									indexDoc(writer, file, attr.lastModifiedTime().toMillis(), ThreadName);
								} catch (IOException ignore) {
								}
							}
						} else {
							try {
								indexDoc(writer, file, attr.lastModifiedTime().toMillis(), ThreadName);
							} catch (IOException ignore) {
							}
						}
						return FileVisitResult.CONTINUE;
					}
				});
				writer.close();

			} catch (IOException e) {
				error("T-" + Thread.currentThread().getId()+ ": ERROR: " + e.getMessage());
			}
		}

	}

	private static void readConfig(String filePath) {
		/* En el archivo de configuración se indican 
		 * docs = paths de las carpetas a indexar
		 * partials = paths de las carpetas de los indices parciales
		 * 		- opcional
		 * 		- tiene que haber una carpeta por cada path indicado en docs
		 * only_files = tipo de archivos a indexar
		 * 		- opcional */

		FileInputStream inputStream;
		Properties prop = new Properties();
		
		try {
			inputStream = new FileInputStream(filePath);
			prop.load(inputStream);
		} catch (IOException e) {
			error("Error trying to read config file: "+e);
			e.printStackTrace();
		}

		//Read DOCS
		String docs_nt = prop.getProperty("DOCS");
		docsDir = new ArrayList<Path>();
		if (docs_nt!=null){
			String[] docs = docs_nt.split(" ");
			for (int i = 0; i< docs.length; i++) {
				Path docDir = Paths.get(docs[i]);
				docsDir.add(docDir);
			}
		}
		else{
			error("Error trying to read config file: DOCS can't be null");
			System.exit(-1);
		}

		//Read PARTIAL_INDEXES
		String partial_nt = prop.getProperty("PARTIAL_INDEXES");
		partialDir = new ArrayList<Path>();
		if (partial_nt!=null){
			String[] partial = partial_nt.split(" ");
			for (int i = 0; i< partial.length; i++) {
				Path partDir = Paths.get(partial[i]);
				partialDir.add(partDir);
			}
		}		
		else{
			error("Error trying to read config file: PARTIAL_INDEXES can't be null");
			System.exit(-1);
		}

		//Read ONLY_FILES
		String only_nt = prop.getProperty("ONLY_FILES");
		fileTypes = new ArrayList<String>();
		if (only_nt!=null){
			String[] only = only_nt.split(" ");
			fileTypes = Arrays.asList(only);
		}

		//Check that if there is partial indexes
		//there is at least one subindex per indexed folder
		if (!(partialDir.size()==0||partialDir.size()==docsDir.size())){
			error("Error trying to read config file: Different size of docs and partial indexes");
			System.exit(-1);
		}
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
				printfile("Adding " + file, 'A');
				writer.addDocument(doc);
			} else {
				printfile("Updating " + file, 'U');
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
		}
	}

	static void indexMulti(IndexWriter mainWriter){
		// j -> indice de las carpetas indicadas en PARTIAL_INDEXES de IndexFiles
		int j = 0;
		
		debug("Start indexMulti");
		//Lista de directorios para despues poder mergear
		List <FSDirectory> directory_list = new ArrayList<FSDirectory>();
		//Iniciamos un pool de ejecutores
		final ExecutorService executor = Executors.newFixedThreadPool(numCores);		
		//Para todos los PATHS indicados en IndexFiles.config
		try{
			for (Path p:docsDir){
				Path partialIndexPath = partialDir.get(j);
				FSDirectory partialIndexDir = FSDirectory.open(partialIndexPath);
				debug ("Sending "+p+ " to be indexed in "+partialIndexPath);
				final Runnable worker = new WorkerThread(p, partialIndexDir);
				executor.execute(worker);
				j++;
			}
		}
		catch (Exception e){
			error("Error during multithread Indexing "+e);
		}
		//Cerramos el pool de ejecutores
		executor.shutdown();
		
		//Le damos 1h para terminar la tarea
		try {
			executor.awaitTermination(1, TimeUnit.HOURS);
		} 
		catch (final InterruptedException e) {
			error("Timeout during index creation: "+e);
			System.exit(-2);
		} 
		finally {
			//Fusionamos los indices temporales
			debug("Merging indexes into "+indexPath);
			try {
				for (FSDirectory tmp : directory_list) {
						mainWriter.addIndexes(tmp);
				}
			} catch (IOException e) {
				error("Error during indexes merge: "+e);
			}
		}
	}

	static void parseArguments(String[] args){
		String usage = 	"USAGE: java -jar IndexFiles [-openmode <APPEND | CREATE | APPEND_OR_CREATE>]" + 
		"[-onlyFiles] [-index INDEX_PATH] [-update] [-partialIndexes] "+
		"[-numThreads NUM_THREADS]";
		
		if (args[0]=="--help") System.out.println(usage);

		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-numThreads".equals(args[i])) {
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

		String ANSI_GREEN = "\u001B[32m";
		String ANSI_YELLOW = "\u001B[33m";

		System.out.println(ANSI_YELLOW+"**************************************************");
		System.out.println(ANSI_GREEN+"Launching IndexFiles with nThreads = "+ANSI_YELLOW+nThreads);
		System.out.println(ANSI_GREEN+"The indexPath is "+ANSI_YELLOW+indexPath);
		System.out.println(ANSI_GREEN+"The indexMode is "+ANSI_YELLOW+openMode);
		System.out.println(ANSI_GREEN+"Partial indexes enable value is "+ANSI_YELLOW+partialIndex);
		System.out.println(ANSI_GREEN+"File extension filtering value is "+ANSI_YELLOW+onlyFiles);
		System.out.println(ANSI_YELLOW+"**************************************************");
	}

	public static void main(String[] args) {
		parseArguments(args);
		readConfig("./IndexFiles.config");
		
		//Check if paths do exist
		for (Path p : docsDir){
			if (!Files.isReadable(p)) {
				error("Document directory '" + p.toAbsolutePath()
						+ "' does not exist or is not readable, please check the path");
				System.exit(-1);
			}
		}
		
		Date start = new Date();
		
		try {
			debug("Indexing to directory '" + indexPath + "'...");

			//Create main index writer
			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(openMode);
			IndexWriter mainWriter = new IndexWriter(dir, iwc);
			
			if (!partialIndex)
				for (Path p : docsDir)
					indexDocs(mainWriter, p);
			else
				indexMulti(mainWriter);

			mainWriter.commit();
			mainWriter.close();
			
		} 
		catch (IOException e) {
			error("Error during index creation: "+e);
		}

		Date end = new Date();
		debug(end.getTime() - start.getTime() + " total milliseconds");
	}
}