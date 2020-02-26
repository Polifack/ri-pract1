package es.udc.fi.ri.pract1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

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

	private IndexFiles() {
	}

	private static String readFile(String fileName) throws IOException{
		String line = null;
		String result = "";
		BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));

		int i = 0;
			while (((line = bufferedReader.readLine()) != null) && i < 5) {
				result +=line+"\n";
				i++;
			}
		bufferedReader.close();
		return result;
	}
	
	private static String readFileBack(String fileName) throws IOException{
		String result = "";
        BufferedReader br = new BufferedReader(
        		new InputStreamReader(new FileInputStream(fileName)));
		List<String> lines = new LinkedList<String>();
		
		//Eliminamos la primera linea de la lista hasta que haya solo 5 lineas
		for(String tmp; (tmp = br.readLine()) != null;) 
		    if (lines.add(tmp) && lines.size() > 5) 
		        lines.remove(0);
		
		for (String line : lines) {
			result += line+"\n";
		}
		
		br.close();
		return result;
		
	}

	static void indexDocs(final IndexWriter writer, Path path) throws IOException {
		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					try {
						indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
					} catch (IOException ignore) {
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
		}
	}

	static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
		try (InputStream stream = Files.newInputStream(file)) {
			Document doc = new Document();
			
			//Path del archivo
			doc.add(new StringField("path", file.toString(), Field.Store.YES));
			//Fecha de modificacion
			doc.add(new LongPoint("modified", lastModified));
			//Contenidos totales del archivo
			doc.add(new TextField("contents",new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
			//Hostname del indexer
			doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
			//Thread que indexa
			doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
			//Tamaño en kbs
			doc.add(new DoublePoint("sizeKb", (double) (new File(file.toString()).length() / 1024)));
			//Primeras 5 lineas
			doc.add(new TextField("top5Lines", readFile(file.toString()), Field.Store.YES)) ;
			//Ultimas 5 lineas
			doc.add(new TextField("bottom5Lines", readFileBack(file.toString()), Field.Store.YES)) ;

			if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
				System.out.println("adding " + file);
				writer.addDocument(doc);
			} else {
				System.out.println("updating " + file);
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
		}
	}

	public static void main(String[] args) {
		String usage = "java -jar IndexFiles" + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
				+ "This indexes the documents in DOCS_PATH, creating a Lucene index"
				+ "in INDEX_PATH that can be searched with SearchFiles";
		String indexPath = "index";
		String docsPath = null;
		boolean create = true;
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-docs".equals(args[i])) {
				docsPath = args[i + 1];
				i++;
			} else if ("-update".equals(args[i])) {
				create = false;
			}
		}

		if (docsPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		final Path docDir = Paths.get(docsPath);
		if (!Files.isReadable(docDir)) {
			System.out.println("Document directory '" + docDir.toAbsolutePath()
					+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}

		Date start = new Date();
		try {
			System.out.println("Indexing to directory '" + indexPath + "'...");

			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

			if (create) {
				iwc.setOpenMode(OpenMode.CREATE);
			} else {
				// Add new documents to an existing index:
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			}

			IndexWriter writer = new IndexWriter(dir, iwc);
			indexDocs(writer, docDir);

			writer.close();

			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
	}

}