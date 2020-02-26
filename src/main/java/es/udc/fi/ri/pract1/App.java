package es.udc.fi.ri.pract1;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import es.udc.fi.ri.pract1.IndexFiles;
import es.udc.fi.ri.pract1.IndexFilesThreadPool;
import es.udc.fi.ri.pract1.SearchFiles;

public class App {

	public static void main(String[] args) throws Exception {
		Analyzer analyzer = new StandardAnalyzer();
        
        System.out.println("Hello world from App");
        IndexFiles.main(args);

	}

}
