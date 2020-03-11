package es.udc.fi.ri.pract1;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class SimilarDocs {

	/**
	 * SimilarDocs con argumentos -index, -rep, -docID y -field. 
	 * Para el documento con docID de Lucene que se le pasa como valor a -docID y 
	 * campo que se le pasacomo valor a -field, y usando las representación pasada 
	 * como valor a -rep, se obtienen y devuelve lalista de 10 documentos mas 
	 * similares, ordenados de mayor a menor similaridad, con respecto al quese 
	 * le pasa como argumento.
	 */

	public static void main(final String[] args) throws IOException {

		if (args.length != 1) {
			System.out.println("Usage: java -jar SimilarDocs <index_folder>");
			return;
		}

		Directory dir = null;
		DirectoryReader indexReader = null;

		try {
			dir = FSDirectory.open(Paths.get(args[0]));
			indexReader = DirectoryReader.open(dir);
		} catch (CorruptIndexException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}

		System.out.printf("%-20s%-10s%-25s%-10s%-80s\n", "TERM", "DOCID", "FIELD", "FREQ",
				"POSITIONS (-1 means No Positions indexed for this field)");

		final FieldInfos fieldinfos = FieldInfos.getMergedFieldInfos(indexReader);

		for (final FieldInfo fieldinfo : fieldinfos) {

			System.out.println("Field = " + fieldinfo.name);
			final Terms terms = MultiTerms.getTerms(indexReader, fieldinfo.name);
			if (terms != null) {
				final TermsEnum termsEnum = terms.iterator();

				while (termsEnum.next() != null) {

					String termString = termsEnum.term().utf8ToString();

					PostingsEnum posting = MultiTerms.getTermPostingsEnum(indexReader, fieldinfo.name,
							new BytesRef(termString));

					if (posting != null) { // if the term does not appear in any document, the posting object may be
											// null
						int docid;
						// Each time you call posting.nextDoc(), it moves the cursor of the posting list
						// to the next position
						// and returns the docid of the current entry (document). Note that this is an
						// internal Lucene docid.
						// It returns PostingsEnum.NO_MORE_DOCS if you have reached the end of the
						// posting list.
						while ((docid = posting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
							int freq = posting.freq(); // get the frequency of the term in the current document
							System.out.printf("%-20s%-10d%-25s%-10d", termString, docid, fieldinfo.name, freq);
							for (int i = 0; i < freq; i++) {
								// Get the next occurrence position of the term in the current document.
								// Note that you need to make sure by yourself that you at most call this
								// function freq() times.
								System.out.print((i > 0 ? "," : "") + posting.nextPosition());
							}
							System.out.println();
						}
					}

				}

			}
		}

		try {
			indexReader.close();
			dir.close();
		} catch (IOException e) {
			System.out.println("Graceful message: exception " + e);
			e.printStackTrace();
		}

	}

}