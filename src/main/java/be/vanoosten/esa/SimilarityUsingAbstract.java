package be.vanoosten.esa;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.helpers.DefaultHandler;
import java.util.HashSet;

/**
 *
 * @author Philip van Oosten
 */
public class SimilarityUsingAbstract extends DefaultHandler implements AutoCloseable {

    private final SAXParserFactory saxFactory;
    private boolean inPage;
    private boolean inPageTitle;
    private boolean inPageText;
    private StringBuilder content = new StringBuilder();
    private String wikiTitle;
    private int numIndexed = 0;
    private int numTotal = 0;

    public static final String TEXT_FIELD = "text";
    public static final String TITLE_FIELD = "title";
    Pattern pat;

    IndexWriter indexWriter;

    int minimumArticleLength;

    private Boolean show_text = false;
    private HashSet<String> articlesToSearch;
    private int WORDS_TO_KEEP = 100;
    private Map<String, String> articleMap = new HashMap<String, String>();
    private String targetTitle = "";
    private String dirToWrite = "";

    /**
     * Gets the minimum length of an article in characters that should be
     * indexed.
     *
     * @return
     */
    public int getMinimumArticleLength() {
        return minimumArticleLength;
    }

    /**
     * Sets the minimum length of an article in characters for it to be indexed.
     *
     * @param minimumArticleLength
     */
    public final void setMinimumArticleLength(int minimumArticleLength) {
        this.minimumArticleLength = minimumArticleLength;
    }

    public SimilarityUsingAbstract(String title, String dir, HashSet<String> articlesToSearchSet) throws IOException, SAXNotSupportedException, SAXNotRecognizedException, ParserConfigurationException {
        targetTitle = title;
        dirToWrite = dir;

        articlesToSearch = articlesToSearchSet;

        saxFactory = SAXParserFactory.newInstance();
        saxFactory.setNamespaceAware(true);
        saxFactory.setValidating(true);
        saxFactory.setXIncludeAware(true);
        saxFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false);

        //IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_48, analyzer);
        //indexWriter = new IndexWriter(directory, indexWriterConfig);
        String regex = "^[a-zA-z]+:.*";
        pat = Pattern.compile(regex);
        setMinimumArticleLength(2000);
    }

    public void parseXmlDump(String path) {
        parseXmlDump(new File(path));
    }

    public void parseXmlDump(File file) {
        try {
            SAXParser saxParser = saxFactory.newSAXParser();
            InputStream wikiInputStream = new FileInputStream(file);
            wikiInputStream = new BufferedInputStream(wikiInputStream);
            wikiInputStream = new BZip2CompressorInputStream(wikiInputStream, true);
            saxParser.parse(wikiInputStream, this);
        } catch (ParserConfigurationException | SAXException | FileNotFoundException ex) {
            Logger.getLogger(WikiIndexer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(WikiIndexer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if ("page".equals(localName)) {
            inPage = true;
        } else if (inPage && "title".equals(localName)) {
            inPageTitle = true;
            content = new StringBuilder();
        } else if (inPage && "text".equals(localName)) {
            inPageText = true;
            content = new StringBuilder();
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (inPage && inPageTitle && "title".equals(localName)) {
            inPageTitle = false;
            wikiTitle = content.toString();
            if(articlesToSearch.contains(wikiTitle)) {
                show_text = true;
                articlesToSearch.remove(wikiTitle);
		System.out.println(String.valueOf(articlesToSearch.size())+" target articles left to go");
            }
            //System.out.println("here:"+content.toString());
        } else if (inPage && inPageText && "text".equals(localName)) {
            inPageText = false;
            String wikiText = content.toString();
            try {
                numTotal++;
                if (index(wikiTitle, wikiText)) {
                    numIndexed++;
                    if (numIndexed % 100000 == 0) {
                        System.out.println("" + numIndexed + "\t/ " + numTotal + "\t" + wikiTitle);
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            if(show_text) {
                // Keep only the first WORDS_TO_KEEP from the complete text
                String[] words = wikiText.split(" ");
                wikiText = "";
		int word_count = WORDS_TO_KEEP;
                if(words.length < WORDS_TO_KEEP)
                    word_count = words.length;
                for (int i = 0; i < word_count; i++) { wikiText = wikiText + " " + words[i];}
                System.out.println(wikiText);
                show_text = false;

                // Assign different key for the target article
                if(wikiTitle.equals(targetTitle))
                    articleMap.put("targetTitle", wikiText);
                else
                    articleMap.put(wikiTitle, wikiText);
                if(articlesToSearch.isEmpty()) {
                    try {
                        FileOutputStream fos = new FileOutputStream(dirToWrite);
                        ObjectOutputStream oos = new ObjectOutputStream(fos);
                        oos.writeObject(articleMap);
                        oos.close();
                        fos.close();
                    }
                    catch(IOException ioe)
                    {
                        ioe.printStackTrace();
                    }
                    throw new SAXException("\nProcessing is done");
                }
            }
        } else if (inPage && "page".equals(localName)) {
            inPage = false;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        content.append(ch, start, length);
    }

    boolean index(String title, String wikiText) throws IOException {
//        Matcher matcher = pat.matcher(title);
//        if (matcher.find() || title.startsWith("Lijst van ") || wikiText.length() < getMinimumArticleLength()) {
//            return false;
//        }
//        Document doc = new Document();
//        doc.add(new StoredField(TITLE_FIELD, title));
//        Analyzer analyzer = indexWriter.getAnalyzer();
//        doc.add(new TextField(TEXT_FIELD, wikiText, Field.Store.NO));
//        indexWriter.addDocument(doc);
        return true;
    }

    @Override
    public void close() throws IOException {
        //indexWriter.close();
    }

}
