package utils;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * @author Juergen Boiselle
 * 
 * Parse an XML file and provide results in an event driven callback routine.
 */
public class XMLReaderService {
	/**
	 * @author Juergen Boiselle
	 *
	 * Implement this interface to get notified whenever the XML parser has finished reading a tag or attribute.
	 */
	public interface XMLReaderCallback {
		/**
		 * Called whenever the parser has finished reading a tag or an attribute.
		 * @param includeParent is the local name of the element that was in the include list.
		 * @param includeCount is the count of includeParent to separate parents with same name.
		 * @param fullPath is the full path that is compared against the regular expressions in include and exclude list.
		 * @param uri of the tag or attribute
		 * @param localName of the tag or attribute
		 * @param qName of the tag or attribute
		 * @param type is an 'A' for attribute or 'T' for tag.
		 * @param value of the tag within <tag>value</tag> or attribute like attribute="value".
		 * @param id as a unique identifier of this entry within the XML document.
		 * @param first_sub_id is the id of the first tag or attribute that is below this node.
		 * 		This column is useful to select parts in the XML.
		 * 		To select all tags and attributes that are contained in this use: <code>where child.id between this.first_sub_id and this.id-1</code>
		 * 		To select the next tag or attribute on the same level as this use: <code>where next.first_sub_id-1 = this.id</code>
		 * 		If this has no children first_sub_id returns id itself.
		 */
		public void newRow(String includeParent, int includeCount, String fullPath,
				String uri, String localName, String qName, String type, String value, int id, int first_sub_id);
	}
	
	private XMLReader xmlReader;
	private XMLReaderCallback xmlReaderCallback;
	private InExcluder inExcluder;

	// Handle parser events and call when new rows were detected.
	private ContentHandler contentHandler = new ContentHandler() {
		private StringBuilder cdata = new StringBuilder(); // Contains the current value of a tag
		private int id = 0;
		private Stack<Integer> stack = new Stack<Integer>();

		public void startDocument() throws SAXException {
			cdata.setLength(0);
			id = 0;
			stack.clear();
			inExcluder.clear();
		}

		public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
			inExcluder.enterNode(localName);
			stack.push(id); // Put on stack for reuse id at end of element as first
			cdata.setLength(0);
			if(atts == null) return; // I've seen many implementations...
			
			// Write out attributes as new rows
			for(int i = 0; i < atts.getLength(); i++) {
				inExcluder.enterNode(atts.getLocalName(i));
				if(inExcluder.isIncluded() && inExcluder.matchesLocalname())
					xmlReaderCallback.newRow(inExcluder.getIncludeParent(), inExcluder.getIncludeCount(), inExcluder.getFullPath(),
							atts.getURI(i), atts.getLocalName(i), atts.getQName(i), "A", atts.getValue(i), id+i, id+i);
				exitNode();
			}
			id += atts.getLength();
		}

		public void characters(char[] ch, int start, int length) throws SAXException {
			cdata.append(ch, start, length);
		}

		public void endElement(String uri, String localName, String qName) throws SAXException {
			int first_sub_id = stack.pop(); // pop id anyway
			if(inExcluder.isIncluded() && inExcluder.matchesLocalname())
				xmlReaderCallback.newRow(inExcluder.getIncludeParent(), inExcluder.getIncludeCount(), inExcluder.getFullPath(),
						uri, localName, qName, "T", cdata.toString(), id, first_sub_id);
			cdata.setLength(0);
			id++;
			exitNode();
		}

		// All other events are ignored
		public void endDocument() throws SAXException {}
		public void endPrefixMapping(String prefix) throws SAXException {}
		public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}
		public void processingInstruction(String target, String data) throws SAXException {}
		public void setDocumentLocator(Locator locator) {}
		public void skippedEntity(String name) throws SAXException {}
		public void startPrefixMapping(String prefix, String uri) throws SAXException {}
		
		// Unbalanced XML is detected when exit is called more often then enter
		private void exitNode() throws SAXException {
			if(inExcluder.isSkipped()) throw new SAXException("skipped"); // The documented way to interrupt parsing
			try {
				inExcluder.exitNode();
			}
			catch(EmptyStackException e) {
				throw new SAXException("XML unbalanced");
			}
		}
	};
	
	public XMLReaderService(List<String> included, List<String> excluded, List<String> skip, List<String> localnames) throws SAXException {
		inExcluder = new InExcluder(included, excluded, skip, localnames);

		xmlReader = XMLReaderFactory.createXMLReader();
		xmlReader.setContentHandler(contentHandler);
	}
	
	public void parse(Reader reader, XMLReaderCallback xmlReaderCallback) throws SAXException, IOException {
		this.xmlReaderCallback = xmlReaderCallback;
		try {
			xmlReader.parse(new InputSource(new NonClosingReader(reader)));
		} catch (SAXException e) {
			if(!e.getMessage().equals("skipped")) throw e; // Do nothing when skipped
		}
	}
	
	private class NonClosingReader extends Reader {
		private Reader reader;

		public NonClosingReader(Reader reader) {this.reader = reader;}
		public void close() throws IOException {} // reader.close(); Do not close this reader
		
		// replace invalid XML chars
		public int read() throws IOException {
			int ret = reader.read();
			return (ret == -1)? -1:Utils.replaceInvalid((char) ret);
		}
		
		// replace invalid XML chars
		public int read(char[] b, int off, int len) throws IOException {
			int ret = reader.read(b, off, len);
			if(ret != -1) {
				for(int i = 0; i < len; i++) b[off+i] = Utils.replaceInvalid(b[off+i]);
			}
			return ret;
		}
		
		public int read(char[] b) throws IOException {return read(b, 0, b.length);}
		public int read(CharBuffer b) throws IOException {return read(b.array());}
		
		public boolean equals(Object arg0) {return reader.equals(arg0);}
		public int hashCode() {return reader.hashCode();}
		public void mark(int arg0) throws IOException {reader.mark(arg0);}
		public boolean markSupported() {return reader.markSupported();}
		public boolean ready() throws IOException {return reader.ready();}
		public void reset() throws IOException {reader.reset();}
		public long skip(long arg0) throws IOException {return reader.skip(arg0);}
		public String toString() {return reader.toString();}
	}
}
