package tests;


import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

import junit.framework.TestCase;

import org.xml.sax.SAXException;

import utils.XMLReaderService;
import utils.XMLReaderService.XMLReaderCallback;

public class XMLReaderServiceTest extends TestCase {
	private static final String firstXML = "" +
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
			"<root>" +
					"<A>" +
					"HAllo" +
					"</A>" +
					"<B a1=\"x\" a2=\"y\"/>" +
					"<B a1=\"1\" a2=\"2\">" +
					"<B1>Welt</B1>" +
					"</B>" +
			"</root>";

	private static final String secondXML = "" +
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
			"<root2>" +
					"<A2>" +
					"HAllo" +
					"</A2>" +
					"<B2 a1=\"x\" a2=\"y\"/>" +
					"<B2 a1=\"1\" a2=\"2\">" +
					"<B21>Welt</B21>" +
					"</B2>" +
			"</root2>";

	private static final String unbalancedXML = "" +
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
			"<root>" +
					"<A>" +
					"HAllo" +
					"</A>" +
					"<B a1=\"x\" a2=\"y\"/>" +
					"<B a1=\"1\" a2=\"2\">" +
					"<B1>Welt</B1>" +
			"</root>"; // misses a B at end

	private static final String wrongXML = "" +
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
			"<root>" +
					"<A" +
					"HAllo" +
					"</A>" +
					"<B a1=\"x\" a2=\"y\"/>" +
					"<B a1=\"1\" a2=\"2\">" +
					"<B1>Welt</B1>" +
					"</B>" +
			"</root>"; // misses a > after A
	
	public final void testXMLReaderService() throws SAXException, IOException {
		// check first well formed XML without in- and exclusion
		XMLReaderService xmlReaderService = new XMLReaderService(null, null, null, null);
		check("check first well formed XML without in- and exclusion", xmlReaderService, firstXML,
				new RowParameter("", "A", "A", "T", "HAllo", 0, 0),
				new RowParameter("", "a1", "a1", "A", "x", 1,1),
				new RowParameter("", "a2", "a2", "A", "y", 2,2),
				new RowParameter("", "B", "B", "T", "", 3, 1),
				new RowParameter("", "a1", "a1", "A", "1", 4,4),
				new RowParameter("", "a2", "a2", "A", "2", 5,5),
				new RowParameter("", "B1", "B1", "T", "Welt", 6,6),
				new RowParameter("", "B", "B", "T", "", 7,4),
				new RowParameter("", "root", "root", "T", "", 8, 0));
		
		// check second well formed XML without in- and exclusion
		check("check second well formed XML without in- and exclusion", xmlReaderService, secondXML,
				new RowParameter("", "A2", "A2", "T", "HAllo", 0, 0),
				new RowParameter("", "a1", "a1", "A", "x", 1,1),
				new RowParameter("", "a2", "a2", "A", "y", 2,2),
				new RowParameter("", "B2", "B2", "T", "", 3, 1),
				new RowParameter("", "a1", "a1", "A", "1", 4,4),
				new RowParameter("", "a2", "a2", "A", "2", 5,5),
				new RowParameter("", "B21", "B21", "T", "Welt", 6,6),
				new RowParameter("", "B2", "B2", "T", "", 7,4),
				new RowParameter("", "root2", "root2", "T", "", 8, 0));

		// check well formed XML with in- and exclusion
		xmlReaderService = new XMLReaderService(Arrays.asList(new String[] {"a2", "B"}), Arrays.asList(new String[] {"B1"}), null, null);
		check("check first well formed XML without in- and exclusion", xmlReaderService, firstXML,
				new RowParameter("", "a1", "a1", "A", "x", 1,1),
				new RowParameter("", "a2", "a2", "A", "y", 2,2),
				new RowParameter("", "B", "B", "T", "", 3, 1),
				new RowParameter("", "a1", "a1", "A", "1", 4,4),
				new RowParameter("", "a2", "a2", "A", "2", 5,5),
				new RowParameter("", "B", "B", "T", "", 7,4));

		// check unbalanced XML without in- and exclusion
		xmlReaderService = new XMLReaderService(null, null, null, null);
		boolean ok = false;
		try {
			check("check unbalanced XML without in- and exclusion", xmlReaderService, unbalancedXML,
					new RowParameter("", "A", "A", "T", "HAllo", 0, 0),
					new RowParameter("", "a1", "a1", "A", "x", 1,1),
					new RowParameter("", "a2", "a2", "A", "y", 2,2),
					new RowParameter("", "B", "B", "T", "", 3, 1),
					new RowParameter("", "a1", "a1", "A", "1", 4,4),
					new RowParameter("", "a2", "a2", "A", "2", 5,5),
					new RowParameter("", "B1", "B1", "T", "Welt", 6,6),
					new RowParameter("", "B", "B", "T", "", 7,4),
					new RowParameter("", "root", "root", "T", "", 8, 0));
		}
		catch(SAXException e) {
			System.out.println(e.getMessage());
			ok = true; // Thats OK behavior
		}
		assertTrue("check unbalanced XML without in- and exclusion", ok);
		
		// check syntactically wrong XML without in- and exclusion
		ok = false;
		try {
			check("check syntactically wrong XML without in- and exclusion", xmlReaderService, wrongXML,
					new RowParameter("", "A", "A", "T", "HAllo", 0, 0),
					new RowParameter("", "a1", "a1", "A", "x", 1,1),
					new RowParameter("", "a2", "a2", "A", "y", 2,2),
					new RowParameter("", "B", "B", "T", "", 3, 1),
					new RowParameter("", "a1", "a1", "A", "1", 4,4),
					new RowParameter("", "a2", "a2", "A", "2", 5,5),
					new RowParameter("", "B1", "B1", "T", "Welt", 6,6),
					new RowParameter("", "B", "B", "T", "", 7,4),
					new RowParameter("", "root", "root", "T", "", 8, 0));
		}
		catch(SAXException e) {
			System.out.println(e.getMessage());
			ok = true; // Thats OK behavior
		}
		assertTrue("check syntactically wrong XML without in- and exclusion", ok);

		// check well formed XML with skipping
		xmlReaderService = new XMLReaderService(null, null, Arrays.asList(new String[] {"a2"}), null);
		check("check well formed XML with skipping", xmlReaderService, firstXML,
				new RowParameter("", "A", "A", "T", "HAllo", 0, 0),
				new RowParameter("", "a1", "a1", "A", "x", 1,1),
				new RowParameter("", "a2", "a2", "A", "y", 2,2));
	}
	
	private int i = 0;
	private void check(final String msg, XMLReaderService xmlReaderService, String xml, final RowParameter... rowParameters) throws SAXException, IOException {
		i = 0;
		xmlReaderService.parse(new StringReader(xml), new XMLReaderCallback() {
			public void newRow(String includeParent, int includeCount, String fullPath,
					String uri, String localName, String qName, String type, String value, int id, int first_sub_id) {
				System.out.println(includeParent + ", " + includeCount + ", " + fullPath + ", " + uri + ", " + localName + ", " + qName + ", " + type + ", " + value + ", " + id + ", " + first_sub_id);
				rowParameters[i].check(i + ": " + msg, uri, localName, qName, type, value, id, first_sub_id);
				i++;
			}
		});
		assertEquals(msg, rowParameters.length, i);
	}
	
	private class RowParameter {
		private final String uri;
		private final String localName;
		private final String qName;
		private final String type;
		private final String value;
		private final int id;
		private final int first_sub_id;
		
		public RowParameter(String uri, String localName, String qName, String type, String value, int id, int first_sub_id) {
			this.uri = uri;
			this.localName = localName;
			this.qName = qName;
			this.type = type;
			this.value = value;
			this.id = id;
			this.first_sub_id = first_sub_id;
		}
		
		public void check(String msg, String uri, String localName, String qName, String type, String value, int id, int first_sub_id) {
			assertEquals(msg, this.uri, uri);
			assertEquals(msg, this.localName, localName);
			assertEquals(msg, this.qName, qName);
			assertEquals(msg, this.type, type);
			assertEquals(msg, this.value, value);
			assertEquals(msg, this.id, id);
			assertEquals(msg, this.first_sub_id, first_sub_id);
		}
	}
}
