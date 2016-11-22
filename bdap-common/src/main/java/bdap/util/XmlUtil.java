package bdap.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TreeMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

public class XmlUtil {
	public static final Logger logger = LogManager.getLogger(XmlUtil.class);

	public static String beanToXML(Object obj) {
		String xml = "";
		try {
			JAXBContext context = JAXBContext.newInstance(obj.getClass());
			Marshaller marshaller = context.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
	        Writer w = new StringWriter();
	        marshaller.marshal(obj, w);
	        xml = w.toString();
		} catch (JAXBException e) {
			logger.error("", e);
		}
		return xml;
	}

	public static <T> T xmlToBean(String xmlStr, Class<T> clazz) {
		T obj = null;
		try {
			JAXBContext context = JAXBContext.newInstance(clazz);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			obj = (T) unmarshaller.unmarshal(new StringReader(xmlStr));
		} catch (JAXBException e) {
			logger.error("", e);
		}
		return obj;
	}
	
	public static <T> void marshal(T object, String qname, String outputfile){
		try {
			File file = new File(outputfile);
			JAXBContext jaxbContext = JAXBContext.newInstance(object.getClass());
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			QName qName = new QName(qname);
		    JAXBElement<T> root = new JAXBElement<T>(qName, (Class<T>) object.getClass(), object);
			jaxbMarshaller.marshal(root, file);
		} catch (JAXBException e) {
			logger.error("", e);
		}
	}
	
	public static <T> byte[] marshalToBytes(T object, String qname){
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(object.getClass());
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			QName qName = new QName(qname);
		    JAXBElement<T> root = new JAXBElement<T>(qName, (Class<T>) object.getClass(), object);
		    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			jaxbMarshaller.marshal(root, outputStream);
			return outputStream.toByteArray();
		} catch (JAXBException e) {
			logger.error("", e);
			return null;
		}
	}
	
	public static <T> String marshalToString(T object, String qname){
		byte[] content = marshalToBytes(object, qname);
		if (content!=null){
			return new String(content);
		}else{
			return null;
		}
	}
	
	public static Document getDocument(String inputXml){
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			InputSource input = new InputSource(new StringReader(inputXml));
			Document doc = builder.parse(input);
			return doc;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public static Document getDocument(FileSystem fs, Path inputXml){
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			InputSource input = new InputSource(new BufferedReader(new InputStreamReader(fs.open(inputXml))));
			Document doc = builder.parse(input);
			return doc;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public static Document getDocumentFromLocalFile(String localFile){
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			InputSource input = new InputSource(Files.newBufferedReader(Paths.get(localFile), Charset.defaultCharset()));
			Document doc = builder.parse(input);
			return doc;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
}
