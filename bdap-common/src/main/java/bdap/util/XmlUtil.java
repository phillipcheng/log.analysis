package bdap.util;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
}
