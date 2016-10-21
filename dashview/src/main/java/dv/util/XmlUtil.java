package dv.util;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import dv.tableau.rest.TsRequest;
import dv.tableau.rest.TsResponse;

/**
 * 
 * @author ximing
 *
 */
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

	public static Object XMLStringToBean(String xmlStr, Class clazz) {
		Object obj = null;
		try {
			JAXBContext context = JAXBContext.newInstance(clazz);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			obj = unmarshaller.unmarshal(new StringReader(xmlStr));
		} catch (JAXBException e) {
			logger.error("", e);
		}
		return obj;
	}
	

}
