package etl.util;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.flow.oozie.wf.WORKFLOWAPP;

public class JaxbUtil {
	
	public static final Logger logger = LogManager.getLogger(JaxbUtil.class);
	
	public static void marshal(WORKFLOWAPP wfa, String outputfile){
		try {
			File file = new File(outputfile);
			JAXBContext jaxbContext = JAXBContext.newInstance(WORKFLOWAPP.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			QName qName = new QName("workflow-app");
		    JAXBElement<WORKFLOWAPP> root = new JAXBElement<WORKFLOWAPP>(qName, WORKFLOWAPP.class, wfa);
			jaxbMarshaller.marshal(root, file);
		} catch (JAXBException e) {
			logger.error("", e);
		}
	}
}
