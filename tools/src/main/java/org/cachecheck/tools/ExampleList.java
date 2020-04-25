package org.cachecheck.tools;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class ExampleList {
	
	private ArrayList<String> caseNames;
	public ExampleList(String exampleList) throws DocumentException {
		caseNames = new ArrayList<String>();
		SAXReader saxReader = new SAXReader();
		File xmlFile = new File(exampleList);
		Document document = saxReader.read(xmlFile);
		Element root = document.getRootElement();
		List<Element> modules = root.elements();
		for (Element module : modules) {
			List<Attribute> moduleAttr = module.attributes();
			String moduleName = "";
			for(Attribute attr: moduleAttr) {
				if(attr.getName() == "name")
					moduleName = attr.getValue();
			}
			List<Element> cases = module.elements();
			for (Element app : cases) {
				String appName = app.getTextTrim();
				if(moduleName.equals(""))
					caseNames.add(appName);
				else
					caseNames.add(moduleName+"."+appName);
			}
		}
	}
	
	public void printList() {
		for(String app: caseNames) 
			System.out.println(app);
	}
	
	public ArrayList<String> getList() {
		return caseNames;
	}
}
