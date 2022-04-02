package com.alibaba.alink.common.io.catalog;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.LocalFileSystem;
import com.alibaba.alink.operator.common.io.reader.HttpFileSplitReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class HiveBaseUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HiveBaseUtils.class);

	public static boolean fileExists(FilePath folder, String file) throws IOException {
		// local
		if (folder.getFileSystem() instanceof LocalFileSystem) {
			return folder.getFileSystem().exists(new Path(folder.getPath(), file));
		}

		String scheme = folder.getPath().toUri().getScheme();

		if (scheme != null && (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
			try (HttpFileSplitReader reader = new HttpFileSplitReader(folder.getPathStr() + "/" + file)) {
				long fileLen = reader.getFileLength();
				reader.open(null, 0, fileLen);
			} catch (FileNotFoundException exception) {
				return false;
			}

			return true;
		} else {
			return folder.getFileSystem().exists(new Path(folder.getPath(), file));
		}
	}

	public static String readFile(FilePath filePath) throws IOException {
		String scheme = filePath.getPath().toUri().getScheme();
		if (scheme != null && (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
			try (HttpFileSplitReader reader = new HttpFileSplitReader(filePath.toString())) {
				long fileLen = reader.getFileLength();
				reader.open(null, 0, fileLen);

				int len = (int) reader.getFileLength();

				byte[] buffer = new byte[len];
				reader.read(buffer, 0, len);

				return new String(buffer, StandardCharsets.UTF_8);
			}
		} else {
			try (FSDataInputStream inputStream = filePath.getFileSystem().open(filePath.getPath())) {
				return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			}
		}
	}

	public static String downloadFolder(FilePath folder, String... files) throws IOException {
		// local
		if (folder.getFileSystem() instanceof LocalFileSystem) {
			return folder.getPathStr();
		}

		File localConfDir = new File(System.getProperty("java.io.tmpdir"), FileUtils.getRandomFilename(""));
		String scheme = folder.getPath().toUri().getScheme();

		if (!localConfDir.mkdir()) {
			throw new RuntimeException("Could not create the dir " + localConfDir.getAbsolutePath());
		}

		if (scheme != null && (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
			for (String path : files) {
				try (HttpFileSplitReader reader = new HttpFileSplitReader(folder.getPathStr() + "/" + path)) {
					long fileLen = reader.getFileLength();
					reader.open(null, 0, fileLen);

					int offset = 0;
					byte[] buffer = new byte[1024];

					try (FileOutputStream outputStream = new FileOutputStream(
						Paths.get(localConfDir.getPath(), path).toFile())) {
						while (offset < fileLen) {
							int len = reader.read(buffer, offset, 1024);
							outputStream.write(buffer, offset, len);
							offset += len;
						}
					}

				} catch (FileNotFoundException exception) {
					// pass
				}
			}
		} else {
			for (String path : files) {
				// file system
				if (!folder.getFileSystem().exists(new Path(folder.getPath(), path))) {
					continue;
				}

				try (FSDataInputStream inputStream = folder.getFileSystem().open(
					new Path(folder.getPath(), path));
					 FileOutputStream outputStream = new FileOutputStream(
						 Paths.get(localConfDir.getPath(), path).toFile())) {
					IOUtils.copy(inputStream, outputStream);
				}
			}
		}

		return localConfDir.getAbsolutePath();
	}

	public static String downloadHiveConf(FilePath hiveConfDir) throws IOException {
		return downloadFolder(hiveConfDir, "hive-site.xml");
	}

	public static Map <String, String> getStaticPartitionSpec(String partitionSpec) {
		Map <String, String> spec = new HashMap <>();
		if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
			String[] partitions = partitionSpec.split("/");
			for (String p : partitions) {
				int pos = p.indexOf('=');
				Preconditions.checkArgument(pos > 0);
				String col = p.substring(0, pos);
				String val = p.substring(pos + 1);
				spec.put(col, val);
			}
		}
		return spec;
	}

	public static void mergeHiveConf(FilePath hiveConfDir, String outputFile, String... xmlFiles) {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;

		try {
			builder = factory.newDocumentBuilder();
		} catch (ParserConfigurationException ex) {
			throw new IllegalStateException(ex);
		}

		Document merged = null;
		Set <String> tagNames = new HashSet <>();

		for (String xmlFile : xmlFiles) {
			try {
				Document document = builder.parse(new Path(hiveConfDir.getPath(), xmlFile).toString());

				Element rootElement = document.getDocumentElement();

				Preconditions.checkState(rootElement.getNodeName().trim().equalsIgnoreCase("configuration"));

				if (merged == null) {
					merged = document;

					NodeList childNodes = rootElement.getChildNodes();

					for (int i = 0; i < childNodes.getLength(); ++i) {
						Node property = childNodes.item(i);

						if (property != null && property.getNodeType() == Node.ELEMENT_NODE) {

							NodeList nameAndValues = property.getChildNodes();

							for (int j = 0; j < nameAndValues.getLength(); ++j) {
								Node nameAndValue = nameAndValues.item(j);

								if (nameAndValue != null
									&& nameAndValue.getNodeType() == Node.ELEMENT_NODE
									&& nameAndValue.getNodeName() != null
									&& nameAndValue.getNodeName().equalsIgnoreCase("name")) {

									tagNames.add(nameAndValue.getTextContent().trim());
								}
							}
						}
					}
				} else {
					Element element = merged.getDocumentElement();

					NodeList childNodes = rootElement.getChildNodes();

					for (int i = 0; i < childNodes.getLength(); ++i) {
						Node property = childNodes.item(i);

						if (property != null && property.getNodeType() == Node.ELEMENT_NODE) {

							NodeList nameAndValues = property.getChildNodes();

							for (int j = 0; j < nameAndValues.getLength(); ++j) {

								Node nameAndValue = nameAndValues.item(j);

								if (nameAndValue != null
									&& nameAndValue.getNodeType() == Node.ELEMENT_NODE
									&& nameAndValue.getNodeName() != null
									&& nameAndValue.getNodeName().equalsIgnoreCase("name")
									&& nameAndValue.getTextContent() != null
									&& !tagNames.contains(nameAndValue.getTextContent().trim())) {

									element.appendChild(merged.importNode(property, true));
									tagNames.add(nameAndValue.getTextContent().trim());
									break;
								}
							}
						}
					}
				}

			} catch (SAXException | IOException e) {
				LOG.info(String.format("Load xml %s fail.", xmlFile), e);
			}
		}

		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = null;
		try {
			transformer = transformerFactory.newTransformer();
		} catch (TransformerConfigurationException e) {
			throw new IllegalStateException(e);
		}

		DOMSource domSource = new DOMSource(merged);
		Result result = new StreamResult(new Path(hiveConfDir.getPath(), outputFile).toString());

		try {
			transformer.transform(domSource, result);
		} catch (TransformerException e) {
			throw new IllegalStateException(e);
		}
	}

	public static List <Map <String, String>> getSelectedPartitions(String[] partitionSpecs) {
		List <Map <String, String>> selected = new ArrayList <>();
		for (String s : partitionSpecs) {
			Map <String, String> spec = getStaticPartitionSpec(s);
			selected.add(spec);
		}
		return selected;
	}

	/**
	 * Structure of hive conf folder.
	 * <p>hive-conf/
	 * <p> |--krb5.conf      # configure of kdc
	 * <p> |--user.keytab    # user kerberos keytab
	 * <p> |--user.name      # user kerberos name
	 * <p> |--hive-site.xml  # hive configure
	 * <p> |--core-site.xml  # hadoop core configure
	 * <p> |--hdfs-site.xml  # hdfs configure
	 */
	public static class HiveConfFolderStructure implements Serializable {
		private static final String KEYTAB_FILE_NAME = "user.keytab";
		private static final String PRINCIPAL_FILE_NAME = "user.name";
		private static final long serialVersionUID = -608927465621306330L;

		private final FilePath folder;

		public HiveConfFolderStructure(FilePath folder) {
			this.folder = folder;
		}

		public String getKerberosPrincipal() throws IOException {
			if (fileExists(folder, PRINCIPAL_FILE_NAME)) {
				return readFile(new FilePath(new Path(folder.getPath(), PRINCIPAL_FILE_NAME), folder.getFileSystem()));
			} else {
				return null;
			}
		}

		public FilePath getKerberosKeytabPath() throws IOException {
			if (fileExists(folder, KEYTAB_FILE_NAME)) {
				return new FilePath(new Path(folder.getPath(), KEYTAB_FILE_NAME), folder.getFileSystem());
			} else {
				return null;
			}
		}
	}
}
