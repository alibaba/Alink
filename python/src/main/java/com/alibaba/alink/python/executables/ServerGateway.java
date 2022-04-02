package com.alibaba.alink.python.executables;

import org.apache.flink.api.java.utils.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;
import py4j.GatewayServerListener;
import py4j.Py4JServerConnection;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Properties;

/**
 * Start a Py4j Java gateway server so the Python process can connect to it.
 * <p>
 * Note: right now, this class is mainly for dev purpose, because the Java gateway server is started by the Python
 * process.
 */
public class ServerGateway {

	private static final Logger LOG = LoggerFactory.getLogger(ServerGateway.class);

	public static String getConfigValue(Properties p, String key, String val) {
		if (p.containsKey(key)) {
			return p.getProperty(key);
		}
		if (System.getenv().containsKey(key)) {
			return System.getenv(key);
		}
		return val;
	}

	public static GatewayServer startGateway(String[] args) throws Exception {
		System.setProperty("alink.direct.read.local", "true");
		System.setProperty("direct.read.local", "true");
		System.setProperty("direct.reader.local", "true");
		System.setProperty("direct.reader.policy", "memory");

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		Properties properties = System.getProperties();

		if (parameterTool.has("configFile")) {
			try (FileInputStream fin = new FileInputStream(parameterTool.get("configFile"))) {
				Properties p = new Properties();
				p.load(fin);
				properties = p;
			}
		}

		final boolean turnOnGc = Boolean.parseBoolean(getConfigValue(properties, "_ALINK_AUTO_GC_", "false"));
		final int port = Integer.parseInt(getConfigValue(properties, "ALINK_PY4J_JAVA_PORT", "9999"));

		InetAddress localhost = InetAddress.getLoopbackAddress();
		GatewayServer gatewayServer = new GatewayServer.GatewayServerBuilder()
			.javaPort(port)
			.javaAddress(localhost)
			.build();
		gatewayServer.addListener(new GatewayServerListener() {
			@Override
			public void connectionError(Exception e) {
				LOG.info("connection error", e);
			}

			@Override
			public void connectionStarted(Py4JServerConnection gatewayConnection) {
				if (turnOnGc) {
					System.gc();
				}
				System.out.println("start one connection to port " + gatewayConnection.getSocket().getPort());
				LOG.info("start one connection to port {}", gatewayConnection.getSocket().getPort());
			}

			@Override
			public void connectionStopped(Py4JServerConnection gatewayConnection) {
				if (turnOnGc) {
					System.gc();
				}
				LOG.info("stop one connection to port {}", gatewayConnection.getSocket().getPort());
			}

			@Override
			public void serverError(Exception e) {
				LOG.info("server error", e);
			}

			@Override
			public void serverPostShutdown() {
			}

			@Override
			public void serverPreShutdown() {
			}

			@Override
			public void serverStarted() {
				System.out.println(">>>>> serverStarted");
			}

			@Override
			public void serverStopped() {
			}
		});
		gatewayServer.start();

		final int boundPort = gatewayServer.getListeningPort();
		if (boundPort == -1) {
			throw new RuntimeException("GatewayServer failed to bind");
		} else {
			System.out.println("GatewayServer listening on port " + boundPort);
		}

		// Tells python side the port of our java rpc server
		String handshakeFilePath = System.getenv("_PYALINK_CONN_INFO_PATH");
		if (handshakeFilePath != null) {
			File handshakeFile = new File(handshakeFilePath);
			File tmpPath = Files.createTempFile(handshakeFile.getParentFile().toPath(),
				"connection", ".info").toFile();
			FileOutputStream fileOutputStream = new FileOutputStream(tmpPath);
			DataOutputStream stream = new DataOutputStream(fileOutputStream);
			stream.writeInt(boundPort);
			stream.close();
			fileOutputStream.close();

			if (!tmpPath.renameTo(handshakeFile)) {
				throw new RuntimeException(
					"Unable to write connection information to handshake file: " + handshakeFilePath);
			}
		}
		return gatewayServer;
	}

	public static void main(String[] args) throws Exception {
		GatewayServer server = startGateway(args);
		try {
			// Exit on EOF or broken pipe.  This ensures that the server dies
			// if its parent program dies.
			//noinspection StatementWithEmptyBody
			while (System.in.read() != -1) {}
			server.shutdown();
			System.exit(0);
		} finally {
			System.exit(1);
		}
	}
}
