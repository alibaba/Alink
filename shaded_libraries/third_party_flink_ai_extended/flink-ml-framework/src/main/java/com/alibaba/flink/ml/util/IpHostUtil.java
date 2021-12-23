/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;
import java.util.Random;

/**
 * machine ip address and port helper function
 */
public class IpHostUtil {
	private static Logger LOG = LoggerFactory.getLogger(IpHostUtil.class);

	private static InetAddress getLocalHostLANAddress() throws Exception {
		try {
			InetAddress candidateIpv4 = null;
			InetAddress candidateIpv6 = null;
			for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
				NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
				for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
					InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
					if (!inetAddr.isLoopbackAddress()) {
						if (inetAddr.isSiteLocalAddress()) {
							return inetAddr;
						}
						if (inetAddr instanceof Inet4Address && candidateIpv4 == null) {
							//ip v4
							candidateIpv4 = inetAddr;
						} else if (inetAddr instanceof Inet6Address && candidateIpv6 == null) {
							//ip v6
							candidateIpv6 = inetAddr;
						}
					}
				}
			}
			if (candidateIpv4 != null) {
				return candidateIpv4;
			}
			if (candidateIpv6 != null) {
				return candidateIpv6;
			}
		} catch (Exception e) {
			LOG.error("Fail to get local ip. ", e);
		}

		try {
			InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
			return jdkSuppliedAddress;
		} catch (Exception e) {
			LOG.error("Fail to get local ip from jdk. ", e);
		}
		return null;
	}

	private static String getHostName() throws UnknownHostException {
		InetAddress address = InetAddress.getLocalHost();
		String hostName = address.getHostName();
		return hostName;
	}

	private static String getLocalIp() throws Exception {
		InetAddress address = InetAddress.getLocalHost();
		String ip = address.getHostAddress();
		return ip;
	}

	/**
	 * @return local ip address.
	 * @throws Exception
	 */
	public static String getIpAddress() throws Exception {
		try {
			return getLocalIp();
		} catch (Exception e) {
			InetAddress inetAddress = getLocalHostLANAddress();
			if (null == inetAddress) {
				return null;
			} else {
				return inetAddress.getHostAddress();
			}
		}
	}


	/**
	 * @return Gets a free port and create a ServerSocket bound to this port.
	 */
	public static ServerSocket getFreeSocket() {
		int MINPORT = 20000;
		int MAXPORT = 65535;
		Random rand = new Random();
		int i = 0;
		while (true) {
			try {
				i = rand.nextInt(MAXPORT - MINPORT) + MINPORT;
				return new ServerSocket(i);
			} catch (Exception e) {
				System.out.println("port:" + i + " in use");
			}
		}
	}

	/**
	 * @return a free port.
	 * @throws IOException
	 */
	public static int getFreePort() throws IOException {
		try (ServerSocket socket = getFreeSocket()) {
			return socket.getLocalPort();
		}
	}
}
