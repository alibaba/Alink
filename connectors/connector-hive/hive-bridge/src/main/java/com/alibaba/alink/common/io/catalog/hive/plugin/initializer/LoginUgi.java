package com.alibaba.alink.common.io.catalog.hive.plugin.initializer;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class LoginUgi implements MapFunction<PrivilegedExceptionAction<Object>, Object> {

    private final UserGroupInformation ugi;

    public LoginUgi(String kerberosPrincipal, String kerberosKeytab) throws IOException {
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, kerberosKeytab);
    }

    @Override
    public Object map(PrivilegedExceptionAction<Object> objectPrivilegedExceptionAction) throws Exception {
        return ugi.doAs(objectPrivilegedExceptionAction);
    }
}
