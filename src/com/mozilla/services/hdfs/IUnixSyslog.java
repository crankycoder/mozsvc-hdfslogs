package com.mozilla.services.hdfs;

import com.sun.jna.Library;

public interface IUnixSyslog  extends Library {
    public void  openlog(final String ident, int option, int facility);
    public void  syslog(int priority, final String format, final String message);
    public void  closelog();
}
