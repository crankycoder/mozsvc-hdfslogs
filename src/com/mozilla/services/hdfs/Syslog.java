package com.mozilla.services.hdfs;

import com.sun.jna.Native;

/*
 * simple access to the syslog logger for error messages
 */
public class Syslog {

    private static IUnixSyslog _syslog;

    public static int EMERGENCY = 0;
    public static int ALERT = 1;
    public static int CRITICAL = 2;
    public static int ERROR = 3;
    public static int WARNING = 4;
    public static int NOTICE = 5;
    public static int INFO = 6;
    public static int DEBUG = 7;

    public Syslog() {
        synchronized(Syslog.class) {
            if (Syslog._syslog == null) {
                Syslog._syslog = (IUnixSyslog) Native.loadLibrary("c", IUnixSyslog.class);
            }
        }
        _syslog.openlog("mozsvc.hdfs", 0x01|0x02, 1<<3);
    }

    public void emergency(String message)
    {
        log(EMERGENCY, message);
    }

    public void alert(String message)
    {
        log(ALERT, message);
    }

    public void critical(String message) 
    {
       log(CRITICAL, message);
    }

    public void error(String message) 
    {
        log(ERROR, message);
    } 

    public void warning(String message) {
        log(WARNING, message);
    }

    public void notice(String message) {
        log(NOTICE, message);
    }

    public void info(String message) {
        log(INFO, message);
    }

    public void debug(String message) {
        log(DEBUG, message);
    }

    public void log(int severity, String message)
    {
        _syslog.syslog(severity, message, "");
    }

    public void closelog()
    {
        _syslog.closelog();
    }

}
