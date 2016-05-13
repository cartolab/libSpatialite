package es.icarto.gvsig.commons.testutils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TestProperties {

    public static String driversPath = "../_fwAndami/gvSIG/extensiones/com.iver.cit.gvsig/drivers";

    public static String server = "localhost";
    public static int port = 5432;
    public static String database = "";
    public static String schema = null;
    public static String username = "postgres";
    public static String password = "postgres";

    static {
	Properties p = new Properties();
	FileInputStream in;
	try {
	    in = new FileInputStream("data-test/test.properties");
	    p.load(in);
	    in.close();
	    driversPath = p.getProperty("driversPath");
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }

}
