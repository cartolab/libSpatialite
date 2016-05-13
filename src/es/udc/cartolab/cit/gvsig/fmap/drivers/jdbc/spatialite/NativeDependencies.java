package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.sqlite.SQLiteConnection;
import org.sqlite.util.OSInfo;

import com.iver.cit.gvsig.fmap.drivers.DBException;

public class NativeDependencies {

    private static final Logger logger = Logger
	    .getLogger(NativeDependencies.class);

    // TODO
    public static String libsPath = "gvSIG" + File.separator + "extensiones"
	    + File.separator + "com.iver.cit.gvsig" + File.separator + "lib"
	    + File.separator;

    private void loadDependency(String libname) {
	String path = new File(libsPath + libname).getAbsolutePath();
	System.load(path);
	logger.info("Library loaded: " + path);
    }

    protected void loadSystemDependencies() {

	String osName = OSInfo.getOSName();
	if (osName.equals("Linux")) {
	    loadDependency("libproj.so.9.1.0");
	    loadDependency("libfreexl.so.1.1.0");
	    loadDependency("libstdc++.so.6.0.21");
	    loadDependency("libgeos-3.5.0.so");
	    loadDependency("libgeos_c.so.1.9.0");
	} else if (osName.equals("Windows")) {
	    loadDependency("libgcc_s_dw2-1.dll");
	    loadDependency("libproj-9.dll");
	    loadDependency("libiconv-2.dll");
	    loadDependency("libfreexl-1.dll");
	    loadDependency("libstdc++-6.dll");
	    loadDependency("libgeos-3-5-0.dll");
	    loadDependency("libgeos_c-1.dll");
	    loadDependency("liblzma-5.dll");
	    loadDependency("zlib1.dll");
	    loadDependency("libxml2-2.dll");
	} else {
	    throw new RuntimeException("Spatialite for " + osName
		    + "is not implemented");
	}

    }

    protected void loadSpatialite(Connection javaCon) throws DBException,
    SQLException {
	String osName = OSInfo.getOSName();
	File spatialiteLib = null;
	String path = null;
	if (osName.equals("Linux")) {
	    spatialiteLib = new File(libsPath + "mod_spatialite.so.7.1.0");
	    path = spatialiteLib.getAbsolutePath();
	    // path = path.substring(0, path.length() - 3);
	} else if (osName.equals("Windows")) {
	    spatialiteLib = new File(libsPath + "mod_spatialite.dll");
	    path = spatialiteLib.getAbsolutePath();

	    /**
	     * FIX FOR SQLITE BUG
	     *
	     * Recent versions of SQLite appear to have a bug where they don't
	     * accept the Windows path separator (\) as the last separator of an
	     * extension path. Previous separators can be either / or \, mixed
	     * in any way.
	     */
	    path = path.replace("\\mod_spatialite.dll", "/mod_spatialite.dll");
	    // path = path.substring(0, path.length() - 4);
	}
	if ((spatialiteLib == null) || (!spatialiteLib.canRead())) {
	    throw new RuntimeException("Can't read spatiaLite library: "
		    + spatialiteLib.getAbsolutePath());
	}

	// Disabled by default http://sqlite.org/loadext.html
	((SQLiteConnection) javaCon).db().enable_load_extension(true);
	String query = "SELECT load_extension('" + path + "');";
	logger.info(query);
	Statement st = javaCon.createStatement();
	st.executeQuery(query);
	st.close();
    }

}
