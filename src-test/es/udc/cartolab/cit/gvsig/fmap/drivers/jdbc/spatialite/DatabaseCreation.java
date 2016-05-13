package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.db.utils.ConnectionWithParams;
import com.iver.cit.gvsig.fmap.drivers.db.utils.SingleVectorialDBConnectionManager;

import es.icarto.gvsig.commons.testutils.Drivers;

public class DatabaseCreation {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private String conStr;
    private String dbpath;

    @Before
    public void setUp() throws Exception {
	String folderPath = folder.getRoot().getAbsolutePath();
	dbpath = folderPath + "test_" + System.currentTimeMillis() + ".sqlite";
	conStr = "jdbc:sqlite:" + dbpath;
    }

    @Test
    public void javaConCreatesDatabase() throws Exception {
	Connection conn = DriverManager.getConnection(conStr);
	assertTrue(new File(dbpath).canRead());
	Statement stat = conn.createStatement();
	ResultSet rs = stat.executeQuery("SELECT 1");
	assertTrue("At least one row in the resultset", rs.next());
	assertEquals(1, rs.getInt(1));
	assertFalse("Only one row in the resultset", rs.next());
    }

    @Test
    public void driverConCreatesDatabase() throws Exception {
	NativeDependencies.libsPath = TestUtils.libPath;

	// TODO. Change it with your absolute path
	String driversPath = "../_fwAndami/gvSIG/extensiones/com.iver.cit.gvsig/drivers/";
	Drivers.initgvSIGDrivers(driversPath);
	SingleVectorialDBConnectionManager ins;
	ins = SingleVectorialDBConnectionManager.instance();
	ConnectionWithParams conwp = ins.getConnection(SpatiaLiteDriver.NAME,
		"", "", "spatiaLite", dbpath, "", "", true);
	assertNotNull("Connection is not null", conwp);
	assertTrue(new File(dbpath).canRead());
	Connection con = ((ConnectionJDBC) conwp.getConnection())
		.getConnection();
	Statement stat = con.createStatement();
	ResultSet rs = stat.executeQuery("SELECT 1");
	assertTrue("At least one row in the resultset", rs.next());
	assertEquals(1, rs.getInt(1));
	assertFalse("Only one row in the resultset", rs.next());
    }

}
