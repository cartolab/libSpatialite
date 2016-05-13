package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sqlite.core.CoreConnection;

import com.iver.cit.gvsig.fmap.drivers.ConnectionFactory;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBException;
import com.iver.cit.gvsig.fmap.drivers.db.utils.ConnectionWithParams;
import com.iver.cit.gvsig.fmap.drivers.db.utils.SingleVectorialDBConnectionManager;
import com.iver.cit.gvsig.fmap.layers.LayerFactory;

import es.icarto.gvsig.commons.testutils.Drivers;

public class LoadLibraries {

    private String conStr;
    private String dbpath;

    @BeforeClass
    public static void setUpBeforeClass() {
	NativeDependencies.libsPath = TestUtils.libPath;

	// TODO
	Drivers.initgvSIGDrivers("../_fwAndami/gvSIG/extensiones/com.iver.cit.gvsig/drivers");
    }

    @Before
    public void setUp() throws IOException {
	// Class.forName("org.sqlite.JDBC");
	File db = File.createTempFile("test", ".sqlite");
	db.deleteOnExit();
	dbpath = db.getAbsolutePath();
	conStr = "jdbc:sqlite:" + db.getAbsolutePath();
    }

    /**
     * This test doesn't really check our SpatiaLite driver, but the native
     * methods from the included library. Only checks that no exception is
     * raised
     */
    @Test
    public void dbConnectionTest() throws DBException, SQLException {
	ConnectionJDBC conn = (ConnectionJDBC) ConnectionFactory
		.createConnection(conStr, "", "");

	conn.getConnection();
	((CoreConnection) conn.getConnection()).realClose();
    }

    @Test
    public void libsCanBeLoadedThroughJDBC() throws DBException, SQLException {
	Connection con = DriverManager.getConnection(conStr);

	NativeDependencies nativeDeps = new NativeDependencies();
	nativeDeps.loadSystemDependencies();
	nativeDeps.loadSpatialite(con);

	Statement st = con.createStatement();
	ResultSet rs = st.executeQuery("SELECT spatialite_version();");
	rs.next();
	assertEquals("4.3.0a", rs.getString(1));
    }

    @Test
    public void libsCanBeLoadedThroughGvSIG() throws Exception {

	SingleVectorialDBConnectionManager ins = SingleVectorialDBConnectionManager
		.instance();
	ConnectionWithParams conwp = ins.getConnection(SpatiaLiteDriver.NAME,
		"", "", "spatialite", dbpath, "", "", true);
	assertNotNull("Connection is not null", conwp);

	SpatiaLiteDriver driver = (SpatiaLiteDriver) LayerFactory.getDM()
		.getDriver(SpatiaLiteDriver.NAME);
	driver.setConnection(conwp.getConnection());

	Connection con = ((ConnectionJDBC) conwp.getConnection())
		.getConnection();
	Statement st = con.createStatement();
	ResultSet rs = st.executeQuery("SELECT spatialite_version();");
	rs.next();
	assertEquals("4.3.0a", rs.getString(1));
    }

    /**
     * Probes that spatialite extension is loaded per connection and no per
     * database
     */
    @Test
    public void testExtensionIsLoadedPerCon() throws Exception {

	// DriverManager.getConnection creates a new sqlite "CoreConnection"
	// each time is invoked
	Connection con = DriverManager.getConnection(conStr);
	Connection conSpatial = DriverManager.getConnection(conStr);

	try {
	    Statement st = con.createStatement();
	    st.executeQuery("SELECT spatialite_version();");
	} catch (SQLException e) {
	    assertTrue(e.toString().contains(
		    "no such function: spatialite_version"));
	}

	NativeDependencies nativeDeps = new NativeDependencies();
	nativeDeps.loadSystemDependencies();
	nativeDeps.loadSpatialite(conSpatial);

	Statement stSpatial = conSpatial.createStatement();
	ResultSet rsSpatial = stSpatial
		.executeQuery("SELECT spatialite_version();");
	rsSpatial.next();
	assertEquals("4.3.0a", rsSpatial.getString(1));

	try {
	    Statement st = con.createStatement();
	    st.executeQuery("SELECT spatialite_version();");
	} catch (SQLException e) {
	    assertTrue(e.toString().contains(
		    "no such function: spatialite_version"));
	}
    }

    /**
     * Probes that the connection pool (SingleVectorialDBConnectionManager)
     * keeps only one connection to the sqlite file, and is enough load natives
     * once
     *
     * When using gvSIG db connections are get from a kind of pool. Each db
     * layer creates and instance of its driver, and each driver instance get a
     * connection (the same instance connection) from the pool. If spatialite
     * lib is loaded once there is no need to load it for all the new drivers
     */
    @Test
    public void testGvSIGPool() throws Exception {
	SingleVectorialDBConnectionManager ins = SingleVectorialDBConnectionManager
		.instance();
	ConnectionWithParams conwp1 = ins.getConnection(SpatiaLiteDriver.NAME,
		"", "", "spatialite", dbpath, "", "", true);
	assertNotNull("Connection is not null", conwp1);

	SpatiaLiteDriver driver1 = (SpatiaLiteDriver) LayerFactory.getDM()
		.getDriver(SpatiaLiteDriver.NAME);
	driver1.setConnection(conwp1.getConnection());

	Connection con1 = ((ConnectionJDBC) conwp1.getConnection())
		.getConnection();
	Statement st1 = con1.createStatement();
	ResultSet rs1 = st1.executeQuery("SELECT spatialite_version();");
	rs1.next();
	assertEquals("4.3.0a", rs1.getString(1));

	ConnectionWithParams conwp2 = ins.getConnection(SpatiaLiteDriver.NAME,
		"", "", "spatialite", dbpath, "", "", true);
	assertNotNull("Connection is not null", conwp2);
	assertTrue(conwp1 == conwp2);

	SpatiaLiteDriver driver2 = (SpatiaLiteDriver) LayerFactory.getDM()
		.getDriver(SpatiaLiteDriver.NAME);
	assertFalse(driver1 == driver2);
	driver2.setConnection(conwp2.getConnection());

	Connection con2 = ((ConnectionJDBC) conwp2.getConnection())
		.getConnection();
	assertTrue(con1 == con2);
	Statement st2 = con1.createStatement();
	ResultSet rs2 = st2.executeQuery("SELECT spatialite_version();");
	rs2.next();
	assertEquals("4.3.0a", rs2.getString(1));

    }

}
