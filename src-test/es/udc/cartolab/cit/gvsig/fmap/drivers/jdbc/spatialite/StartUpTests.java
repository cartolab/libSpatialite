package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import java.io.File;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.sqlite.core.CoreConnection;

import com.iver.cit.gvsig.fmap.drivers.ConnectionFactory;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBException;

/**
 * StartUpTests
 *
 * This class simply checks that the driver can be properly initialized, which
 * includes loading the OS native libraries. It has no asserts as we only want
 * to check that no exception is raised.
 *
 * @author Jorge López <jlopez@cartolab.es>
 */
public class StartUpTests {

    private static SpatiaLiteDriver driver;

    @BeforeClass
    public static void setUpBeforeClass() {
	NativeDependencies.libsPath = TestUtils.libPath;
	driver = new SpatiaLiteDriver();
    }

    @Test
    public void librariesLoadingTest() {
	// We do nothing for testing the driver creation
    }

    /*
     * This test doesn't really check our SpatiaLite driver, but the native
     * methods from the included library.
     */
    @Test
    public void dbConnectionTest() throws DBException, SQLException {
	File file = new File(TestUtils.testDataPath);
	String connectionString = driver.getConnectionString(
		file.getAbsolutePath(), "", "", "", "");

	ConnectionJDBC conn = (ConnectionJDBC) ConnectionFactory
		.createConnection(connectionString, "", "");

	conn.getConnection();
	((CoreConnection) conn.getConnection()).realClose();
    }

}
