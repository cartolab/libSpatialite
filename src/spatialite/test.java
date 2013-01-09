package spatialite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.SQLiteConfig;

import com.hardcode.gdbms.driver.exceptions.ReadDriverException;
import com.iver.cit.gvsig.fmap.core.IGeometry;

public class test {


	public static void main(String[] args) {
		readingSinglePointGeom();

	}

	//It works!
	private static void readingSinglePointGeom() {
		SpatiaLiteDriver driver = new SpatiaLiteDriver();
		try {
			IGeometry geom = driver.getShape(1);
			System.out.println(geom.toJTSGeometry().toText());
		} catch (ReadDriverException e) {
			e.printStackTrace();
		}
	}
	
	//It works!
	public void simpleTest() {
		SQLiteConfig config = new SQLiteConfig();
		config.enableLoadExtension(true);
		Connection conn;
		try {
			conn = DriverManager.getConnection("jdbc:sqlite:/home/jlopez/spatialite/prueba.sqlite", config.toProperties());
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT load_extension('/usr/lib/libspatialite.so.3.2.0');");
			ResultSet result = stmt.executeQuery("SELECT cod_com FROM comunidad;");
			while (result.next()) {
				System.out.println(result.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
