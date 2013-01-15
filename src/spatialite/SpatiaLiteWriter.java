package spatialite;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import javax.swing.JOptionPane;

import org.apache.log4j.Logger;

import com.hardcode.gdbms.driver.exceptions.InitializeWriterException;
import com.hardcode.gdbms.driver.exceptions.WriteDriverException;
import com.hardcode.gdbms.engine.data.driver.DriverException;
import com.iver.cit.gvsig.exceptions.visitors.ProcessVisitorException;
import com.iver.cit.gvsig.exceptions.visitors.ProcessWriterVisitorException;
import com.iver.cit.gvsig.exceptions.visitors.StartWriterVisitorException;
import com.iver.cit.gvsig.exceptions.visitors.StopWriterVisitorException;
import com.iver.cit.gvsig.fmap.core.FShape;
import com.iver.cit.gvsig.fmap.core.IFeature;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.FieldDescription;
import com.iver.cit.gvsig.fmap.drivers.IConnection;
import com.iver.cit.gvsig.fmap.drivers.ITableDefinition;
import com.iver.cit.gvsig.fmap.edition.IFieldManager;
import com.iver.cit.gvsig.fmap.edition.IRowEdited;
import com.iver.cit.gvsig.fmap.edition.ISpatialWriter;
import com.iver.cit.gvsig.fmap.edition.fieldmanagers.JdbcFieldManager;
import com.iver.cit.gvsig.fmap.edition.writers.AbstractWriter;

public class SpatiaLiteWriter extends AbstractWriter implements ISpatialWriter,
IFieldManager {

	private DBLayerDefinition lyrDef;
	private IConnection conex;
	private Statement st;
	private boolean bCreateTable;
	private boolean bWriteAll;
	private SpatiaLite spatiaLite = new SpatiaLite();
	private JdbcFieldManager fieldManager;

	/**
	 * Useful to create a layer from scratch Call setFile before using this
	 * function
	 * 
	 * @param lyrDef
	 * @throws InitializeWriterException
	 * @throws IOException
	 * @throws DriverException
	 */
	public void initialize(ITableDefinition lyrD)
	throws InitializeWriterException {
		super.initialize(lyrD);
		this.lyrDef = (DBLayerDefinition) lyrD;
		conex = lyrDef.getConnection();

		try {
			st = ((ConnectionJDBC) conex).getConnection().createStatement();

			if (bCreateTable) {
				dropTableIfExist();

				String sqlCreate = spatiaLite.getSqlCreateSpatialTable(lyrDef,
						lyrDef.getFieldsDesc(), true);
				System.out.println("sqlCreate =" + sqlCreate);
				st.execute(sqlCreate);

				String sqlAlter = spatiaLite.getSqlAlterTable(lyrDef);
				System.out.println("sqlAlter =" + sqlAlter);
				st.execute(sqlAlter);
				((ConnectionJDBC) conex).getConnection().commit();
			}
			((ConnectionJDBC) conex).getConnection().setAutoCommit(false);

			String schema_tablename = lyrDef.getComposedTableName();
			fieldManager = new JdbcFieldManager(((ConnectionJDBC)conex).getConnection(), schema_tablename);



		} catch (SQLException e) {
			throw new InitializeWriterException(getName(), e);
		}

	}

	public void preProcess() throws StartWriterVisitorException {
		numRows = 0;

		ResultSet rsAux;
		try {
			((ConnectionJDBC) conex).getConnection().rollback();
			alterTable();

			rsAux = st.executeQuery("SHOW server_encoding;");
			rsAux.next();
			String serverEncoding = rsAux.getString(1);
			System.out.println("Server encoding = " + serverEncoding);
			spatiaLite.setEncoding(serverEncoding);
		} catch (SQLException e) {
			throw new StartWriterVisitorException(getName(), e);
		} catch (WriteDriverException e) {
			throw new StartWriterVisitorException(getName(), e);
		}

	}

	public void process(IRowEdited row) throws ProcessWriterVisitorException {

		String sqlInsert;
		try {
			switch (row.getStatus()) {
			case IRowEdited.STATUS_ADDED:
				IFeature feat = (IFeature) row.getLinkedRow();
				sqlInsert = spatiaLite.getSqlInsertFeature(lyrDef, feat);
				System.out.println("sql = " + sqlInsert);
				st.execute(sqlInsert);

				break;
			case IRowEdited.STATUS_MODIFIED:
				IFeature featM = (IFeature) row.getLinkedRow();
				if (bWriteAll) {
					sqlInsert = spatiaLite.getSqlInsertFeature(lyrDef, featM);
					System.out.println("sql = " + sqlInsert);
					st.execute(sqlInsert);
				} else {
					String sqlModify = spatiaLite.getSqlModifyFeature(lyrDef,
							featM);
					System.out.println("sql = " + sqlModify);
					st.execute(sqlModify);
				}
				break;
			case IRowEdited.STATUS_ORIGINAL:
				IFeature featO = (IFeature) row.getLinkedRow();
				if (bWriteAll) {
					sqlInsert = spatiaLite.getSqlInsertFeature(lyrDef, featO);
					st.execute(sqlInsert);
				}
				break;
			case IRowEdited.STATUS_DELETED:
				String sqlDelete = spatiaLite.getSqlDeleteFeature(lyrDef, row);
				System.out.println("sql = " + sqlDelete);
				st.execute(sqlDelete);

				break;
			}

			numRows++;
		} catch (ProcessVisitorException e) {
			Logger.getLogger(this.getClass()).error(getName(), e);
		} catch (SQLException e) {
			throw new ProcessWriterVisitorException(this.getName(), e);
		}

	}

	public void postProcess() throws StopWriterVisitorException {
		try {
			((ConnectionJDBC) conex).getConnection().setAutoCommit(true);
		} catch (SQLException e) {
			throw new StopWriterVisitorException(getName(), e);
		}
	}

	public String getName() {
		return "PostGIS Writer";
	}

	public boolean canWriteGeometry(int gvSIGgeometryType) {
		switch (gvSIGgeometryType) {
		case FShape.POINT:
			return true;
		case FShape.LINE:
			return true;
		case FShape.POLYGON:
			return true;
		case FShape.ARC:
			return false;
		case FShape.ELLIPSE:
			return false;
		case FShape.MULTIPOINT:
			return true;
		case FShape.TEXT:
			return false;
		}
		return false;
	}

	public boolean canWriteAttribute(int sqlType) {
		switch (sqlType) {
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.INTEGER:
		case Types.BIGINT:
			return true;
		case Types.DATE:
			return true;
		case Types.BIT:
		case Types.BOOLEAN:
			return true;
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.LONGVARCHAR:
			return true;

		}

		return false;
	}

	/**
	 * @return Returns the bCreateTable.
	 */
	public boolean isCreateTable() {
		return bCreateTable;
	}

	/**
	 * @param createTable
	 *            The bCreateTable to set.
	 */
	public void setCreateTable(boolean createTable) {
		bCreateTable = createTable;
	}

	public FieldDescription[] getOriginalFields() {
		return lyrDef.getFieldsDesc();
	}

	public void addField(FieldDescription fieldDesc) {
		fieldManager.addField(fieldDesc);

	}

	public FieldDescription removeField(String fieldName) {
		return fieldManager.removeField(fieldName);

	}

	public void renameField(String antName, String newName) {
		fieldManager.renameField(antName, newName);

	}

	public boolean alterTable() throws WriteDriverException {
		return fieldManager.alterTable();
	}

	public FieldDescription[] getFields() {
		return fieldManager.getFields();
	}

	public boolean canAlterTable() {
		return canSaveEdits();
	}

	public boolean canSaveEdits() {
		try {
			String connUrl = lyrDef.getConnection().getURL();
			File sqliteFile = new File(connUrl.replace(spatiaLite.getConnectionStringBeginning(), ""));
			return sqliteFile.canWrite();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private boolean dropTableIfExist() throws SQLException {
		if (!this.existTable(lyrDef.getTableName())) {
			return false;
		}
		st = ((ConnectionJDBC) conex).getConnection().createStatement();
		st.execute("DROP TABLE " + lyrDef.getComposedTableName() + ";");
		st.close();
		return true;
	}

	private boolean existTable(String tableName)
	throws SQLException {
		boolean exists = false;
		
		String sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";

		PreparedStatement st = ((ConnectionJDBC) conex).getConnection().prepareStatement(sql);
		st.setString(1, tableName);
		ResultSet rs = st.executeQuery();
		if (rs.next()) {
			exists = true;
		}
		rs.close();
		st.close();

		return exists;
	}
}
