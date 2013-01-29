/*
 * Copyright (c) 2010. CartoLab, Universidad de A Coruña
 *
 * This file is part of extDBConnection
 *
 * extDBConnection is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or any later version.
 *
 * extDBConnection is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with extDBConnection.
 * If not, see <http://www.gnu.org/licenses/>.
*/
package es.udc.cartolab.gvsig.spatiaLiteConnector.utils;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JOptionPane;

import org.cresques.cts.IProjection;

import com.hardcode.driverManager.Driver;
import com.hardcode.driverManager.DriverLoadException;
import com.iver.andami.Launcher;
import com.iver.andami.Launcher.TerminationProcess;
import com.iver.andami.PluginServices;
import com.iver.andami.ui.wizard.UnsavedDataPanel;
import com.iver.cit.gvsig.ProjectExtension;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBException;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.FieldDescription;
import com.iver.cit.gvsig.fmap.drivers.IVectorialJDBCDriver;
import com.iver.cit.gvsig.fmap.drivers.db.utils.ConnectionWithParams;
import com.iver.cit.gvsig.fmap.drivers.db.utils.SingleVectorialDBConnectionManager;
import com.iver.cit.gvsig.fmap.layers.FLayer;
import com.iver.cit.gvsig.fmap.layers.LayerFactory;
import com.iver.cit.gvsig.project.Project;


public class DBSession {

	private static DBSession instance = null;
	private final String sqliteFile;
	private ConnectionWithParams conwp;

	private DBSession(String sqliteFile) {
		this.sqliteFile = sqliteFile;
	}
	/**
	 *
	 * @return the DB Connection or null if there isn't any
	 */
	public static DBSession getCurrentSession() {
		return instance;
	}

	/**
	 * Creates a new DB Connection or changes the current one.
	 * @param sqliteFile
	 * @return the connection
	 * @throws DBException if there's any problem (server error or login error)
	 */
	public static DBSession createConnection(String sqliteFile) throws DBException {
		if (instance != null) {
			instance.close();
		}
		instance = new DBSession(sqliteFile);
		connect();
		return instance;
	}

	/**
	 * To be used only when there's any error (SQLException) that is not handled by gvSIG
	 * @return the session
	 * @throws DBException
	 */
	public static DBSession reconnect() throws DBException {
		if (instance!=null) {
			return createConnection(instance.sqliteFile);
		}
		return null;
	}

	private static void connect() throws DBException {
		try {
			instance.conwp = SingleVectorialDBConnectionManager.instance().getConnection("SpatiaLite JDBC Driver",
					"", "", "SpatiaLite_connection", instance.sqliteFile, "", "", true);
		} catch (DBException e) {
			if (instance!=null) {
				if (instance.conwp != null) {
					SingleVectorialDBConnectionManager.instance().closeAndRemove(instance.conwp);
				}
			}
			instance = null;
			throw e;
		}

	}

	public String getSQLiteFile() {
		return sqliteFile;
	}

	public Connection getJavaConnection() {
		return ((ConnectionJDBC) conwp.getConnection()).getConnection();
	}

	public void close() throws DBException {

		if (conwp!=null) {
			SingleVectorialDBConnectionManager.instance().closeAndRemove(conwp);
			conwp = null;
		}
		instance = null;

	}

	/* GET LAYER */

	public FLayer getLayer(String layerName, String tableName, String whereClause,
			IProjection projection) throws SQLException, DBException {
		//Code by Sergio Piñón (gvsig_desarrolladores)

		if (whereClause == null) {
			whereClause = "";
		}

		DBLayerDefinition dbLayerDef = new DBLayerDefinition();
		dbLayerDef.setCatalogName(""); //Nombre de la base de datos
		dbLayerDef.setSchema(""); //Nombre del esquema
		dbLayerDef.setTableName(tableName); //Nombre de la tabla
		dbLayerDef.setWhereClause("");
		dbLayerDef.setConnection(conwp.getConnection());

		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();
		DatabaseMetaData metadataDB = con.getMetaData();

		String tipos[] = new String[1];
		tipos[0] = "TABLE";
		ResultSet tablas = metadataDB.getTables(null, "", tableName, tipos);
		tablas.next();
		//String t = tablas.getString(tablas.findColumn( "TABLE_NAME" ));

		ResultSet columnas = metadataDB.getColumns(null, "", tableName, "%");
		ResultSet claves = metadataDB.getPrimaryKeys(null, "", tableName);

		//ResultSetMetaData aux = columnas.getMetaData();

		ArrayList<FieldDescription> descripciones = new ArrayList <FieldDescription>();
		ArrayList<String> nombres = new ArrayList<String>();

		while(columnas.next()) {
			//log.info("Tratando atributo: \""+columnas.getString("Column_Name")+"\" de la tabla: "+nombreTabla);
			if(columnas.getString("Type_Name").equalsIgnoreCase("geometry")) {
				/*si es la columna de geometria*/
				//log.info("Encontrado atributo de geometria para la tabla: "+nombreTabla);
				dbLayerDef.setFieldGeometry(columnas.getString("Column_Name"));
			}
			else {
				FieldDescription fieldDescription = new FieldDescription();
				fieldDescription.setFieldName(columnas.getString("Column_Name"));
				fieldDescription.setFieldType(columnas.getType());
				descripciones.add(fieldDescription);
				nombres.add(columnas.getString("Column_Name"));
			}
		}
		FieldDescription fields[] = new FieldDescription[descripciones.size()];
		String s[] = new String[nombres.size()];
		for(int i = 0; i < descripciones.size(); i++)  {
			fields[i] = descripciones.get(i);
			s[i] = nombres.get(i);
		}

		dbLayerDef.setFieldsDesc(fields);
		dbLayerDef.setFieldNames(s);

		if (whereClause.compareTo("")!=0) {
			dbLayerDef.setWhereClause(whereClause);
		}

		/*buscamos clave primaria y la añadimos a la definicion de la capa*/
		//OJO, esta solución no vale con claves primarias de más de una columna!!!
		while(claves.next()) {
			dbLayerDef.setFieldID(claves.getString("Column_Name"));
		}

		if (dbLayerDef.getFieldID() == null) {
			dbLayerDef.setFieldID("PK_UID");
		}

		dbLayerDef.setSRID_EPSG(projection.getAbrev());

		Driver drv;
		FLayer lyr = null;
		try {
			drv = LayerFactory.getDM().getDriver("SpatiaLite JDBC Driver");
			IVectorialJDBCDriver dbDriver = (IVectorialJDBCDriver) drv;

			dbDriver.setData(conwp.getConnection(), dbLayerDef);

			lyr =  LayerFactory.createDBLayer(dbDriver, layerName, projection);
			/*asignamos proyección a la capa y al ViewPort*/
//			dbLayerDef.setSRID_EPSG(projection.getAbrev());
		} catch (DriverLoadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lyr;
	}

	public FLayer getLayer(String tableName, String whereClause,
			IProjection projection) throws SQLException, DBException {
		return getLayer(tableName, tableName, whereClause, projection);
	}

	public FLayer getLayer(String tableName, IProjection projection) throws SQLException, DBException {
		return getLayer(tableName, null, projection);
	}


	/* GET TABLE */

	private String[] getColumnNames(String tablename) throws SQLException {

		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();

		String query = "SELECT * FROM " + tablename + " LIMIT 1";
		Statement st = con.createStatement();
		ResultSet resultSet = st.executeQuery(query);
		ResultSetMetaData md = resultSet.getMetaData();
		String[] cols = new String[md.getColumnCount()];
		for (int i=0; i<md.getColumnCount(); i++) {
			cols[i] = md.getColumnLabel(i+1);
		}
		return cols;
	}

	private int getColumnType(String tablename, String column) throws SQLException {

		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();

		DatabaseMetaData meta = con.getMetaData();
		ResultSet rsColumns = meta.getColumns(null, "", tablename, column);
		while (rsColumns.next()) {
			if (column.equalsIgnoreCase(rsColumns.getString("COLUMN_NAME"))) {
				return rsColumns.getInt("COLUMN_TYPE");
			}
		}
		return -1;
	}

	public String[][] getTable(String tableName, String whereClause,
			String[] orderBy, boolean desc) throws SQLException {

		String[] columnNames = getColumnNames(tableName);

		return getTable(tableName, columnNames, whereClause,
				orderBy, desc);
	}


	public String[][] getTable(String tableName, String[] fieldNames, String whereClause,
			String[] orderBy, boolean desc) throws SQLException {
		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();

		if (whereClause == null) {
			whereClause = "";
		}

		int numFieldsOrder;

		if (orderBy == null) {
			numFieldsOrder = 0;
		} else {
			numFieldsOrder = orderBy.length;
		}


		String query = "SELECT ";
		for (int i=0; i<fieldNames.length-1; i++) {
			query = query + fieldNames[i] + ", ";
		}

		query = query + fieldNames[fieldNames.length-1] + " FROM " + tableName;

		List<String> whereValues = new ArrayList<String>();

		if (whereClause.compareTo("")!=0) {
			int quoteIdx = whereClause.indexOf('\'');
			while (quoteIdx>-1) {
				int endQuote = whereClause.indexOf('\'', quoteIdx+1);
				String subStr = whereClause.substring(quoteIdx+1, endQuote);
				whereValues.add(subStr);
				quoteIdx = whereClause.indexOf('\'', endQuote+1);
			}

			for (int i=0; i<whereValues.size(); i++) {
				whereClause = whereClause.replaceFirst("'" + whereValues.get(i) + "'", "?");
			}

			if (whereClause.toUpperCase().startsWith("WHERE")){
				query = query + " " + whereClause;
			} else {
				query = query + " WHERE " + whereClause;
			}
		}

		if (numFieldsOrder > 0) {
			query = query + " ORDER BY ";
			for (int i=0; i<numFieldsOrder-1; i++) {
				query = query + orderBy[i] + ", ";
			}
			query = query + orderBy[orderBy.length-1];

			if (desc) {
				query = query + " DESC";
			}
		}

		PreparedStatement stat = con.prepareStatement(query);
		for (int i=0; i<whereValues.size(); i++) {
			stat.setString(i+1, whereValues.get(i));
		}

		ResultSet rs = stat.executeQuery();

		ArrayList<String[]> rows = new ArrayList<String[]>();
		while (rs.next()) {
			String[] row = new String[fieldNames.length];
			for (int i=0; i<fieldNames.length; i++) {
				String val = rs.getString(fieldNames[i]);
				if (val == null) {
					val = "";
				}
				row[i] = val;
			}
			rows.add(row);
		}
		rs.close();

		return rows.toArray(new String[0][0]);

	}

	public String[][] getTable(String tableName,
			String[] orderBy, boolean desc) throws SQLException {
		return getTable(tableName, null, orderBy, desc);
	}

	public String[][] getTable(String tableName, String whereClause) throws SQLException {
		return getTable(tableName, whereClause, null, false);
	}

	public String[][] getTable(String tableName) throws SQLException {
		return getTable(tableName, null, null, false);
	}



	/* GET DISTINCT VALUES FROM A COLUMN */

	public String[] getDistinctValues(String tableName, String fieldName, boolean sorted, boolean desc, String whereClause) throws SQLException {

		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();

		Statement stat = con.createStatement();

		if (whereClause == null) {
			whereClause = "";
		}

		String query = "SELECT DISTINCT " + fieldName + " FROM " + tableName + " " + whereClause;

		if (sorted) {
			query = query + " ORDER BY " + fieldName;
			if (desc) {
				query = query + " DESC";
			}
		}

		ResultSet rs = stat.executeQuery(query);

		List <String>resultArray = new ArrayList<String>();
		while (rs.next()) {
			String val = rs.getString(fieldName);
			resultArray.add(val);
		}
		rs.close();

		String[] result = new String[resultArray.size()];
		for (int i=0; i<resultArray.size(); i++) {
			result[i] = resultArray.get(i);
		}

		return result;

	}

	public String[] getDistinctValues(String tableName, String fieldName, boolean sorted, boolean desc) throws SQLException {
		return getDistinctValues(tableName, fieldName, sorted, desc, null);
	}

	public String[] getDistinctValues(String tableName, String fieldName) throws SQLException {
		return getDistinctValues(tableName, fieldName, false, false, null);
	}

	public String[] getTables(boolean onlyGeospatial) throws SQLException {

		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();
		DatabaseMetaData metadataDB = con.getMetaData();
		ResultSet rs = metadataDB.getTables(null, null, null, new String[] {"TABLE"});
		List<String> tables = new ArrayList<String>();
		while (rs.next()) {
			String tableName = rs.getString("TABLE_NAME");
			if (onlyGeospatial) {
				boolean geometry = false;
				ResultSet columns = metadataDB.getColumns(null,null,tableName, "%");
				while (columns.next()) {
					if (columns.getString("Type_name").equalsIgnoreCase("geometry")) {
						geometry = true;
						break;
					}
				}
				if (geometry) {
					tables.add(tableName);
				}
			} else {
				tables.add(tableName);
			}
		}
		String[] result = new String[tables.size()];
		for (int i=0; i<tables.size(); i++) {
			result[i] = tables.get(i);
		}
		return result;
	}

	public String[] getColumns(String table) throws SQLException {

		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();
		DatabaseMetaData metadataDB = con.getMetaData();

		ResultSet columns = metadataDB.getColumns(null,"",table, "%");
		List <String> cols = new ArrayList<String>();
		while (columns.next()) {
			cols.add(columns.getString("Column_name"));
		}
		String[] result = new String[cols.size()];
		for (int i=0; i<cols.size(); i++) {
			result[i] = cols.get(i);
		}
		return result;
	}

	/**
	 * Be careful!
	 * @param table
	 * @param whereClause
	 * @throws SQLException
	 */
	public void deleteRows(String table, String whereClause) throws SQLException {

		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();

		String sql = "DELETE FROM " + table + " " +  whereClause;

		Statement statement = con.createStatement();
		statement.executeUpdate(sql);
		con.commit();
	}

	public void insertRow(String table, Object[] values) throws SQLException {

		String[] columns = getColumnNames(table);
		insertRow(table, columns, values);
	}

	public void insertRow(String table, String[] columns, Object[] values) throws SQLException {

		Connection con = ((ConnectionJDBC) conwp.getConnection()).getConnection();

		if (columns.length == values.length) {
			String sql = "INSERT INTO " + table + " (";
			for (String col : columns) {
				sql = sql + col + ", ";
			}
			sql = sql.substring(0, sql.length()-2) + ") VALUES (";
			for (int i=0; i<columns.length; i++) {
				sql = sql + "?, ";
			}
			sql = sql.substring(0, sql.length()-2) + ")";

			PreparedStatement statement = con.prepareStatement(sql);

			for (int i=0; i<columns.length; i++) {
				statement.setObject(i+1, values[i]);
			}

			statement.executeUpdate();
			con.commit();

		}

	}
	/**
	 * Be careful!
	 * @param tablename
	 * @param fields
	 * @param values
	 * @param whereClause
	 * @throws SQLException
	 */
	public void updateRows(String tablename, String[] columns,
			Object[] values, String whereClause) throws SQLException {

		Connection con =((ConnectionJDBC) conwp.getConnection()).getConnection();

		if (columns.length == values.length) {
			String sql = "UPDATE " + tablename + " SET ";
			for (String column : columns) {
				sql = sql + column + "=?, ";
			}
			sql = sql.substring(0, sql.length()-2) + " " + whereClause;

			PreparedStatement statement = con.prepareStatement(sql);
			for (int i=0; i<values.length; i++) {
				statement.setObject(i+1, values[i]);
			}

			statement.executeUpdate();
			con.commit();
		}

	}

	public boolean tableExists(String tablename) throws SQLException {

		Connection con =((ConnectionJDBC) conwp.getConnection()).getConnection();
		
		String sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";

		PreparedStatement st = con.prepareStatement(sql);
		st.setString(1, tablename);
		ResultSet rs = st.executeQuery();
		if (rs.next()) {
			return true;
		}
		return false;

	}

	/**
	 * Checks if there is an active and unsaved project and asks the user to save resources.
	 * @return true if there's no unsaved data
	 */
	public boolean askSave() {

		ProjectExtension pExt = (ProjectExtension) PluginServices.getExtension(ProjectExtension.class);
		Project p = pExt.getProject();

		if (p != null && p.hasChanged()) {
			TerminationProcess process = Launcher.getTerminationProcess();
			UnsavedDataPanel panel = process.getUnsavedDataPanel();
			panel.setHeaderText(PluginServices.getText(this, "Select_resources_to_save_before_closing_current_project"));
			panel.setAcceptText(
					PluginServices.getText(this, "save_resources"),
					PluginServices.getText(this, "Save_the_selected_resources_and_close_current_project"));
			panel.setCancelText(
					PluginServices.getText(this, "Dont_close"),
					PluginServices.getText(this, "Return_to_current_project"));
			int closeCurrProj = process.manageUnsavedData();
			if (closeCurrProj==JOptionPane.NO_OPTION) {
				// the user chose to return to current project
				return false;
			} else if (closeCurrProj==JOptionPane.YES_OPTION) {
				//trick to avoid ask twice for modified data
				p.setModified(false);
			}
		}
		return true;
	}
}
