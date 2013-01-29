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
package es.udc.cartolab.gvsig.spatiaLiteConnector;

import java.io.File;

import com.iver.andami.PluginServices;
import com.iver.andami.plugins.Extension;
import com.iver.andami.preferences.IPreference;
import com.iver.andami.preferences.IPreferenceExtension;
import com.iver.cit.gvsig.fmap.drivers.DBException;
import com.iver.utiles.XMLEntity;

import es.udc.cartolab.gvsig.spatiaLiteConnector.gui.DBConnectionDialog;
import es.udc.cartolab.gvsig.spatiaLiteConnector.preferences.UsersPreferencePage;
import es.udc.cartolab.gvsig.spatiaLiteConnector.utils.DBSession;


public class DBConnectionExtension extends Extension implements IPreferenceExtension {

	public static UsersPreferencePage usersPreferencesPage = new UsersPreferencePage();

	public void execute(String actionCommand) {
		//without header image
		DBConnectionDialog dialog = new DBConnectionDialog();
		dialog.openWindow();
	}

	public void initialize() {
		//Creating config Dir
		String symbolsDirStr = System.getProperty("user.dir") + File.separator + "Leyendas";
		File symbolsDir = new File(symbolsDirStr);
		symbolsDir.mkdir();

		//icon
		registerIcons();
	}

	protected void registerIcons() {
		PluginServices.getIconTheme().registerDefault(
				"DBConnect",
				this.getClass().getClassLoader().getResource("images/sessionconnect.png")
			);
	}

	public boolean isEnabled() {
		// TODO Auto-generated method stub
		return true;
	}

	public boolean isVisible() {
		// TODO Auto-generated method stub
		return true;
	}

	public IPreference[] getPreferencesPages() {
		// TODO Auto-generated method stub
		IPreference[] preferences=new IPreference[1];
		preferences[0]=usersPreferencesPage;
		return preferences;
	}

	public void terminate() {
		DBSession dbs = DBSession.getCurrentSession();
		if (dbs != null) {
			try {
				dbs.close();
			} catch (DBException e) {
				e.printStackTrace();
			}
		}
	}

	public void postInitialize() {
		//poner if
		PluginServices ps = PluginServices.getPluginServices(this);
		XMLEntity xml = ps.getPersistentXML();
		if (xml.contains(UsersPreferencePage.CONNECT_DB_AT_STARTUP_KEY_NAME)) {
			if (xml.getBooleanProperty(UsersPreferencePage.CONNECT_DB_AT_STARTUP_KEY_NAME)) {
				execute(null);
			}
		}

	}

}
