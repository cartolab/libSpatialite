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

import com.iver.andami.PluginServices;
import com.iver.andami.plugins.Extension;
import com.iver.cit.gvsig.ProjectExtension;
import com.iver.cit.gvsig.fmap.drivers.DBException;

import es.udc.cartolab.gvsig.spatiaLiteConnector.utils.DBSession;

public class CloseSessionExtension extends Extension {

	public void execute(String actionCommand) {
		DBSession dbs = DBSession.getCurrentSession();
		if (dbs!=null) {
			try {

				if (!dbs.askSave()) {
					return;
				}
				dbs.close();

				ProjectExtension pExt = (ProjectExtension) PluginServices.getExtension(ProjectExtension.class);
				pExt.execute("NUEVO");

			} catch (DBException e) {
				e.printStackTrace();
			}
		}
	}

	public void initialize() {
		registerIcons();
	}

	protected void registerIcons() {
		PluginServices.getIconTheme().registerDefault(
				"DBClose",
				this.getClass().getClassLoader().getResource("images/sessiondisc.png")
			);
	}

	public boolean isEnabled() {
		return true;
	}

	public boolean isVisible() {
		DBSession dbs = DBSession.getCurrentSession();
		return dbs!=null;
	}

}
