
# Actualización de SQLite JDBC

Este proyecto usa un fork de SQLite JDBC que se [mantiene aquí](https://github.com/cartolab/sqlite-jdbc)

Cada release de libSpatialite debe ser tagueada con el mismo nombre/tag que se use en el fork. La versión del jar debe ser la misma que la que se use en el proyecto original en el momento de la actualización.

Si se actualiza el jar de SQLiteJDBC debe actualizarse también el build.xml de libSpatialite para reflejar la actualización de la librería.

# Actualización de las librerías nativas

Los mantenedores de spatialite sólo proporcionan binarios para windows ([x86](http://www.gaia-gis.it/gaia-sins/windows-bin-x86/) y [amd64](http://www.gaia-gis.it/gaia-sins/windows-bin-amd64/)). Para linux deben [compilarse de forma manual](http://www.gaia-gis.it/gaia-sins/linux_how_to.html) u optar por reaprovechar los binarios ya compilados en un paquete de alguna distribución.

Debe tenerse en cuenta que a su vez el binario de spatialite tiene dependencias de versiones específicas de otras librerías nativas, por lo que todas las dependencias nativas deben incluírse en la distribución de gvSIG-libSpatialite. Si se delega en las librerías del sistema se puede incurrir en errores difíciles de detectar.

En Windows, basta con descargar el fichero mod_spatialite-XXX-win-x86.7z. Dado que tanto el binario de spatialite como el resto de dependencias binarias vienen incluídas en el fichero. Y actulizar es sólo substituir unos ficheros por otros. Las instrucciones de como [compilar los binarios para windows](http://www.gaia-gis.it/gaia-sins/mingw_how_to.html) dan más información sobre las dependencias y versiones

spatialite no especifica con que version de sqlite es compatible pero solicita una superior a la: 3.7.3 y los binarios de windows (v4.3.0a) se compilan contra SQLite v1.12

En Linux se puede optar por revisar los paquetes de Ubuntu para spatialite. Desde la página principal podemos hacer la [búsqueda por versión o por paquete](http://packages.ubuntu.com/). Y a partir de la información disponible bajar el paquete y las dependencias necesarias. Descomprimirlos e incluirlos en nuestros propios builds. Debe tenerse en cuenta que existen (fundamentalmente) [dos paquetes distintos](http://permalink.gmane.org/gmane.linux.debian.gis/2223):

* libspatialite7: Usado cuando la aplicación se "linka" directamente con spatialite
* libsqlite3-mod-spatialite: El módulo a cargar de forma dinámica en SQLite.

Téngase en cuenta que si cambian los números de versiones debe actualizarse es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.SpatiaLiteDriver

# Versiones de las dependencias actuales (20160503)

* sqlite-jdbc-3.9.1-SNAPSHOT.jar
* La versión de SQLite soportada por este conector es la 3.9.1
* La versión de SQLite más actual es la 3.12.2
* spatilite v4.3.0a
* Librerías nativas de las que depende spatialite actualizadas a partir de los binarios para windows proporcionados por spatialite y del paquete [libsqlite3-mod-spatialite](https://launchpad.net/ubuntu/xenial/i386/libsqlite3-mod-spatialite/4.3.0a-5) para Ubuntu 16.04 Xerial.


# Funcionamiento del driver

spatialite es un módulo para sqlite que debe ser cargado dinámicamente en la conexión a sqlite mediante comandos específicos del conector o una query propia de SQL

```sql
SELECT load_extension('mod_spatialite');
```

Donde mod_spatialite son las librerías nativas en forma de dll para windows o .so para linux. Las librerías nativas deben estar en el path. Además la versión de SQLite usada debe haber sido compilada con soporte para módulos y spatialite (que es lo más habitual)
