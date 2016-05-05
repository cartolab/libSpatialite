# Mejora de rendimiento y estabilidad de gvSIG-libSpatialite - 20160503

De cara a optimizar el rendimiento y mejorar la estabilidad del driver de spatialite para gvSIG (proyecto gvSIG-libSpatialite) en este documento se hace un análisis de las dependencias del driver y mejoras en el proceso de *build* que puedan contribuír a estos dos objetivos.

Toda la información que aparece en este documento toma como referencia la fecha de: 3 de Mayo de 2016

## Dependencias

Para crear un driver para gvSIG que pueda acceder a bases de datos spatialite son imprescindibles dos dependencias:

* Un conector de Java para SQLite
* Cargar el módulo binario de spatialite en la base de datos

Mantener estas dependencias actualizadas es clave para el rendimiento y la estabilidad final de gvSIG-libSpatilite.

### Análisis de conectores Java - SQLite

A la hora de implementar el driver se debe escoger un conector/driver de sqlite y java. Es decir la librería que permite a aplicaciones Java operar con bases de datos SQLite.

Cuando arranco el proyecto en 2014, la más prometedora y utilizada era [SQLite JDBC](https://bitbucket.org/xerial/sqlite-jdbc) de Xerial (Taro L. Saito). En este documento se analiza el estado de las librerías actuales (), por si los costes de implantar un nuevo conector compensaran a efectos de rendimiento, estabilidad u otros respecto a la implementación actual.

#### Listado de conectores disponibles

En la [web de sqlite](http://www.sqlite.org/cvstrac/wiki?p=SqliteWrappers) y en la [documentación de uno de los wrappers](https://bitbucket.org/almworks/sqlite4java/wiki/ComparisonToOtherWrappers) hay una lista de posibles librerías a emplear. Tras analizarlas se hace un estudio más detallado de las siguientes, por estar el resto desactualizadas.

* [SQLJet](http://sqljet.com/). A pesar de que la últime versión es de 2014, interesa mencionarlo por ser una aproximación distinta al resto de drivers. No implementa el protocolo JDBC, por lo que los costos de integración con software existente son mayores. A cambio pesa muy poco y promete ser bastante rápido. Por no estar actualizado también es descartado.

* [Javasqlite Wrapper/JDBC driver from Christian Werner](http://www.ch-werner.de/javasqlite/). Se descarta por no disponer de un repositorio público por lo que hace difícil seguir la evolución del proyecto y en análisis hecho en sqlite4java.

* [sqlite4java](https://bitbucket.org/almworks/sqlite4java). Está actualizado, pero no es un conector JDBC por lo que la integración en el código existente y en gvSIG puede ser complicada. Además puede tener problemas de integración con [spatialite](https://bitbucket.org/almworks/sqlite4java/issues/80/sqliteconnectionloadextension-doesnt-work). Actualmente está actualizado para usar SQLite 3.8.7.

* [Xerial SQLite JDBC](https://github.com/xerial/sqlite-jdbc). El conector JDBC más usado. Recomendada varios ORM ([ormlite](http://ormlite.com/), [SQLite-Database-Model](https://github.com/SheldonNeilson/SQLite-Database-Model)) y el que está empleando libSpatialite en la actualidad. Actualmente está actualizado para usar SQLite 3.9.1.

De estos cuatro unicamente sqlite4java y SQLiteJDBC son candidatos serios. Dado que SQLiteJDBC está más actulizado y es el empleado actualmente se decide continuar con él.

#### Actualización de SQLiteJDBC

SQLiteJDBC necesita algunas modificaciones para su correcta integración con gvSIG. Para ello se mantiene un [fork público del proyecto](https://github.com/cartolab/sqlite-jdbc). Desde la última revisión ha habido 47 commits en el proyecto original (de [754c670](https://github.com/xerial/sqlite-jdbc/commit/754c670682994e9906e577b9a2521e3cfb70b2e1) a [1182886](https://github.com/xerial/sqlite-jdbc/commit/118288600bef15c4557cae901e73af3f14c80f7e). Tras examinar los cambios y ejecutars tests se actualiza la versión de SQLiteJDBC usada en libSpatialite a la última versión disponible en master.


La versión actualmente usada de SQLiteJDBC es: sqlite-jdbc-3.9.1-SNAPSHOT.jar
La versión de SQLite soportada por este conector es la 3.9.1
La versión de SQLite más actual es la 3.12.2

## Binarios de spatialite

spatialite es un módulo para sqlite que debe ser cargado dinámicamente en la conexión a sqlite mediante comandos específicos del conector o una query propia de SQL

```sql
SELECT load_extension('mod_spatialite');
```

Donde mod_spatialite son las librerías nativas en forma de dll para windows o .so para linux. Las librerías nativas deben estar en el path. Además la versión de SQLite usada debe haber sido compilada con soporte para módulos y spatialite (que es lo más habitual)

        
### Actualización de las librerías nativas

Los mantenedores de spatialite sólo proporcionan binarios para windows ([x86](http://www.gaia-gis.it/gaia-sins/windows-bin-x86/) y [amd64](http://www.gaia-gis.it/gaia-sins/windows-bin-amd64/)). Para linux deben [compilarse de forma manual](http://www.gaia-gis.it/gaia-sins/linux_how_to.html) u optar por reaprovechar los binarios ya compilados en un paquete de alguna distribución.

Debe tenerse en cuenta que a su vez el binario de spatialite tiene dependencias de versiones específicas de otras librerías nativas, por lo que todas las dependencias nativas deben incluírse en la distribución de gvSIG-libSpatialite. Si se delega en las librerías del sistema se puede incurrir en errores difíciles de detectar.

En Windows, basta con descargar el fichero mod_spatialite-XXX-win-x86.7z. Dado que tanto el binario de spatialite como el resto de dependencias binarias vienen incluídas en el fichero. Y actulizar es sólo substituir unos ficheros por otros. Las instrucciones de como [compilar los binarios para windows](http://www.gaia-gis.it/gaia-sins/mingw_how_to.html) dan más información sobre las dependencias y versiones

spatialite no especifica con que version de sqlite es compatible pero solicita una superior a la: 3.7.3 y los binarios de windows (v4.3.0a) se compilan contra SQLite v1.12

En Linux se puede optar por revisar los paquetes de Ubuntu para spatialite. Desde la página principal podemos hacer la [búsqueda por versión o por paquete](http://packages.ubuntu.com/). Y a partir de la información disponible bajar el paquete y las dependencias necesarias. Descomprimirlos e incluirlos en nuestros propios builds. Debe tenerse en cuenta que existen (fundamentalmente) [dos paquetes distintos](http://permalink.gmane.org/gmane.linux.debian.gis/2223):

* libspatialite7: Usado cuando la aplicación se "linka" directamente con spatialite
* libsqlite3-mod-spatialite: El módulo a cargar de forma dinámica en SQLite.

Téngase en cuenta que si cambian los números de versiones debe actualizarse es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.SpatiaLiteDriver

La versión de [spatialite disponible para la última LTS](http://packages.ubuntu.com/xenial/libsqlite3-mod-spatialite) de Ubuntu (16.04 Xerial) es la [4.3.0a](https://launchpad.net/ubuntu/xenial/i386/libsqlite3-mod-spatialite/4.3.0a-5)

Tras efecutar varios tests de forma satisfactoria se actualizan las librerías nativas de gvSIG-libSpatialite a la última versión estable disponible (4.3.0a)



## Máquina virtual de Java

gvSIG 1.12 trabaja con la jre 1.6. Si bien está jre ya no es mantenida por Oracle, actualizar gvSIG a una nueva versión requiere de un esfuerzo elevado. Hasta ahora las versiones portables de gvSIG para los proyectos que usaban libSpatialite (gvSIG-PMF y gvSIG-Fonsagua fundamentalmente) empaquetaban la versión 1.6.0.21 de la jre para Windows y la versión 1.6.0_20 para Linux.

La última [versión disponible de la jre](http://www.oracle.com/technetwork/java/javase/downloads/java-archive-downloads-javase6-419409.html) es la 1.6.45

Se [actualiza](https://github.com/cartolab/create-gvsig-portable/commit/0b07758e1afb3676fe83bbb0e8bf8a8a434891f4) el sistema de creación de portables para usar las nuevas versiones y se hace público bajo licencia GPL v3.


## Actualización del código compilado

El bytecode generado para la jre de las aplicaciones portables de los proyectos que usaban el driver (gvSIG-PMF y gvSIG-Fonsagua fundamentalmente) era compilado contra la versión 1.5 de la máquina virtual por motivos de compatibilidad. Tras efectuar varios tests se decide generar bytecode con target para la [1.6 -v50.0-](https://blogs.oracle.com/darcy/entry/source_target_class_file_version). No se aprecían mejoras de rendimiento pero tampoco ningún error y se prefiere mantener todo el sistema lo más actualizado posible.

Se [actualiza](https://github.com/cartolab/create-gvsig-portable/commit/6bb7b8eb875c27d059bf0a2fb282b75cd823ee9e) el sistema de creación de portables para usar el nuevo target.

