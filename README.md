# connector-jdbc-example
Example of a custom JDBC connector build using the Polygon JDBC connector abstract supertype.

This example was created and tested for one table using Oracle database server.

If you use Oracle database server, you need to add jar file of OJDBC Driver to tomcat library to directory (Tomcat home)/lib. Table has to contain columns with names "id", "username" and "password".

Jar file is possible to download from link: <a href="http://www.oracle.com/technetwork/database/features/jdbc/jdbc-ucp-122-3110062.html">ojdbc8.jar</a>

If you develope your own connector, you should copy Messages.properties to your project, because jdbc superclass uses Messages.properties to create descriptions of configuration attributes.