ZeroG - .NET and JavaScript software libraries
==============================================

ZeroG is a free, open source collection of libraries for use with .NET based 
applications. The source code is available on GitHub and may be forked and 
modified as desired.

Version Information
-------------------
ZeroG is still a work in progress and is progressing towards an initial beta release in the near future.
The majority of ongoing work is around the ZeroG.Data Object storage framework.
Changes are only committed to `master` once they have been completed and unit tested.

Major Components
----------------

There are several major libraries that make up the ZeroG library collection.

### ZeroG.Lang

Programming language utilities including support for `JSON`, `BSON`, and `binary` value formatting.

  * Light-weight, high speed JSON/BSON tokenizer, walker, and validator
  * JSON/BSON selector - supports a sub-set of JSONPath to allow values to be extracted from JSON/BSON documents.
  * Binary helper that allows binary values to be converted to and from hexadecimal strings efficiently.

### ZeroG.Data

Provides a generic interface for working with relational databases, and also provides a robust framework
for storing and indexing objects.

  * `DatabaseService` simplifies working with databases by providing a generic interface along with convenience methods.
  * `ObjectService` provides a hybrid Key/Value store and Indexer. The Indexer currently utilizes any relational database for which there is a ZeroG.Data interface.

The ObjectService provides a hybrid approach between a Key/Value store and relational database. The intention 
is to try to capture the speed of a NoSQL approach but retain the capability to perform certain operations 
efficiently. The ObjectService is mainly optimized for reading data as fast as possible and hence it has an 
Object Versioning and caching scheme built in.

  * Build "Object Stores" on the fly that hold object metadata, object values, and indexes.
  * Store Objects in a high-speed Key/Value store.
  * Index the Objects with arbitrary index values.
  * Query indexed objects using a JSON-based constraint language.
  * Provides a JSON to SQL compiler.
  * SQL tables are generated on the fly to hold index values.
  * Index query caching.
  * Backup/Restore
  * Bulk insert

### ZeroG.Database.MySQL

An implementation of the DatabaseService for MySQL.

### ZeroG.Database.SQLServer

An implementation of the DatabaseService for Microsoft SQL Server.

### ZeroG.DataClient

A library intended to be used by applications utilizing the ObjectService.

### ZeroG.Tests

Unit tests for all of the above projects.