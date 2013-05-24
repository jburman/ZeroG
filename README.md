ZeroG - For building high performance, data intensive .NET applications.
========================================================================

ZeroG is a free, open source collection of libraries for use with .NET based 
applications. The source code is available on GitHub and may be forked and 
modified as desired.

Version Information
-------------------
ZeroG is still a work in progress and is progressing towards an initial beta release in the near future.
The majority of ongoing work is around the ZeroG Object Store framework, which provides high speed 
data storage capability along with convenient features common in relational database systems such as indexes and a query language.
Changes are only committed to `master` once they have been completed and unit tested. The software is 
fairly stable and reliable even though it is still in beta development.

Major Components
----------------

There are several major libraries that make up the ZeroG library collection.

### ZeroG.Lang

Programming language utilities including support for `JSON` and `BSON` parsing as well as `binary` value formatting.

  * Feather-weight, high speed JSON/BSON tokenizer, walker, and validator
  * Can parse thousands of JSON objects per millisecond on commodity hardware and supports indefinite object nesting with high performance.
  * JSON/BSON selector - supports a sub-set of JSONPath to allow values to be extracted from JSON/BSON documents.
  * Binary helper that allows binary values to be converted to and from hexadecimal strings efficiently.

### ZeroG.Data

Provides a generic interface for working with relational databases, and also provides a robust framework
for storing and indexing objects.

  * `DatabaseService` simplifies working with databases by providing a generic interface along with convenience methods.
  * `ObjectService` provides a hybrid Key/Value store and Indexer. The Indexer currently utilizes any relational database for which there is a ZeroG.Data interface.

NOTE: The ObjectService relies heavily on [RazorDB](https://github.com/gnoso/razordb), which is an open source, .NET based Key/Value store.

The ObjectService provides a hybrid approach between a Key/Value store and relational database, and it is optimized to 
cache object indexes as long as possible. The intention is to capture the speed of the NoSQL approach but retain 
the flexability to perform index lookup operations.

Key Capabilities

  * Store, retrieve, and index thousands of objects per second.
  * Build "Object Stores" on the fly that hold object metadata, values, and indexes.
  * Store Objects in a high-speed Key/Value store.
  * Index the Objects with arbitrary index values by utilizing existing RDMS drivers.
  * Query indexed objects using a JSON-based constraint language.
  * SQL tables are generated on the fly to hold index values.
  * Index query caching optimizes usage of key/value store.
  * Bulk insert and index objects efficiently.
  * Full backup/restore capability of object stores and index values.
  * Backup/restore utilizes compressed files with a portable data format (i.e. data can be backed up from one database engine and be restored into another.)

### ZeroG.Database.MySQL

An implementation of the DatabaseService and Object Store Indexer for MySQL.

### ZeroG.Database.SQLite

An implementation of the DatabaseService and Object Store Indexer for SQLite.

### ZeroG.Database.SQLServer

An implementation of the DatabaseService and Object Store Indexer for Microsoft SQL Server.

### ZeroG.DataClient

A library intended to be used by applications utilizing the ObjectService.

### ZeroG.Tests

Unit tests for all of the above projects.