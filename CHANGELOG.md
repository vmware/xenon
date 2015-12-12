# CHANGELOG

## 0.3.2-SNAPSHOT

* Upgrade to Lucene 5.3.1 from 5.2.1. The Lucene file format is
  backwards compatible, so this is a transparent update. 

## 0.3.1

* Indexing I/O path optimizations producing 20% throughput
increase. Removed ServiceOption.INSTRUMENTATION from
LuceneDocumentIndexService. It can be toggled through /config
at runtime.

* Referer is no longer indexed with every document update

* All Operation request and response headers are converted
to lower case before being added to request/response maps

* Tunable query limit for service load during factory service
restart and during synchronization. A large result limit will
increase the service load throughput and decrease overal time
for factory child services becoming available. Large limits
can impact memory and network utilization however.

* Add keyPassphrase command line argument for providing SSL private key
passphrase.

## 0.3.0

* Renamed ServiceOption.EAGER_CONSISTENCY to ServiceOption.ENFORCE_QUORUM

* Renamed packages, build artifacts and various properties to
Xenon (from DCP). This is a runtime and binary breaking change
since HTTP headers, runtime properties, command line arguments
have changed.

## 0.2.0

* Request rate limiting support using authorization subject.
New serviceHost.setRequestRateLimit method and rate tracking
logic added to ServiceHost.

* Remove Operation.joinWith and associated support code in service
client. Joins are created and sent with OperationJoin.create() and
sendWith.

* Add Claims class to common package that extends the Claims in the jwt
package to include a properties map. This can be used to carry arbitrary
session state.

* Add QueryOption.CONTINUOUS so query tasks can be used for real time
update notifications with queries spanning the entire index

* Add ServiceDocument.documentUpdateAction so each state version is
associated with the action that caused the change. Also allows
subscribers to continuous query tasks to determine if a notification
was for a deleted state. This is a breaking change, please delete
existing lucene index

## 0.1.1

* Service authorization.

* Basic authentication.

* Allow booleanClauses to have single clause.

* Implement sort for QueryTask.

* Add QueryFilter for in-place evaluation of query specifications against documents.

* Add support for JavaScript DCP services connected over WebSocket.

## 0.1.0

* Start of CHANGELOG. See commit history.
