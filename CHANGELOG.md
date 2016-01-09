# CHANGELOG

## 0.4.1-SNAPSHOT

* Updated the API for backing up or restoring the index. The Bearer
  parameter is now named bearer (the capitalization changed). While
  this is a breaking change, we believe this is unused and should be
  harmless.

## 0.4.0

* Added default support for HTTP Options. StafulService and
StatelessService now return the same content as GET on
*/template suffix, providing a default self documentation
capability.

* Added honoring of verbs (POST, PATCH, etc) in roles

* Renamed authentication cookie from dcp-auth-cookie to
  xenon-auth-cookie. This should be transparent for clients that use
  cookies, but clients that extract the cookie value in order to make
  an x-xenon-auth-token header need to use this new cookie instead. 

* The basic authentication service (/core/authn/basic) now includes
  the x-xenon-auth-token header in its response, in addition to the
  cookie. 

* ExampleServiceHost now takes arguments to create users (one admin,
  one non-admin that can access example services). This enables you to
  use the ExampleServiceHost when authorization is enabled with the
  isAuthorizationEnabled flag. The new arguments are:

  - adminUser
  - adminUserPassword
  - exampleUser
  - exampleUserPassword

## 0.3.2

* Add on demand service load, enabled with
ServiceOption.ON_DEMAND_LOAD

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
