# CHANGELOG

## 0.6.1-SNAPSHOT

* Add FactoryService.create(), update ExampleService.
  Service authors no longer need to implement a factory service
  and derive from FactoryService class. Instead, they can
  just use FactoryService.create() to get a default, functional
  factory service instance which can then be started with
  ServiceHost.startService()

* Enable update of synch quorum through PATCH to
  node-groups/<group>/config. Membership quorum was already
  supported, now synch quorum is included.

* Fix consensus protocol commit behavior. Commit messages
  are no longer sent if the request to owner service fails
  or the service does not have ENFORCE_QUORUM.

* Split Service.handleDelete into Service.handleStop and
  Service.handleDelete. If the service is being stopped, the
  host has always added a special pragma, indicating this is
  a DELETE with the intent to stop a service, and to avoid
  any persistence or replication side effects.
  This is now formalized through the new handleStop method,
  invoked only when the service is being issued a DELETE, with the
  pragma included. This is potentially a breaking change for
  services that did special cleanup ONLY on stop, not delete+stop

## 0.6.0

* Add new per service utility suffix, /available. Provides a
  consistent mechanism for a service to declare it is available,
  re-using the underlying stats support. By default, all services
  that are started and in ProcessingState.AVAILABLE will return
  200 (OK) on GET /<service/available

* Add service host set/get service cache clear delay so cache clears are
  configurable

* Fixed bug: when DELETEs occurred during network partition, they
  would not be correctly synchronized after nodes rejoined.

## 0.5.1

* Enable HTTPS support for node groups. Several fixes in URI
  manipulation now allow Xenon hosts to start with just a HTTPS
  listener and use exclusively HTTPS for node group operations,
  including join, maintenance, replication and forwarding

* Add ServiceHost.Arguments.resourceSandbox to speed up UI
  development experience. When specified, xenon will publish
  the UI resource files directly from the file folder, allowing
  for real time edits / refresh of UI content

## 0.5.0

* Implement authorization on stateless services. This is a breaking
  change for xenon deployments that use authorization and have 
  non factory service implementations of StatelessService class.
  Stateless services must now be included in roles, using a resource
  specification over the document self link.

## 0.4.1

* Updated the API for backing up or restoring the index. The Bearer
  parameter is now named bearer (the capitalization changed). While
  this is a breaking change, we believe this is unused and should be
  harmless.

* Added new ServiceOption: CONCURRENT_GET_HANDLING. This is (and has
  been) the default, but can now be disabled when appropriate.

* Added SLF4J-Xenon bridge

* QueryTasks that are broadcast queries can now be direct tasks.

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
