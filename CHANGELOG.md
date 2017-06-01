# CHANGELOG

## 1.5.2-SNAPSHOT

* "offset" parameter is added to QueryTask. This enables pagination logic to
  be executed in QueryTaskService rather than caller visits next pages.
  Major usecase is to provide data for list style pages, and it is expected to
  be used with TOP_RESULTS query option and sorting term specified but not
  limited to.


## 1.5.1

* Providing an override for the method ServiceHost.startDefaultCoreServicesSynchronously
  to make joining peer nodes optional.

* Token verification during authentication now returns an AuthorizationContext
  object instead of a Claims object to ensure ServiceHost has access to the auth
  token

* Roll back to Maven 3.3.9 until https://issues.apache.org/jira/browse/MDEPLOY-221
  can be fixed.

* Breaking change: replace usage of Set with List in NodeGroupBroadcastResult
  to avoid edge case with equal results documents. In the event that
  ServiceDocument types properly implement `equals`, per-node results in
  NodeGroupBroadcastResult can contain fewer results than expected -- in
  particular, `getSuccessesAs` is vulnerable to this bug. For more details, see
  https://www.pivotaltracker.com/story/show/132633309

* MigrationTaskService has added two new parameters: "sourceReferences" and
  "destinationReferences".  When specified, migration directly use them as source
  and destination nodes without checking convergence of node-groups. Therefore,
  Caller should check convergence before starting migration.
  Also, migration task retrieves each document from its owner node. The
  "sourceReferences" needs to include all node uris in source node-group;
  Otherwise, only partial number of documents will be migrated.

* MigrationTaskService now sets a 'xn-from-migration' Pragma header when
  interacting with the destination cluster. Services can leverage this to alter
  their behavior based on whether handleStart comes from a migration or not.

* Breaking change: Add support for Accept-Encoding: gzip request header.
  When present the response body is gzipped and the response header
  Content-Encoding: gzip is added to the response.  Previously, the request
  header was ignored.

* Breaking change: Changed signature of Utils.encodeBody() (both overloaded methods)
  to take an additional 'isRequest' parameter.


## 1.5.0

* Breaking change: Links to QueryPageService instances (e.g. the nextPageLink
  and prevPageLink in ServiceDocumentQueryResult) are now provided through a
  dedcated forwarding service rather than the default node group forwarding
  service:

    http://host:port/core/query-page-forwarding?peer={id}&path={id}

  This change is transparent to most consumers -- the nextPageLink and
  prevPageLink links can be traversed as normal -- but callers who depend on
  the old format in some fashion can enable it using the new
  "xenon.UriUtils.DISABLE_QUERY_PAGE_FORWARDING" flag.

* Breaking change: The Lucene index service uses a new naming scheme for sorted
  DocValues fields in 1.5.0. This is transparent to most Xenon consumers, but
  is breaking for products which depend on opening an existing index at upgrade
  time instead of migration and which make use of sorted queries; such products
  should use the new "xenon.LuceneIndexDocumentHelper.DISABLE_SORT_FIELD_NAMING"
  flag to disable the use of the new naming scheme.

* Upgrade Netty to [4.1.10](http://netty.io/news/2017/04/30/4-0-46-Final-4-1-10-Final.html) and
  netty-tcnative to [2.0.1](https://github.com/netty/netty-tcnative/releases/tag/netty-tcnative-parent-2.0.1.Final)

* Remove `Utils.computeHash`, `Utils.getCurrentFileDirectory`, `Utils.atomicGetOrCreate`,
  `Utils.toHexString`, `Utils.toServiceErrorResponseJson` as there are
  obvious/shorter/standard ways to achieve the same. Methods `Utils.getOsName` and
  `Utils.determineOsFamily` are moved to `SystemHostInfo`.

* Upgrade Lucene to [6.5.0](http://lucene.apache.org/core/6_5_0/changes/Changes.html)

* Upgrade Gson to [2.8.0](https://github.com/google/gson/blob/master/CHANGELOG.md#version-28)

* In MigrationTaskService, calculation of migration target docs(available in
  "estimatedTotalServiceCount" stats) became optional and default is disabled.
  To enable getting estimate count, "ESTIMATE_COUNT" migration option needs to be
  set in migration request.


## 1.4.2

* Add ability to expand only selected fields in queries
  See QueryOption.EXPAND_SELECTED_FIELDS and querySpec.selectTerms
  also odata format: $select=<comma-separated-list-of-fieldnames>

* Add Service.get/SetDocumentIndexPath() enabling a service type to use
  a custom index service instance. This enables many new scenarios, for production
  or testing, since the per service instance indexing of the I/O pipeline can now use
  new types of indexing service. This is similar to the node selector per service path
  accessors and capability.

* Add InMemoryLuceneDocumentIndexService, which uses the lucene document index service
  code but relies on the lucene RAMDirectory index store, vs the memory mapped, file based
  writer. The in memory index service can be optionally started side by side with the
  durable index service and can be set as the default index service for service types using
  the new Service.get/SetDocumentIndexPath() accessors

* MigrationTask default page size is increased from 500 to 10,000.

* Switch QueryTaskService to use the 1X node selector, instead of the full replication
  selector. This greatly reduces the overhead of creating a query task, but still provides
  the owner selection load balancing benefit. This change should be transparent to users,
  since all requests to a query task are forwarded to owner (before and after this change).

* Isolate scheduled task execution for core services from any other service. This
  is a internal change with no visible API changes (backwards compatible).

* Add new ServiceOption.CORE reserved for critical runtime services (like the index service,
  node selector, node group management). The option is currently only used to schedule
  tasks using the private core scheduled executor.

* Xenon UI: Introduce pagination on factory services and query results,
  which allows the UI to handle large amount of documents.

* "VerificationHost#setTimeoutSeconds()" propagates specified timeout to local
  peer nodes if exist.

* All variants of "ServiceHost.initialize()" methods no longer call "setProcessOwner(true)" as part of
  initialization. This changes the shutdown behavior via DELETE to "/core/management".
  If application needs "System.exit" to be called at shutdown, it is now required to explicitly call
  "setProcessOwner(true)" in application code.

* Incremental backup of document index.
  "/core/management" endpoint now accepts backup request(PATCH) with local directory specifying "backupType=DIRECTORY".
  When destination directory contains previous snapshot, it will perform incremental backup.
  See the detail on wiki page: https://github.com/vmware/xenon/wiki/Backup-Restore

  Using backup/restore API on LuceneDocumentIndexService directly is deprecated.
  API on ServiceHostManagementService must be used.

* Document signature is computed differently and this may cause issues if signatures are
  stored externally.


## 1.4.1

* Fix migration task bug that may cause data loss when combined with transformation service.
  https://www.pivotaltracker.com/story/show/141885113

* Extend per service config REST API to include the node selector path.
  Add ServiceConfiguration.peerNodeSelectorPath field, reflecting the value set
  on replicated services instances, through the existing Service.setPeerNodeSelectorPath()

## 1.4.0

* Move ServiceHost.failRequestXXXX methods to Operation.failXXXX equivalents

* Remove per-host connection limit APIs in ServiceClient. Consumers who
  wish to set connection rate limits should use the per-tag APIs instead.

* ServiceHost.failRequestXXX methods have been made static so they are
  useful without a host reference

* Add pending request queue limit on ServiceClient interface and NettyHttpServiceClient
  implementation. Default is configured through JVM parameter.

* Additional levels of Operation tracing, making operation inspection
  easier. The tracing toggle PATCH request to /core/management now takes
  a new level property that determines the operation tracing behavior:
  Level.Fine - Operations are tracked through time series stats available on
  /core/management/stats named as follows: <service path>:<action>
  Level.Finer - Operations are logged to the java logging output
  Level.Finest/Level.All - Operations are deeply indexed in the core/operation-index
  (this was the only level of tracing until now)

* Added special handling for `doc.documentExpirationTimeMicros` field in `Utils.mergeWithState(desc,source,patch)`.
  Previously, source document expiration time was ALWAYS updated by patch expiration time.
  Special handling is added to **patch** expiration time.
  When **patch** expiration time is 0, **source** expiration time will NOT be updated.
  Any other value is set, then source expiration time will be updated to that value.

* Remove `VerificationHost#createExampleServices(ServiceHost, long, List<URI>, Long)`.
  Instead of passing `List<URI> exampleURIs`, overloading methods returns list of full uris.

* Added local file support for ServiceHostManagementService backup/restore request.
  "destination" URI parameter now takes "file" scheme for local file in addition to "http" and "https".


## 1.3.7

* For factories using setUseBodyForSelfLink, FactoryService will
  fail the request with 400 (BAD REQUEST) if body is missing in request.

* Add TestRequestSender sendStatsGetAndWait helper method

* Add convenience method Operation.getErrorResponseBody() that retrieves
  a type ServiceErrorResponse if its present in the operation body.

* Remove LuceneBlobIndexService. The service was originally used for binary
  serializing service context in pause / resume, which now uses a custom
  file based service (since 1.2.0)

* The index service will now purge all document versions for a previously
  deleted self link, if its recreated with POST + PRAGMA_FORCE_INDEX_UPDATE.
  This avoids duplicate versions, for the same self link appearing in the index,
  which can happen due to synchronization or migration (even if the runtime does
  do a best effort to increment the version when a self is recreated)

* Added support for simple ODATA filtering on service stats, using URI query parameters.
  For example:
  GET /core/document-index/stats?$filter name eq indexedDocumentCount
  GET /core/document-index/stats?$filter name eq indexedDocument*
  GET /core/document-index/stats?$filter name eq *PerDay
  GET /core/document-index/stats?$filter name eq *PerDay and latestValue ge 1000.0

* Added support for QueryOption#TIME_SNAPSHOT. The new query option will return results that contain
  latest versions of documents as on a given time. QuerySpecification#timeSnapshotBoundaryMicros
  will allow specifying the time.

* Fix bug with JWT token timestamps being interpreted as micros instead of seconds. As the default auth
  provider in Xenon doesn't persist tokens anyway this is not a breaking change.

* Update version retention limit through service config update for the service instance.

* Added support for QueryOption#EXPAND_BINARY_CONTENT. The new query option will return results
  that contain documents in the binary form. This option should only be set on
  local query tasks, with the client co-located (same service host) as the query task. This should
  not be used along with EXPAND_CONTENT / EXPAND_BUILTIN_CONTENT_ONLY / OWNER_SELECTION.

## 1.3.6

* Integrations with 3rd party libraries are migrated to a [new repository](https://github.com/vmware/xenon-utils)
  and now have their own release cycle. There are no API changes but the Maven coordinates are different. Update
  your pom.xml's using this table:

  | Old Dependency| New Dependency |
  |---|---|
  | com.vmware.xenon:xenon-swagger:1.3.6 | com.vmware.xenon:xenon-swagger-adapter:0.0.1 |
  | com.vmware.xenon:xenon-slf4j:1.3.6   | com.vmware.xenon:slf4j-xenon:0.0.1  |

* Introduce "Operation Tracing" feature to Xenon UI that allows users to trace
  operations sent or received by a service host instance via an interactive query builder
  and examine results visually.

* Fix groupBy on numeric fields. When annotated with PropertyIndexingOption.SORT,
  add a SortedDocValuesField for the numeric property. The change has no impact on
  how query specification is written. However a blue-green update is necessary in
  order for previously indexed documents to be queried using groupBy on a numeric field.
  Documents which match the query but have the groupBy term missing are returned
  under a special group "DocumentsWithoutResults".

* Remove Utils.toServiceErrorResponse(Throwable t, Operation op).
  Utils.toServiceErrorResponse(Throwable t) or
  toValidationErrorResponse(Throwable t, Operation op) can be used. The
  Operation argument is needed for localization of the error.

* Added support for index upgrade from pre 1.1.1 version. Should be used only as a last resort.
  If the `xenon.kryo.handleBuiltInCollections` system property is set to false, index contents
  can be read back ONLY from pre-1.1.1 created indeces. If documents don't hold instances
  created by Collections.emptyList() and friends upgrade will still be possible
  without using this property.

## 1.3.5

* Add a new GatewayService to facilitate with blue/green upgrades of xenon
  node-groups. The GatewayService can be used during upgrades to pause incoming
  traffic while data is being migrated to the new node-group. Once data has
  been migrated, the gateway can be resumed to point all incoming traffic to
  the new node-group.

* Remove xenon-client from xenon project. The same is available in xenon-utils.
  Also removed GO toolchain in xenon-common.

* Enable HTTP/2 over TLS. Xenon consumers can enable this behavior by including
  a netty-tcnative jar (for OpenSSL) or a jetty-alpn jar (for JDK support) in
  their classpath. Otherwise, the default behavior of using HTTP1.1 for all
  HTTPS operations is maintained.

* Implement version lookup cache inside the document index service. This is
  a significant optimization for general queries that return lots of results
  over documents with multiple versions. This change is transparent to clients
  and does not change the document query API

## 1.3.4

* Enforce implicit query result limits. All queries
  that do not specify a resultLimit, including GETs to factory services, will
  now be limited to the default, which is 10,000 results. For well behaved
  clients, this change will be transparent. For clients that issued queries
  that returned large number of results, without pagination enabled, this change
  will be noticed and will *fail* their queries.
  See https://www.pivotaltracker.com/story/show/130467457

* NodeSelectorStatus.nodeSelectorStatus has been renamed to status, so
  it is consistent with NodeState.status. This is a in-memory infrastructure
  service and the field was just added in 1.3.0, so this change should have
  minimal side-effects

* Change the contract for infrastructure method Utils.decodeBody(). It
  no longer calls operation complete or fail in the supplied operation. The
  method will now throw an exception on decoding error

* Introduce ServiceStateMapUpdateRequest to allow deleting map items in a
  service state through a PATCH request. Passing a null map value in a
  regular service state PATCH body to delete the corresponding map key does
  not work in remote requests (due to gson serialization ignoring null
  values).

* Add builders for UserGroupState, RoleState, ResourceGroupState

* The method `ServiceHost.nextUUID()` is now the preferred way to generate IDs.
  `ServiceHost.getNodeIdHash()` has been removed.

## 1.3.3

* Bug fixes in QueryFilter when matching null strings.

* Bug fix when clearing the AuthZ cach for a user.

## 1.3.2

## 1.3.1

* Remove OperationOption.SEND_WITH_CALLBACK. This was an experimental custom
  protocol that allows re-use of the same http connection across multiple
  requests. Same functionality as HTTP/2 but supported on HTTPS. Default
  protocol on HTTPS will now be HTTP1.1 even if OperationOption.CONNECTION_SHARING
  is enabled. Future support for HTTP/2 with TLS will provide connection
  re-use in HTTPS connections

* Fix connection keep-alive behavior. The http client was not keeping
  connections alive because the default Operation options did not have
  OperationOption.KEEP_ALIVE. This is a transparent optimization/fix

* Completed AuthZ Role Policy support to handle DENY option.

* Added methods to NodeSelectorState to support a 'PAUSE' state for the
  NodeSelectorService (ConsistentHashingNodeSelectorService).

* Created a SampleAuthenticationService outlining the contract between
  framework and a custom authentication service

* NodeSelectorService.getNodeGroup() is renamed to getNodeGroupPath()

* Custom node selector implementations must queue SelectAndForwardRequest instance
  before queuing or using in a different thread context

* Document version retention now uses a watermark approach by which old
  versions of a document can be deleted in bulk once the retention threshold is
  reached.

* The default consistent hashing selector now uses a FNV hashing algorithm,
  replacing the use of Murmur3. Better performance, no collisions for millions
  of keys, better key distribution across nodes. Utils.computeHash uses FNV
  now as well, and so does Utils.computeSignature. This should be a transparent
  change except in one situation: If nodes are updated in place, owner selection
  will re-assign documents on node restart

* Default logger format now produces UTC-offset, rfc3339 formatted strings,
  for example 2016-11-09T18:55:36.037Z

* Added thread id to log formatter.

* Added support for external authentication in xenon.
  You can now:
  1) Redirect unauthenticated requests to an external oauth server for authentication
  2) Ability to verify access tokens generated from a configured oauth server

* Removed public method setAuthorizationContext(ServiceHost, Operation) from
  OperationContext.java

* Added a new UiFileContentService which provides a simpler internal implementation
  and enables hosting of UI resources in the root application namespace

* Support sorting by multiple fields using query task. The order(asc/desc) methods
  on QueryTask builder can be called multiple times to select more than one sort
  fields. QueryTask.QuerySpecification now includes additionalSortTerms and
  additionalGroupSortTerms to capture the same.

## 1.3.0

* Document signature calculation changed to be more efficient while format stays
  the same. If an older signature is compared to a new one documents may
  incorrectly be reported as different.

## 1.2.0

* Note: The release is not recommended for production.
  Document index service leaks one file descriptor every 5 seconds.

* Roll back to Lucene to version 6.0.0 due to an issue in 6.2.1. See:
  https://issues.apache.org/jira/browse/LUCENE-7491

* Improve Service.setRequestRateLimit methods. Apply rate limits only
  on requests originating from remote nodes

* Operations received from remote clients now have the
  OperationOption.REMOTE set.

* NodeGroupService.UpdateQuorumRequest PATCH no longer returns the service
  state as the response. Any subscribers would loose the ability to
  determine what the PATCH was for.

* LuceneDocumentIndexService's "backup" API now provides access to the
  backup .zip file location in its `BackupResponse`

* Remove xenon-dns module. The DNS service did not make enough progress
  to be a full featured registry with native DNS, hence the change.

## 1.1.1

* Note: This release has a bug where Lucene will occasionally throw MergeExceptions
  due to https://issues.apache.org/jira/browse/LUCENE-7491. This release should not
  be used in production.

* Reduce memory usage under heavy query load, using single index searcher
  per thread, in LuceneDocumentIndexService. See:
  https://www.pivotaltracker.com/n/projects/1471320/stories/133188647

* Optimize result processing for services with ServiceOption.IMMUTABLE.
  Skip latest version lookup and validation during query result processing
  resulting in 3x throughput increase (links processed / sec)

* Add Utils.getSystemNowMicrosUtc() which can be used for expirations or
  whenever a non unique time value is sufficient

* Add Utils.beforeNow convenience method to check if a absolute time value
  is in the past

* Throw exception in Utils.getNowMicrosUtc() if detect large drift between wall
  clock / system time and the xenon sequence value

* Remove time-series point stats in LuceneDocumentIndexService in favor of
  the existing per-day / per-hour statistics.

* Modify the handling of time-series stats aggregated by SUM to record the
  delta when using adjustStat rather than the updated value.

* Validate service options for Stateless services.
  Using PERSISTENCE, REPLICATION, OWNER_SELECTION, STRICT_UPDATE_CHECKING
  will fail service start.

* Support serialization in ServiceDocument of collections/maps obtained using
  Collections.empty*, Collections.singleton* and Arrays.asList().

## 1.1.0

* Note: This release has a bug where Lucene will occasionally throw MergeExceptions
  due to https://issues.apache.org/jira/browse/LUCENE-7491. This release should not
  be used in production.

* Remove infrastructure Utils.toDocumentBytes/fromDocumentBytes and related methods,
  KryoSerializers.serialize/deserializeObject are used instead

* Pause/Resume now happens only for services with ServiceOption.ON_DEMAND_LOAD.
  Independent of memory usage, a ODL service will be either stopped or
  paused to disk, the moment we determine its idle. If the service has stats
  or subscriptions, it will be paused/resumed. If it has neither, it will be
  simple stopped and on demand started

* Upgrade Lucene from 6.0.0 to 6.2.1

* Service pause/resume improvements:
  - New, simple, file based key/value service context index for storing
    service serialized instance
  - Service.setProcessingStage now atomically serializes service state
    if processing stage is PAUSED
  Service.setProcessingStage() now returns ServiceRuntimeContext

## 1.0.0

* Improve Xenon UI's' node selector to support multi node group use cases.

## 0.9.7

* Add new ServiceOption.IMMUTABLE, for services/documents that have
  write once, read many semantics (metrics, logs, immutable configuration).
  Several times faster in terms of POST throughput, for indexed services,
  since it avoids previous version lookup, version retention. More optimizations
  will be made in the near future, in a transparent fashion.

* Use new scheme for generating random self links on factory POST. Instead
  of UUID.randomGuid(), now use Murmur3 hash of host id plus local, non repeating
  time in micros. Its 20x faster than the UUID scheme.
  Add Utils.buildUUID() that uses the new scheme.

* Fix race condition in authz cache clear when a user access request
  interleaved with a cache clear operation would result in a stale
  authz cache

* Authorize all requests on default notification target services created in
  ServiceHost.startSubscriptionService()

* Add "Queries" to Xenon UI to enable building query specifications
  interactively and reviewing results.

## 0.9.6

* Note: This release has a regression where the documentKind field of
  a ServiceDocument is not set in certain conditions. This release should
  not be used for production

* Add errorCode property to ServiceErrorResponse to allow Xenon services
  to indicate a specific error case that otherwise cannot be expressed
  through the status code. For xenon's internal errors the range 0x8xxxxxxx
  is reserved.

* Add query operation queues per authorized subject in the index service. New
  scheme offers fair scheduling across users and mitigates head-of-line
  queuing problems. The support is transparent to all existing code and works
  with a single queue (for system context) if authorization is disabled.

* Introduce Xenon UI, a built-in application which federates information
  from nodes across the node groups and visualize them in a single pane of UI.
  With this UI you can:
  1) Navigate to any node within the node groups and browse node-specific document contents.
  2) Add/Edit/Delete instances with test payloads.
  3) Extract logs that are specific to each node.
  *NOTE*: Xenon UI has only been developed and tested against Chrome as of now. Browser
  compatibility improvements will be done in future releases.

## 0.9.5

* Add PREFIX option in QueryTerm.MatchType to support string prefix field queries

* Add new SynchronizationTaskService. An instance of this task is created per
  FactoryService that acts as a dedicated task synchronizing child-services of
  the associated factory. This also simplifies the FactoryService by moving
  synchronization functionality to the new task. No functional change, since
  synchronization still works the same way.

* Add host "location" property, set via system property xenon.NodeState.location.
  In case of OWNER_SELECTION, if the owning node is part of a location, quorum
  is required among hosts in the same location. This is helpful in multi-geo
  deployment, where strong consistency within a region (with eventual consistency
  across regions) is required.

* Add QueryOption.EXPAND_BUILTIN_CONTENT_ONLY, used by synchronization code for
  state comparison

* Change ServiceDocumentQueryResult.selectedDocuments from Map<String,String> to
  Map<String,Object> making it the same as the Map<String,Object> documents field.
  This avoid ugly escaping of the document content which was already JSON

* Add support for case insensitive queries. New PropertyIndexingOption.CASE_INSENSITIVE
  instructs the index service to index the field in lower case. The original field
  content is preserved in original case. At query time, the field match value
  must be converted to lower case. New helper methods added to hide the toLower()
  conversion when using the Query.Builder().

* Add a new sendWithDeferredResult pair of methods to the request senders
  The new sendWithDeferredResult set of methods return DeferredResult
  instances to enable monadic style of chaining potentially asynchronous
  code blocks. Check the refactored SamplePreviousEchoService and
  SampleBootstrapService for usage examples.
  The implementation of DeferredResult encapsulates CompletableFuture with
  similar interface, excluding the blocking methods.

* Update "VerificationHost#waitForNodeGroupConvergence" logic to use "NodeGroupUtils"
  instead of own implementation to check the network convergence. This change makes check
  logic in test to align with production code.
  To keep the existing code to work, API signature was kept same but most of the parameters
  are now unused.

* Implicitly add PropertyUsageOption.LINKS and PropertyIndexingOption.EXPAND annotations
  on service document fields which name ends with "Links".
  Remove the ServiceDocumentDescription.expandTenantLinks() method as it is no longer needed.

## 0.9.4

* QueryOption.INCLUDE_ALL_VERSIONS now includes deleted versions,
  no longer requiring INCLUDE_DELETED to be also set. So it actually
  trully returns *all* versions

* Add support for per thread user defined kryo serializers. See method
  Utils#registerCustomKryoSerializers

* Improve QueryFilters to support numeric document properties

* Add TestRequestSender to provide synchronous API for writing unit tests.
  This class is recommended for sending operations instead of sendXxx() method
  variants in VerificationHost. All synchronous methods return response operation
  or body document which eliminates the use of TestContext in test code.
  Please reference the TestExampleService for usage.

* Add stat for tracking number of times maintenance was ran for the ServiceHost.

## 0.9.3

* Add a flavor of mergeWithState() that processes a
  ServiceStateCollectionUpdateRequest object to add and remove
  elements from a collection

* Add Operation#appendCompletion method. This method adds completion handler
  in FIFO style. This is symmetric to nestCompletion which adds completion
  handler in LIFO. See the javadoc for details.

* Add time series stats for thread count, CPU, memory, disk in the
  existing stats tracked by /core/management/stats

* Add support for new GraphQueryOption.FILTER_STAGE_RESULTS. The option
  enables automatic filtering of results across query stages, only keeping
  documents that contributed to results in the final stage. It essentially
  walks the graph backwards and removes all edges and nodes that did not
  lead to a graph node in the final stage

* Support ODATA custom query options to specify tenantLinks. The
  query param will be used to populate the tenantLinks field of
  the underlying QueryTask

* Rename TimeSeriesStats DataPoint data structure to TimeBin

* Add support for groupBy queries. The new QueryOption.GROUP_BY, in
  combination with the QuerySpecification.groupByTerm, groupSortTem
  and groupSortOrder, allow for grouping, by field values of document
  results. The new ServiceDocumentQueryResult.nextPageLinksByGroup
  map contains links to the per group results, returning a QueryTask,
  similar to existing query pages, scoped to results for that group

* Improve MigrationTaskService's transformation contract to use a
  TransformRequest for the transform's input and a TransformResponse as the
  ouput (instead of serializing a Collection directly). Existing transform's
  work as before, but new contract should be preferred moving forward by
  specifying MigrationOption.USE_TRANSFORM_REQUEST.

## 0.9.2

* Using sourceTimeMicrosUtc field, if set, for time series aggregation.

## 0.9.1

* Add a new field *sourceTimeMicrosUtc* to ServiceStat to capture the
  time the data value was acquired at the source.

## 0.9.0

* Add sample code for Bootstrap Service Tutorial.
  https://github.com/vmware/xenon/wiki/Bootstrap-Service-Tutorial

* Move VerificationHost#findUnixProcessInfoByXxx,killUnixProcess to SystemUtils class.
  Referenced class(ProcessInfo) has moved to SystemUtils class as well.

* Clear the authz cache maintained in ServiceHost on all nodes in a multi-node
  deployment.

* Add support for graph queries. The new GraphQueryTaskService, listening on
  /core/graph-queries enables multi stage queries that can traverse linked
  documents. Each stage uses the QueryTask specification, to select the documents
  that serve as the graph nodes. The graph edges, that link nodes together are
  specified through link fields, and the of QueryOption.SELECT_LINKS, which is
  automatically enabled to return the set of document links that form the nodes
  for the next stage of the graph. Like regular query tasks, both direct, and
  asynchronous REST pattern models are supported on the task service instance.
  The following is an example of a two stage graph query
  Stage 0: Filter documents by kind eq ParentState. SelectLinks: childLink,nephewLink
  Stage 1: Filter documents by kind eq ChildState AND field name = Jimmy
  Since each stage is a full query, the results can be sorted, paginated, use
  complex boolean, nested expression trees, etc. They can even use broadcast
  for 3x replication scenarios.
  The graph queries are load balanced across nodes.

* Modify new QueryOption.SELECT_LINKS and QueryOption.EXPAND_LINKS behavior
  to properly de-duplicate expanded state for selected links and make the
  selection results similar to the existing documentLinks and documents,
  in usage and behavior.
  The selectedLinks map is now called selectedLinksPerDocument.

## 0.8.2

* Add global stats for total service pauses, resumes, cache clears
  and ON_DEMAND_LOAD service stops on /core/management service.

* Move a few infrastructure query related helper methods from Utils class
  to QueryTaskUtils. The mergeQueryResults family of methods are used by
  core services, so this should have minimal impact

* Add PropertyUsageOption.REQUIRED to describe fields that are required.

* Add validateState(description, state) to Utils to validate fields that
  are required. If the field is null, REQUIRED and ID, a UUID is automatically
  generated. If the field is null and REQUIRED, an Exception is thrown.

* Add QueryOption.EXPAND_LINKS for expanding selected link values with the
  target document state and including it in the query results. The serialized
  state, is placed in the results.selectedLinks map.

* Add PropertyUsageOption.SENSITIVE to describe fields that contain sensitive
  information. When marked, the field will be hidden when serializing to JSON
  using toJson(boolean hideSensitiveFields, boolean useHtmlFormatting).

* Add support for configurable auth expiration via JVM property or login request to
  BasicAuthenticationService.

* Use SEND_WITH_CALLBACK in NettyHttpServiceClient if the request was
  configured for connectionSharing and SSL

* Add QueryOption.SELECT_LINKS for selecting fields marked with
  PropertyUsageOption.LINK and including the link values in the query results.
  The link selection will be used for future graph query support and
  automatic link content expansion.

* Add transaction flow support, across related operations, similar to
  authorization context and context id flow support

* Rename LuceneQueryTaskFactoryService and LuceneQueryTaskService to
  QueryTaskFactoryService and QueryTaskService. Similar change for
  local query task service. Service code should use
      ServiceUriPaths.CORE_QUERY_TASKS
      ServiceUriPaths.CORE_LOCAL_QUERY_TASKS
  instead of the service SELF_LINK fields.

* Upgrade Netty from 4.1.0.CR7 to 4.1.0.Final

* Add new JVM properties in ServiceClient and ServiceRequestListener interfaces
  for maximum request and response payload size limits.

* Remove ServiceClient.sendWithCallback and ServiceHost.sendRequestWithCallback.
  Functionality is available through OperationOption.SEND_WITH_CALLBACK,
  symmetric to HTTP/2 functionality that is toggled through CONNECTION_SHARING

* Add new static fields that map to JVM properties that enable selection of
  HTTP scheme, for replication and forwarding, in NodeSelectorService interface.

* Invalidate authz cache in ServiceHost when any authz service(UserGroupService,
  RoleService or ResourceGroupService) is created, modified or deleted

* Add Operation.toggleOption and Operation.hasOption to allow direct manipulation
  of operation options and reduce code in Operation class methods

* Add support for OData to filter by all searchable fields of a document.
  Using "ALL_FIELDS" as a property name in a typical OData filter, e.g.
  /documents?$filter=ALL_FIELDS eq foo, will unfold the search to all indexed
  fields of document and their sub-fields nested up to 2 levels, excluding the
  build-in ServiceDocument fields.

* Introduce PropertyIndexingOption.FIXED_ITEM_NAME. This option directs the
  document indexing service to ensure the indexing property name will be a fixed
  value, matching that of the field itself. Applicable for fields of type MAP,
  it will allow to make queries based on the name of the field to search for
  keys and values of a map.

## 0.8.1

* Add support for expiration on stateful in-memory services. We now
  support expiration and auto-DELETE on services with
  ServiceOption.PERSISTED and without

* Add Operation.disableFailureLogging to allow control on default
  failure logging behavior. Disable logging of failures on on demand
  load services, if the demand load action is a DELETE

* Add OData query parameter $orderbytype used for specifying the type
  of property specified through $orderby parameter. This allows the
  OData query service in Xenon to support sorting for both numeric
  and string types.

* Add Utils.registerKind(Class<?>, String) allowing for custom mapping
  of service document types to kind strings. The default is derived
  from the canonical name of the class, as before. This is not a breaking
  change.

* Enable binary payload serialization during replication, using the same KRYO
  serialization we currently use for storing state in the index. The
  binary payload is only used during replication to peers so this is
  an internal optimization, not visible to clients or service authors.
  HTTP + JSON is still the default mechanism for all I/O between
  services and clients to xenon hosts. This is not a breaking change.

* Add StatefulService.getStateDescription() convenience method
  which also reduces allocations when a service author needs
  the reflect state document description

* Upgrade Lucene from 5.3.1 to 6.0.0

* Use Murmur3 hash, instead of SHA1 for ServiceDocument.equals
  and document signature calculation

* Add support for binary serialization of operation body, using
  KRYO binary serializer (same as what currently used for storing
  state in the index). The client opts in by setting the operation
  content type to the new Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM

* Remove experimental PRAGMA_VALUE_HTTP2, it is now expressed
  through OperationOption.CONNECTION_SHARING.

* Add support for starting ServiceHost only with HTTPS.
  To disable HTTP listener, provide "--port=-1" to the startup parameters.

* Add support for connection tags, optimize HTTP/2 I/O path. Connection
  tags allow finer control of connection pools, connection limit. HTTP/2
  default connection tag uses (by default) 4 parallel HTTP/2 connections
  per host, with potentially millions of pending operations in each.
  Perf gain of 2x on HTTP/2, with these changes.

* Enable HTTP2 for forwarding (as part of built in owner selection and loadbalancing)

## 0.8.0

* Enhance QueryOption.BROADCAST to use documents from owner nodes
  For details please see: https://www.pivotaltracker.com/projects/1471320/stories/116412415

* Upgrade netty from 4.1.0.CR3 to 4.1.0.CR7

* Update JWT(Json Web Token) to use private key when SSL is enabled. Otherwise,
  default value(same across all nodes) is used. (NOT SECURE)
  JWT is used to sign/verify authorization context token.

  In multi node environment, it is required to use same certificate and private
  key within same node group.
  If different cert is used, communication between nodes will fail since different
  private key is used to sign/verify json.
  We are planning certificate rotation in future.

  NOTE:
    Using the default token is insecure and should not be done in production
    settings when authorization is enabled.

* Add additional "Operation#setOperationCompletion" that allows the caller
  to supply two discrete callbacks: one for successful completion, and
  one for failure.
  For completion that does not share code between the two paths, this
  should be the preferred alternative.

* Use HTTP/2 for replication and forwarding requests

* Add logging methods that take lambda expression to construct
  log message. The lambda is evaluated lazily.

* Remove PRAGMA_DIRECTIVE_NO_QUEUING and make no-queuing
  the default behavior. That means finding a service succeeds
  if the service exists or fails-fast in the case it doesn't.
  Use PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY to
  override the default.

## 0.7.6

* Improve index searcher management in lucene document index
  service. Track index searchers for paginated query tasks
  using a sorted tree map, and close them when their query
  task expiration is reached. This avoid query failures
  under load, for paginated tasks.

* Add replication request header that allows the replication
  level to be set per operation. Useful for scenarios that
  require all peer nodes to see an update, independent of the
  membership quorum associated with the node group. For example
  all authorization related updates should replicate to all
  peers, before the operation completes

* Switch consistent hashing algorithm used by the default
  node selector, to Murmur3 32bit, instead of SHA1. Much less
  memory use, faster, leaner. This is an implementation detail,
  not visible to consumers to xenon.

* Add ServiceHost.checkFactoryAvailable and
  NodeGroupUtils.checkServiceAvailability convenience methods
  that use broadcast GET requests to all peers, to the service
  /available suffix, and return success if at least one peer
  reports status OK. This is useful for tests and production
  code that wants to determine if factory synchronization is
  complete, either on initial node start or after node changes

* Add MigrationTaskService, a task that can synchronize state
  between two node groups, for a given factory. Used as part
  of the "blue/green" live upgrade procedure. See wiki for
  multi node live upgrade details

* Add userGroupLinks as a field in UserService

* Remove TypeName.ARRAY and use TypeName.COLLECTION instead

## 0.7.5

* New TaskService.java base class to reduce duplicate code when
  implementing a task service. Subclasses no longer need to
  override: handleStart(), sendSelfPatch(), updateSelf(). Also
  provides common validation and default task expiration.

* New FactoryService.create helpers and ServiceHost.startFactory
  and startService helpers that reduce duplicate code

* New Operation.createXXXX(ServiceHost h, link) helpers
  to eliminate need for UriUtils.buildUri

* Option to serve a Swagger 2.0 API description of a ServiceHost

* Add a UiContentService that behaves like a regular web-server serving
  static files from a folder. It simplifies custom UI hosting.

## 0.7.2

* Simplify service synchronization logic during node group
  changes. Eliminate support for complex divergence scenarios
  and focus on reliable behavior on a few scenarios.
  We now throttle synchronization further by electing a single
  node to synchronize a given factory. Since we already only let
  a single factory synchronize per node, this further reduces
  contention.
  See https://www.pivotaltracker.com/story/show/115367955

* Throttle peer synchronization of services further: we now only
  synchronize one replicated factory at a time. A factory already
  throttles and synchronizes a limited number of children before
  doing the next batch. This will greatly help with scenarios when
  N nodes, with lots of existing state, all start and join at once.

* Add ServiceDocument.PropertyOptions that supercedes
  ServiceDocument.UsageOptions and can also specify indexing
  options.

* Add ServiceRequestSender interface that captures the capability
  to send Operation. Allows for ServiceHost, Service and ServiceClient
  to be dealt with uniformly. Clients may need to recompile as a lot
  of overloaded methods are removed.

* Add ServiceHost helper method startFactoryServicesSynchronously
  for starting factory services.

* Add a new module xenon-client that provides 'xenonc' executable.
  The client application works with an intuitive DSL to interact
  with a xenon-host. YAML and JSON are currently supported.

## 0.7.1

* Added VerificationHost.waitFor() utility method
  useful for convergence loops in tests.

* Support ODATA $orderby and $top

* Introduce ServiceOption.URI_NAMESPACE_OWNER. Allow
  for a service to handle all requests that start with
  a given prefix, allowing for emulation of other REST
  APIs, gateway style translation, etc.

* Add handleCreate handler to Service interface.
  This is not a breaking change. The new handler is
  opt-in, just like handleStop.
  Service lifecycle is now symmetric:
  1) Client POST -> handleCreate
  2) Client DELETE -> handleDelete
  handleStart always follows handleCreate, on owner only.
  handleStop always follows handleDelete, on owner only.
  handleStart occurs on service start, due to host restart,
  synch, or create (on owner)
  handleStop occurs on service stop, on service host stop,
  or due to delete (on owner)
  Added ServiceHost.isServiceCreate(operation) to match
  ServiceHost.isServiceStop(operation)

* Add StatelessService & StatefulService handlePeriodicMaintenance
  method to get periodic maintenance notifications when enabling
  ServiceOption.PERIODIC_MAINTENANCE. Invoked by handleMaintenance
  on MaintenanceReason.PERIODIC_SCHEDULE reason.

* Add StatelessService & StatefulService handleNodeGroupMaintenance
  method to get node group change notifications. Invoked by
  handleMaintenance on MaintenanceReason.NODE_GROUP_CHANGE reason.

* Include ServiceDocument.documentExpirationTimeMicros in the
  document signature computation done by Utils.computeSignature.
  Effectively this means that comparisons of two service
  documents using ServiceDocument.equals will return false if
  their "documentExpirationTimeMicros" differ.

## 0.7.0

* Add PRAGMA_FORCE_INDEX_UPDATE allowing for conflict
  resolution or overriding version checks on restarting a
  deleted service

* Fix NodeGroupBroadcastResponse race, where the jsonResponse
  list was getting corrupted, thus removing results from some
  of the nodes. This affected broadcast request behavior.

* Simplify constructs relating to consensus and availability.
  ServiceOption.ENFORCE_QUORUM is removed. Its semantics are
  rolled in with ServiceOption.OWNER_SELECTION. This implies
  that a service enabling owner selection, will get strong
  consensus and leader election, across the node group. The
  membershipQuorum must be properly set for strong consensus
  on state update.
  Eventual consistency is still supported and achieved by
  relaxing the membership quorum (using the REST api on
  node group /config suffix)
  Services with just ServiceOption.REPLICATION are always
  eventually consistent.
  Xenon will make an attempt to converge state across peers
  regardless of the options set.

* Remove NodeState.synchQuorum - Its functionality is collapsed
  into the existing membershipQuorum field. The quorum number
  determines when requests are allowed to flow, given node group
  health, and when resynchronization should occur. If the
  host argument --peerNodes is used, membership quorum is
  automatically set to the majority: (total / 2) + 1

* Services with ServiceOption.ON_DEMAND_LOAD and
  ServiceOption.REPLICATION will not automatically synchronize
  on every node group change. Services marked as such are
  expected to be explicitly synchronized or, on first use.

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
