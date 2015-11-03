# CHANGELOG

## 0.2.0-SNAPSHOT

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
