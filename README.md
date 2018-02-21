# 1.0 Brief Introduction to Xenon [![Build Status](https://travis-ci.org/vmware/xenon.svg?branch=master)](https://travis-ci.org/vmware/xenon)

## 1.1 Xenon Highlights
Xenon is a framework for writing small REST-based services. (Some people call them microservices.)
The runtime is implemented in Java and acts as the host for the lightweight, asynchronous
services. The programming model is language agnostic (does not rely on Java specific constructs)
so implementations in other languages are encouraged. The services can run on a set of
distributed nodes. Xenon provides replication, synchronization, ordering, and consistency for the state
of the services. Because of the distributed nature of Xenon, the services scale
well and are highly available.

Xenon is a "batteries included" framework. Unlike some frameworks that provide
just consistent data replication or just a microservice framework, Xenon provides
both. Xenon services have REST-based APIs and are backed by a consistent, replicated
document store.

Each service has less than 500 bytes
of overhead and can be paused/resumed, making Xenon able to host millions of
service instances even on a memory constrained environment.

Service authors annotate their services with various service options, acting
as requirements on the runtime, and the framework implements the appropriate
algorithms to enforce them. The runtime exposes each service with a URI
and provides utility services per instance, for stats, reflection, subscriptions and configuration.
A built-in load balancer routes client requests between nodes, according to service options and
plug-able node selection algorithms. Xenon supports multiple, independent node groups, maintaining
node group state using a scalable gossip scheme.

A powerful index service, invoked as part of the I/O pipeline for persisted services, provides a
multi version document store with a rich query language.

High availability and scale-out is enabled through the use of a consensus and replication
algorithm and is also integrated in the I/O processing.

## 1.2 What can Xenon be used for?

The lightweight runtime enables the creation of highly available and scalable applications in the
form of cooperating light weight services. The operation model for a cluster of Xenon nodes is the
same for both on premise, and service deployments.

The [photon controller](https://vmware.github.io/photon-controller) project makes heavy use of Xenon
to build a scalable and highly available Infrastructure-as-a-Service fabric, composed of stateful
services, responsible for configuration (desired state), work flows (finite state machine tasks),
grooming and scheduling logic. Xenon is also used by several teams building new products, services
and features, within VMware.

## 1.3 Learning More
For a more detailed description of Xenon, keep reading this document.

For more technical details including tutorials, please refer to the
[wiki](https://github.com/vmware/xenon/wiki).

Various code samples are in the xenon-samples directory.

## 1.4 Reporting Issues
Xenon uses a public pivotal tracker project for tracking and reporting issues: https://www.pivotaltracker.com/n/projects/1471320

# 2.0 Detailed Introduction to Xenon

Xenon is a framework for writing small REST-based services. (Some people call them microservices.)
It supports a long list of features, but let’s step back a moment and take it a step at a time.

## 2.1 What is a Xenon service?
There are multiple definitions of "services", so let's define what we mean by a Xenon service.
(For now, we’ll assume we’re talking about Xenon running on a single host--we'll get to
using multiple hosts in a moment.)

A single Xenon service implements a REST-based API for a single URI endpoint.
For example, you might have a service running on:

```
https://myhost.example.com/example/service
```

You have a lot of choices in how you implement the service, but a few things are true:

1. The service can support any of the standard REST operations: GET, POST, PUT,
PATCH, DELETE

2. This service has a document associated with it. If you do a GET on the service,
you’ll see the document. In most cases, the document is represented in JSON.
There are a few standard JSON fields that every document has, such as the the
"kind" of document, the version, the time it was last updated, etc.

3. Your service may have business logic associated with it. Perhaps when you do
a POST, it creates a new VM, and a PATCH will modify the VM.

4. A service may have its state persisted on disk (a "stateful" service) or it
may be generated on the fly (a “stateless” service). Persisted services can have
an optional expiration time, after which they are removed from the datastore.

5.  All services can communicate with all other services by using the same APIs
that a client will use. Within a single host, communication is optimized (no need
to use the network). API calls from clients or services are nearly identical and
treated the same way. This makes for a very consistent communication pattern.

6. For stateful services, only one modification may happen at a time: modifications
to a statefule service are serialized, while reads from a service can be made in parallel.
If two modifications are attempted in parallel, there will be a conflict. One will
succeed, and the other will receive an error. (Xenon is flexible, and allows you
to disable this if you really want to.)


* [Example Service Tutorial](https://github.com/vmware/xenon/wiki/Example-Service-Tutorial)

## 2.2 Factory Services
How are services made? In general, a Xenon host will be started with a number of
"factory services". These are services that can create other services. For instance,
you might have a service for creating VMs:

```
https://myhost.example.com/vms
```

If a client POSTs a valid document describing a VM to that service, a new service
will be created and (presumably) the new VM will also be created. Typically the
service will be created with a unique random ID (a UUID):

```
https://myhost.example.com/vms/bb15980c-166e-11e6-b6ba-3e1d05defe78
```

The factory service is stateless. It does not explicitly keep track of all the
services that were made. Clients can do a GET to the factory service to find all
of the services of that type. Internally, the factory service just queries the
underlying document store to find all of them. These queries are implemented
efficiently on top of a Lucene index.

## 2.3 Task Services: Just for business logic
Some services do not need long-term persistence, but just need to accomplish
a short-lived task. For example, you might have a task that finds all disks that are
running out of space and send a message to a Slack channel. Interesting and valuable,
but ephemeral.

Task services are just like any other services, but they take advantage of two
features of Xenon:

1. Task services use the ability to communicate with services: they communicate
to themselves. As a task proceeds, it records its state by using its own API to update itself.
For instance:
  - A client POSTs to a task factory and the task service is made
  - The task service does something (search for disks, to continue above example)
  - The task services sends a PATCH to itself to update its state
  - When the task receives the PATCH, it triggers the next step: send message to Slack
  - When the task is done, it sends a PATCH to itself to mark task as done: any client
  can see that it’s done by doing a GET to the task service.
2. Task services are asynchronous: every step happens as part of asynchronously
responding to the initial POST or the subsequent PATCHs.

The mechanisms used to implement task services are identical as those for other services.
As a result, tasks are transparent (clients can see progress of the tasks) and
encourages asynchronous implementations, which scale well.

* [Tutorial on Task Services (wiki)](https://github.com/vmware/xenon/wiki/Task-Service-Tutorial)

## 2.4 What can you do with Xenon services?
Just about anything you want. Think about it this way: Xenon encourages you to
build a system as a set of small services that have REST-based APIs and can
communicate with each other. People have built a wide variety of systems on top
of Xenon including an IaaS system,
[Photon Controller](https://github.com/vmware/photon-controller).

## 2.5 Authentication & Authorization
Xenon provides both authentication and authorization. Today it has a
username/password mechanism for authentication, but allows for it to be extended
(via the addition of appropriate services). Users can be given access to all
documents or a subset of documents, depending on how Xenon is configured. All
configuration of users and permissions is via Xenon services.

* [Authentication and Authorization Design](https://github.com/vmware/xenon/wiki/Authentication-and-Authorization-Design)
* [Authentication and Authorization Tutorial](https://github.com/vmware/xenon/wiki/Authentication-and-Authorization-Tutorial)

## 2.6 How does Xenon work when there are multiple hosts?
Xenon is architected to work well when there are multiple Xenon hosts in a cluster.
Systems that build on top of Xenon have choices in how they want to build their system.

Note that each of these configuration choices is done per-service: different
services can have different ownership, replication, and consistency configuration,
depending on their needs.

*Ownership:* In cases where it matters, developers can choose to have Xenon
select a single owner for a service. Xenon calls these "owner-selected" services.
The owner is chosen automatically using a consitent hashing algorithm and it will
be updated if Xenon hosts are added or removed from the system. When a host is
added or removed, the ownership may be updated immediately or as-needed, but to
users cannot tell the difference: whenever they need to access a document, the
latest version is provided. Users never need to be aware of the ownership of the
document: this is an internal detail that Xenon manages.

*Replication:* Xenon replicates all service documents to other nodes. Developers
can choose between symmetric replication (all nodes have a copy of the data) or
asymmetric replication (some subset has it). The choice between these two is
between simplicity and performance. For operations that modify a services, the
developer can choose to either wait for all nodes to have the same data, or
they can for a smaller number, called the quorum, to have the data. By default,
the quorum is a majority of nodes.

*Consistency:* Xenon allows developers to choose between strongly consistent or
eventually consistent, but is most commonly strongly consistent. Strongly consistent
services use owner-selection (the service document is managed by the owner) and
replication.

*Node failures:* If a node fails, other Xenon nodes continue to run.
If a Xenon service was configured to require all nodes to have a replica of the data,
the service cannot be updated until the missing node is returned to
service or the quorum size is reduced to match the number of nodes.
When a node returns to service, it is updated with changes that happened in its absence.
This update process is called synchronization.

# 2.5 Tracing operations with OpenTracing

Xenon ships with an inbuilt intracluster tracing mechanism as well as optional support
for OpenTracing tracers.

Configure a tracer by providing an `io.opentracing.Tracer` instance to your `ServiceHost`
or by setting the environment variable `XENON_TRACER_FACTORY_PROVIDER`, plus any
additional settings needed by your chosen implementation.

## 2.5.1 Tracing configuration

### Core settings
| Environment variable                   | effect
|----------------------------------------|-------
| XENON_TRACER_FACTORY_PROVIDER          | (Required) Set to select a tracer: `zipkin` or `jaeger`

Be sure to include the appropriate implementation JAR files in your builds:
they are marked optional by `xenon-common`.

### Zipkin settings

| Environment variable  | effect
|-----------------------|-------
| ZIPKIN_SERVICE_NAME   | (Required) Set to configure the opentracing service name for the process.
| ZIPKIN_SAMPLERATE     | Set to a float to control what % of traces are sampled.
| ZIPKIN_URL            | (Required) Set to direct the ZIPKIN reporter target URL. e.g. http://host/apis/v1/spans

Add the following to your project dependencies:

```
<dependency>
  <groupId>io.opentracing.brave</groupId>
  <artifactId>brave-opentracing</artifactId>
  <version>0.22.1</version>
</dependency>
<dependency>
  <groupId>io.zipkin.reporter2</groupId>
  <artifactId>zipkin-sender-okhttp3</artifactId>
  <version>2.0.2</version>
</dependency>
<dependency>
  <groupId>io.zipkin.reporter2</groupId>
  <artifactId>zipkin-sender-urlconnection</artifactId>
  <version>2.0.2</version>
</dependency>
```

### Jaeger settings

See https://github.com/jaegertracing/jaeger-client-java/blob/master/jaeger-core/README.md
for the complete list of variables Jaeger's Java client supports.

| Environment variable  | effect
|-----------------------|-------
| JAEGER_SERVICE_NAME   | (Required). Set the opentracing service name for Jaeger reporting
| JAEGER_AGENT_HOST     | Set the host to send spans to
| JAEGER_SAMPLER_TYPE   | What type of sampler e.g. const
| JAEGER_SAMPLER_PARAM  | Sampler parameter    e.g. 1

Add the following to your project dependencies:

```
<dependency>
  <groupId>com.uber.jaeger</groupId>
  <artifactId>jaeger-core</artifactId>
  <version>0.21.0</version>
</dependency>
```

## Tracing Operations

Anything handled by `ServiceHost.handleRequest` or `ServiceHost.sendRequest`
will be automatically assigned a span with relevant metadata from the
Operation. The current span can be accessed via `host.getTracer().activeSpan()`
- see the OpenTracing Java API for more details.

Additional metadata can be added to the existing span, or if you have an
operation, or more generally any substantial work that should be tracked
separately, an additional span can be tracked like so:

```
try (ActiveSpan span = host.getTracer().buildSpan($"OPERATION").startActive()) {
    // Tracked code
}
```

If, as is common in Xenon, your work will be run in a thread via the
`ServiceHost.run` call, the current span at the time of submission will be
carried over as a closure into the context of the run callback:

```
try (ActiveSpan span = host.getTracer().buildSpan($"OPERATION").startActive()) {
    span.setTag(Tags.COMPONENT.getKey(), "crypto");
    host.run(() -> {
        // Tracked code
    }
}
```

You can access the span in the tracked code too:

```
try (ActiveSpan span = host.getTracer().buildSpan($"OPERATION").startActive()) {
    host.run(() -> {
	host.getTracer().activeSpan().setTag(Tags.COMPONENT.getKey(), "crypto");
        // Tracked code
    }
}
```

If you are writing your own deferral mechanism, see the implementation of e.g.
`ServiceHost.run()` for examples of context hand-off across threads. (Or see the
OpenTracing Java API docs).

Spans can be annotated with additional metadata such as debugging information via
the `Span.log` family of APIs. The recommendation when doing that is not to include
PII or customer data: doing so may significantly increase your operational burden
around management of the tracing infrastructure : but ultimately Xenon itself does
not care, and you can do whatever you like. Xenon won't log such data to OpenTracing
spans.

# 3.0 Getting started

## 3.1 Building the code

A detailed list of pre-requisite tools can be found in the
[developer guide](https://github.com/vmware/xenon/wiki/Developer-Guide). Xenon uses
[Maven](https://maven.apache.org/) for building.

Once you have installed all the pre-requisites from the root of the repository execute the following
Maven command:

~~~bash
  mvn clean test
~~~

The above command will compile the code, run checkstyle, and run unit-tests.

## 3.2 Editing the code

The team uses Eclipse or IntelliJ. Formatting style settings for both these editors can be found in
the [contrib](https://github.com/vmware/xenon/tree/master/contrib) folder.

# 4.0 Contributing

We welcome contributions and help with Xenon!  If you wish to contribute code and you have not
signed our contributor license agreement (CLA), our bot will update the issue when you open a
[Pull Request](https://help.github.com/articles/creating-a-pull-request). For any questions about
the CLA process, please refer to our [FAQ](https://cla.vmware.com/faq).

## 4.1.1 Pull Request Guide
TBD
