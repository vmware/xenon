## What is it?

Xenon is both a set of software components and a service oriented design pattern.
The runtime is implemented in Java and acts as the host for the lightweight, asynchronous
services. The programming model is language agnostic (does not rely on Java specific constructs)
so implementations in other languages are encouraged.

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

## What is it for?

The lightweight runtime enables the creation of highly available and scalable applications in the
form of cooperating light weight services. The operation model for a cluster of Xenon nodes is the
same for both on premise, and service deployments.

The [photon controller](https://vmware.github.io/photon-controller) project makes heavy use of Xenon
to build a scalable and highly available Infrastructure-as-a-Service fabric, composed of stateful
services, responsible for configuration (desired state), work flows (finite state machine tasks),
grooming and scheduling logic. Xenon is also used by several teams building new products, services
and features, within VMware.

## Getting started

For detailed information please refer to the [wiki](https://github.com/vmware/xenon/wiki).
Various samples are under the xenon-samples directory.

### Building the code

A detailed list of pre-requisite tools can be found in the
[developer guide](https://github.com/vmware/xenon/wiki/Developer-Guide).

Once you have installed all the pre-requisites from the root of the repository execute the following
command:

~~~bash
  mvn clean test
~~~

The above command will compile the code, run checkstyle and run unit-tests.

### Contributing

We welcome contributions and help with Xenon!  If you wish to contribute code and you have not
signed our contributor license agreement (CLA), our bot will update the issue when you open a
[Pull Request](https://help.github.com/articles/creating-a-pull-request). For any questions about
the CLA process, please refer to our [FAQ](https://cla.vmware.com/faq).

### Editing the code

The team uses Eclipse or IntelliJ. Formatting style settings for both these editors can be found in
the [contrib](https://github.com/vmware/xenon/tree/master/contrib) folder.

