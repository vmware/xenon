# Project Xenon

This repository contains the code  for **DCP** (_Decentralized Control Plane_).

## What is it?
DCP is both a set of software components and a service oriented design pattern. 
The runtime is implemented in Java and acts as the host for the lightweight, asynchronous
services. Implementation in Go is also available, with the same programming model but
it currently has a subset of the functionality. 

Each service has less than 500 bytes
of overhead and can be paused/resumed, making DCP able to host millions of
service instances even on a memory constrained environment.

Service authors annotate their services with various service options, acting
as requirements on the runtime, and the framework implements the appropriate
algorithms to enforce them. The runtime exposes each service as a REST endpoint
and provides stats, reflection, subscriptions, per instance.

A powerful index service, invoked state changes on a service, provides a multi version
document store with a rich query language.

High availability and scale-out is enabled through the use of a consensus and replication
algorithm.

## What is it for?
The lightweight runtime (each service is less than 500 bytes of memory overhead) enables
the creation of highly available and scalable applications in the form of cooperating light
weight services. The operation model for a cluster of DCP nodes is the same for both on
premise, and service deployments.


## Getting started

Please refer to the [wiki](https://github.com/vmware/xenon/wiki). Tutorials for each
DCP service patterns will be made available in the coming months. Various samples
are under the dcp-samples directory.
