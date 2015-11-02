# Project Xenon

This repository contains the code  for **DCP** (_Decentralized Control Plane_).

## What is it?

DCP is both a set of software components and a service oriented design pattern. 
It enables the creation of stateless or stateful orchestration in the form of cooperating light
weight services across many nodes.
The implementation enables a common model for persistence, indexing, querying,
concurrency, scale-out, and interaction through a restricted set of primitives
(HTTP verbs), and an observable, dynamic state model.

Service authors annotate their services with various service options, acting
as requirements on the runtime, and the framework implements the appropriate
algorithms to enforce them

This fabric is also referred to as the **Decentralized Control Plane** (DCP).

## What is it for?
The design and code helps developers deliver functionality as a service and/or
shrink wrapped software.

## Details

Please refer to the [wiki](https://github.com/vmware/xenon/wiki) on more information and how
to get started building DCP services.