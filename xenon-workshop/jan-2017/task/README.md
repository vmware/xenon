# About

[Xenon for Implementing Tasks/Workflow](https://github.com/vmware/xenon/wiki/workshop/2017-01/2017-01-Tasks.pptx)


## Commands

Create some example services:

```
curl -X POST -H "Content-Type: application/json" -d '{"name":"example1"}' http://127.0.0.1:8000/core/examples
curl -X POST -H "Content-Type: application/json" -d '{"name":"example2"}' http://127.0.0.1:8000/core/examples
curl -X POST -H "Content-Type: application/json" -d '{"name":"example3"}' http://127.0.0.1:8000/core/examples
```


Verify the example services exist:

```
curl http://127.0.0.1:8000/core/examples
```


Create a new task service:

```
curl -X POST -H "Content-Type: application/json" -d '{}' http://127.0.0.1:8000/core/demo-tasks
```


If your task has done everything right, your example services should no longer exist:

```
curl http://127.0.0.1:8000/core/examples
```