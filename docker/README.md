# 1.0 Build Docker Image From .Jar File
## 1.1 Copy jar file to docker folder
## 1.2 make
# 2.0 Start Container
## 2.1 make shell
## 2.2 In HOST terminal, curl localhost:8000
# 3.0 Notes
## 3.1 To avoid host ports conflict, -P could be used instead of -p $(HOSTPORT):$(XENONPORT), s.t a high port (32768 to 61000) while be assigned

Reference: https://docs.docker.com/engine/tutorials/usingdocker/
