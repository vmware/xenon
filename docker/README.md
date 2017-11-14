# build images
make build
# go back to xenon parent path
# run test in container
./mvnw -B -e package -DskipAnalysis -pl xenon-common -Dtest=RunTestInContainer -Dxenon.containerImage=vmware/xenon-base -Dxenon.containerEnv=JVM_HEAP=256m,JVM_METASPACE=100m -Dxenon.containerExposedPort=8000