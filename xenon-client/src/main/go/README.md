# xenonc

Simple command line interface to Xenon services.

## Installation

If GO 1.5 or 1.6 is found in PATH, mvn install automatically builds xenonc.
The binaries can be found in `resources/linux/bin/xenonc` and
`resources/darwin/bin/xenonc` respectively. If GO is not found xenonc is
skipped. Use `-DskipGO` as maven argument to skip the build explicitly.

xenonc can be built on OSX and GNU/Linux using make.

```
cd xenon/xenon-common/src/main/go
make deps verify
```
The above step downloads and installs common dependencies and tooling.

```
cd xenon/xenon-client/src/main/go
make clean deps verify test
make gobuild package 
```

## Usage

`xenonc` takes an HTTP verb argument, a service location, and a list of
flags that build a JSON request body.

The `XENON` environment variable must point to the Xenon node you want to talk to.

For example:

```
export XENON=http://localhost:8000/
```

As XENON services typically respond with JSON, we recommend using a tool such as
[jq][1] to interpret and transform these responses.

[1]: http://stedolan.github.io/jq/


To get this particular node's management information:

```
$ xenonc get /core/management | jq -r .systemInfo.ipAddresses[0]
10.0.1.41
```

To POST to the example factory service:

```
$ xenonc post /core/examples \
    --name=Joe \
    --keyValues.keyA=valueA \
    --keyValues.keyB=valueB
{
  "keyValues": {
    "keyA": "valueA",
    "keyB": "valueB"
  },
  "name": "Joe",
}
```

### Flags

* Use a verbatim `key` to specify a property in an object
* Use a dot to nest properties, e.g. `keyA.keyB`
* Use brackets to index into an array property, e.g. `array[2]`

Combine these to build complex objects.

For example:

* `--key.array[0].foo=bar`
* `--key.array[1].qux=foo`
