package main

import "testing"

func TestFlags(t *testing.T) {
	flags := []string{
		"--key=value",
		"--mapA.k1=value",
		"--mapA.k2=value",
		"--sliceA[0].k1=value",
		"--sliceA[0].k2=value",
		"--sliceA[1].k1=value",
		"--sliceA[1].k2=value",
	}

	v, err := Map(flags)
	if err != nil {
		panic(err)
	}

	t.Logf("%#v", v)
}
