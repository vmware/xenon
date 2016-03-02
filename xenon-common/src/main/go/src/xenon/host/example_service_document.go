package host

import "xenon/common"

type ExampleServiceDocument struct {
	common.ServiceDocument

	Map    map[string]string
	Int    int64
	String string
}

func (e *ExampleServiceDocument) GetServiceDocument() *common.ServiceDocument {
	return &e.ServiceDocument
}
