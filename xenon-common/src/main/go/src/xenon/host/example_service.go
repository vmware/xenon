package host

type ExampleService struct {
	State ExampleServiceDocument
}

func (s *ExampleService) GetState() interface{} {
	return &s.State
}
