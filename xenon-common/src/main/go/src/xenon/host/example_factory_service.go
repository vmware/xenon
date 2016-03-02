package host

type ExampleFactoryService struct{}

func (s *ExampleFactoryService) CreateDocument() ServiceDocumentGetter {
	return &ExampleServiceDocument{}
}

func (s *ExampleFactoryService) CreateService() Service {
	return NewServiceContext(&ExampleService{})
}
