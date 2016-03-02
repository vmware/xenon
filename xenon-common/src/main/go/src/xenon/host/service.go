package host

type Service interface {
	StageHandler

	Host() *ServiceHost
	SetHost(h *ServiceHost)

	SelfLink() string
	SetSelfLink(s string)

	StartHandler
	RequestHandler
}
