package common

type NodeState struct {
	GroupReference           string         `json:"groupReference"`
	MembershipAgentReference string         `json:"membershipAgentRpcReference"`
	SystemHostInfo           SystemHostInfo `json:"systemHostInfo"`
	ID                       string         `json:"id"`
	Status                   string         `json:"status"`
}
