package common

const (
	Core = "/core"

	Management = Core + "/management"
	ProcessLog = Management + "/process-log"
	SystemLog  = Management + "/system-log"

	ODataQuery = Core + "/odata-queries"

	NodeGroupFactory = Core + "/node-groups"

	DefaultNodeGroupName = "default"
	DefaultNodeGroup     = NodeGroupFactory + "/" + DefaultNodeGroupName
)
