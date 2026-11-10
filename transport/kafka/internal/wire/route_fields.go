package wire

// Route field IDs match route.thrift; they are independent of Envelope field IDs.
const (
	RouteFieldEndpoint  = 1
	RouteFieldInstance  = 2
	RouteFieldSession   = 3
	RouteFieldID        = 4
	RouteFieldTopic     = 5
	RouteFieldPartition = 6
	RouteFieldExpires   = 7

	routeRequiredFields uint16 = (1 << RouteFieldExpires) - 1
)
