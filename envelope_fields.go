package courier

// Field IDs match envelope.thrift. Keep explicit values: these are wire identifiers.
const (
	envelopeFieldVersion        = 1
	envelopeFieldKind           = 2
	envelopeFieldService        = 3
	envelopeFieldMethod         = 4
	envelopeFieldID             = 5
	envelopeFieldSession        = 6
	envelopeFieldDeadline       = 7
	envelopeFieldReplyTopic     = 8
	envelopeFieldReplyPartition = 9
	envelopeFieldPayload        = 10
	envelopeFieldErrorCode      = 11
	envelopeFieldErrorMessage   = 12

	envelopeRequiredFields uint16 = (1 << envelopeFieldReplyPartition) - 1
	envelopeErrorFields    uint16 = (1 << (envelopeFieldErrorCode - 1)) | (1 << (envelopeFieldErrorMessage - 1))
)
