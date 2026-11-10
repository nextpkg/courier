// Wire prefix: 80 43 52 01, followed by this TBinaryProtocol struct.
struct Envelope {
 1: required i32 Version,
 2: required string Kind,
 3: required string Service,
 4: required string Method,
 5: required string ID,
 6: required string Session,
 7: required i64 Deadline,
 8: required string ReplyTopic,
 9: required i32 ReplyPartition,
 10: optional binary Payload,
 11: optional string ErrorCode,
 12: optional string ErrorMessage
}
