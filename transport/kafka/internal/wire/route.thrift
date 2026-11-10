// Wire prefix: 80 52 54 01; routing metadata for trusted internal services.
struct ReplyRoute {
 1: required string Endpoint,
 2: required string Instance,
 3: required string Session,
 4: required string ID,
 5: required string Topic,
 6: required i32 Partition,
 7: required i64 Expires,
}
