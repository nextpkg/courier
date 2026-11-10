# Binary message encoding

Courier messages use a single binary encoding. `EncodeEnvelope` writes Thrift binary framing and `DecodeEnvelope` rejects non-binary input. `EncodeThriftEnvelope` is an explicit alias. There is no JSON message fallback or `Options.BinaryEnvelope` switch; Kafka, NATS and memory transports carry the same envelope bytes.

Envelope framing is `80 43 52 01`, followed by the TBinaryProtocol struct in `envelope.thrift`. Reply routes use `80 52 54 01` and `transport/kafka/reply_route.thrift`. Known fields have strict types; missing/duplicate fields, truncated/trailing data and size violations are rejected. Unknown Thrift fields are skipped for schema evolution. Requests reserve space for a bounded binary error response before admission, and raw incoming records are checked against the same budget.

Business payloads remain complete Thrift messages. Routes contain endpoint, instance, session, request ID, topic, partition and expiry, without HMAC. HTTP single-message headers base64-wrap binary route bytes. Both HTTP and Thrift batch forwarding use the same compact binary list: uint32 count, then for each reply uint32 instance length, instance bytes, int64 expiry, uint32 envelope length and envelope bytes. Statuses remain a uint32 list; HTTP responses use application/octet-stream. JSON remains only in diagnostic reports, not the RPC message path.

`RequestPreparer.PrepareRequest` resolves destination/reply partition and encodes once before admission. The prepared value owns its bytes. Parsed service/inbox interfaces reuse validated envelopes while retaining limits and correlation checks.

Codec pools retain buffers up to 64 KiB (route buffers up to 4 KiB); outputs and decoded strings/payloads own their storage. Thrift batch encoding uses exclusively borrowed buffers, returned after synchronous forwarding and cancellation cleanup. Oversized buffers are discarded on release; sync.Pool may discard entries during GC and does not impose a process-wide memory limit. HTTP request bodies retain independently allocated bytes because net/http may close bodies asynchronously.

Both formats define the first-release wire format. JSON message producers are unsupported; use the binary encoder consistently across callers, services and routers. No live deployment migration is performed by this change.

Canonical route frames (fields 1–7 in writer order, with no extra fields) use a checked cursor over the original bytes. Reordered or extended frames use the full Thrift decoder. Both paths retain size, type, required-field, endpoint and correlation checks.

Each router maintains immutable snapshots of at most 64 stable route identities, keyed by the encoded Endpoint/Instance/Session prefix. Hits reuse validated endpoint/instance/session strings and the configured reply topic; request ID and expiry are processed for every frame. ID gets independent storage because the dedup ring may retain it. Cache keys and values own their bytes, so receive buffers can be released after decoding. Misses publish a new snapshot; when capacity is reached a new generation starts. This bounds each published table, not all transient snapshots awaiting GC, and high identity churn may reduce hit rate.


### Borrowed reply views

`DecodeReplyEnvelopeView` accepts response envelopes only and validates their complete structural shape and required metadata. Canonical writer order is scanned directly over the input bytes without copying strings or payload; reordered/extended frames fall back to the full decoder. It retains the same binary version and size/type/duplicate/trailing-data checks.

`ReplyEnvelopeView` exposes read-only byte slices for filtering. The original frame must remain alive and unchanged until the view is no longer used. `OwnedEnvelope` copies strings/payload and error state before handing results to asynchronous callers; it also separates payload ownership on the fallback path. Do not store borrowed views in pending maps or dedup rings.

Kafka direct readers look up the session before materializing an envelope and skip owned decoding for other sessions or closed/old inboxes. Reply routers match routing metadata against the view, forward the original frame for remote delivery, and materialize only for local delivery. Target-side delivery also uses the view and materializes only for a live, nonduplicate inbox. Server request processing and its pre-handler validation are unchanged.


### Compact forwarded metadata

The Kafka routing header is still validated against the reply envelope before enqueueing. A forwarded batch carries only Instance, Expires and the original envelope bytes: the destination connection identifies Endpoint, and ID/Session/Topic/Partition are read from the validated envelope view. The receiver checks its process instance, expiry, configured reply topic/partition bounds, live inbox and dedup state. The dedup ID owns storage before callbacks execute, including raw callbacks that may modify their input.

Routing metadata retains the existing 768-byte budget, calculated from the omitted canonical route fields without rebuilding a header. Batch decoding validates the complete shape before delivering any item; instance/data slices are capacity-limited borrowed views. Single-message forwarding retains its full binary route header. This is the initial unpublished batch format, not an additional version or compatibility branch.
