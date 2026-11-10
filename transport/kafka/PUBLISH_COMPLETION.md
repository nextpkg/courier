# Publication completion lifetime

Each transport maintains an idle cache of completion slots, capped at the smaller of PublishBuffers and 1024. Slots are allocated when needed and reused; no unused large heap allocation or GC policy change is involved.

A slot contains a buffered result channel, an atomic owner count and its cache reference. The caller and SDK callback each own one reference. A caller timeout releases only the caller reference. The final owner drains any unconsumed result before returning the slot to the bounded cache; a full cache drops the slot for normal GC. Public Call/Future objects are not pooled.

Transport shutdown detaches the cache under the existing transport lock. In-flight calls and callbacks retain their cache until they finish. The result channel is never closed or handed to another operation while either owner can still use it. Publication deadlines, overload mapping and callback error propagation retain their existing behavior.
