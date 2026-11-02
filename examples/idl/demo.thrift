namespace go demo

exception Rejected { 1: string reason }
struct Item { 1: string id, 2: string source, 3: list<i64> values }

service Orders {
  Item Get(1: string id, 2: i32 delay_ms) throws (1: Rejected rejected),
  list<Item> List(1: list<string> ids),
  void Ping()
}

service Inventory {
  i64 Count(1: string id),
  Item Reserve(1: string id)
}
