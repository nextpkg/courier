namespace go shapes
exception ShapeError { 1: string reason }
service Shapes {
  void Ping(),
  void Check(1: bool fail) throws (1: ShapeError problem),
  binary Echo(1: binary data),
  map<string, list<i32>> Nest(1: map<string, list<i32>> values)
}
