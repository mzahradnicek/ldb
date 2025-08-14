package ldb

type Store struct {
	conn *Connection
}

func (o Store) SetConnection() *Connection {
	return o.conn
}

func (o Store) Conn() *Connection {
	return o.conn
}

func NewStore(conn *Connection) *Store {
	return &Store{conn}
}
