package ldb

type Store struct {
	conn *Connection
}

func (o *Store) SetConnection(c *Connection) {
	o.conn = c
}

func (o Store) Conn() *Connection {
	return o.conn
}

func NewStore(conn *Connection) *Store {
	return &Store{conn}
}
