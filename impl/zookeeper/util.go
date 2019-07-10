package zookeeper

import "github.com/samuel/go-zookeeper/zk"

type ZkIterator func(current string, parent string, root string, depth int) bool

func Iterate(conn *zk.Conn, root string, parent string, depth int, iterator ZkIterator) error {
	if depth < 0 {
		return nil
	}
	if parent[len(parent)-1] != '/' {
		parent = parent + "/"
	}
	var children, _, err = conn.Children(parent)
	if err != nil {
		return err
	}
	var n = len(children)
	for i := 0; i < n; i++ {
		var current = parent + children[i]
		if !iterator(current, parent, root, depth) {
			break
		}

		_ = Iterate(conn, root, current, depth-1, iterator)
	}
	return nil
}
