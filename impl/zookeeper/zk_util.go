package zookeeper

import "github.com/camsiabor/go-zookeeper/zk"

type ZkIterator func(conn *zk.Conn, current string, parent string, root string, depth int) bool

func ZkIterate(conn *zk.Conn, root string, parent string, depth int, iterator ZkIterator) error {
	if depth < 0 {
		return nil
	}
	var children, _, err = conn.Children(parent)
	if err != nil {
		return err
	}
	if parent[len(parent)-1] != '/' {
		parent = parent + "/"
	}
	var n = len(children)
	for i := 0; i < n; i++ {
		var current = parent + children[i]
		if !iterator(conn, current, parent, root, depth) {
			break
		}
		_ = ZkIterate(conn, root, current, depth-1, iterator)
	}
	return nil
}
