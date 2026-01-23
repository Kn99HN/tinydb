package db

type Node interface {
	Insert(k string, v string) bool
	Find(k string) uint64
	Delete(k string) bool
}

type NotFoundError struct {
	message string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s", e.arg, e.message)
}


type InternalNode struct {
	m int
	keys [m]string
	children [m]*Node
}

type LeafNode struct {
	m int
	keys [m]string
	values [m]uint64
}

func newInternalNode(m int) *InternalNode {
	return &InternalNode {
		m, make([]string, m), make([]*Node, m),
	}
}

func binarySearchKeys(k string, keys []string, low int, high int) int {
	if (high - low) == 1 {
		if current_key <= k {
			return low
		} else {
			return high
		}
	}
	half := (high + low) / 2
	current_key := keys[half]
	if current_key == k {
		return half
	}
	if current_key < k {
		return binarySearchKeys(k, keys, half, high)
	}
	return binarySearchKeys(k, keys, low, half)
}

func (n *InternalNode) Find(k string) (uint64, error) {
	next_child_index := binarySearchKeys(k, n.keys, 0, n.m)
	return n.children[next_child_index].Find(k)
}

func (n *LeafNode) Find(k string) (uint64, error) {
	current_child_index := binarySearchKeys(k, n.keys, 0, n.m)
	if current_child_index == -1 {
		return 0, &NotFoundError{ fmt.Sprintf("No index for %s", k) }
	}
	return n.values[current_child_index]
}

func newLeafNode(m int) *LeafNode {
	return &LeafNode {
		m, make([]string, m), make([]uint64, m),
	}
}
