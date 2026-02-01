package db

import (
	"fmt"
	//"slices"
	"strings"
)

type TreeNode interface {
	//Insert(k string, v uint64) bool
	//Find(k string) (uint64, error)
	//Balance(k string, low TreeNode, high TreeNode) bool
	SetParent(p TreeNode)
	String() string
	//Delete(k string) bool
}

type NotFoundError struct {
	message string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s", e.message)
}

type Child struct {
	key string
	value uint64
	left TreeNode
	right TreeNode
}

func (c Child) String() string {
	return fmt.Sprintf("{ %s, %d, %s, %s }", c.key, c.value, c.left, c.right)
}

func (t InternalNode) String() string {
	child_string := "{"
	for _, c := range(t.children) {
		child_string += fmt.Sprintf("%s,", c)
	}
	child_string += "}"
	return fmt.Sprintf("{ %d, %s }", t.m, child_string)
}

func (l LeafNode) String() string {
	child_string := "{"
	for i, v := range(l.keys) {
		child_string += fmt.Sprintf("(%s,%s) ", v, l.values[i])
	}
	child_string += "}"
	return fmt.Sprintf("{ %d, %s }", l.m, child_string)
}


type RootNode struct {
	m int
	child TreeNode
}

type InternalNode struct {
	m int
	children []TreeNode
	keys []string
	parent TreeNode
}

type LeafNode struct {
	m int
	parent TreeNode
	keys []string
	values []string
}

func newChild(k string, v uint64, left TreeNode, right TreeNode) *Child {
	return &Child {
		k, v, left, right,
	}
}

func newRootNode(m int) *RootNode {
	return &RootNode {m, nil}
}

func newInternalNode(m int) TreeNode {
	return &InternalNode {
		m, make([]TreeNode, m), make([]string, m - 1), nil,
	}
}

func newLeafNode(m int) TreeNode {
	return &LeafNode {
		m, nil, make([]string, m - 1), make([]string, m - 1),
	}
}

func (n *RootNode) SetParent(p TreeNode) {}

func (n *LeafNode) SetParent(p TreeNode) {
	n.parent = p
}

func (n *InternalNode) SetParent(p TreeNode) {
	n.parent = p
}

func BinarySearch(keys []string, k string, low int, high int) int {
	mid := (high + low) / 2
	if mid < 0 || mid > len(keys) {
		mid = len(keys) - 1
	}
	pivot := keys[mid]
	res := strings.Compare(pivot, k)
	if (high - low) == 1 {
		if res <= 0 {
			return mid + 1
		}
		return mid
	}
	if res <= 0 {
		return BinarySearch(keys, k, mid, high)
	}
	return BinarySearch(keys, k, low, mid)
}

func QuickSort(keys[] string) {
	QuickSortHelper(keys, 0, len(keys) - 1)
}

func QuickSortHelper(keys[] string, low int, high int) {
	if (high - low) < 1 { return }
	partition_index := (low + high) / 2
	i := low
	j := high
	for i < j {
		pivot := keys[partition_index]
		low_val := keys[i]
		high_val := keys[j]
		ltp := strings.Compare(low_val, pivot) >= 0
		gtp := strings.Compare(high_val, pivot) <= 0
		if ltp && gtp {
			tmp := keys[j]
			keys[j] = keys[i]
			keys[i] = tmp
			if i == partition_index {
				partition_index = j
			}
			if j == partition_index {
				partition_index = i
			}
			i++
			j--
		}
		if !ltp { i++ }
		if !gtp { j--}
	}
	QuickSortHelper(keys, low, partition_index)
	QuickSortHelper(keys, partition_index + 1, high)
}

/*
func (n *RootNode) Balance(k string, low TreeNode, high TreeNode) bool {
	n.child = newInternalNode(n.m, make([]*Child, 0), n)
	return n.child.Balance(k, low, high)
}

func (n *RootNode) Find(k string) (uint64, error) {
	return n.child.Find(k)
}

func (n *RootNode) Insert(k string, v uint64) bool {
	if n.child != nil {
		return n.child.Insert(k, v)
	}
	children := make([]*Child, 0)
	n.child = newLeafNode(n.m, children, n)
	return n.child.Insert(k, v)
}

func (n *InternalNode) Balance(k string, low TreeNode, high TreeNode) bool {
	children := append(n.children, &Child{ k, 0, low, high})
	slices.SortFunc(children, func(a,b *Child) int {
		return strings.Compare(a.key, b.key)
	})
	i, _ := slices.BinarySearchFunc(children, &Child{k, 0, nil, nil}, 
		func(a,b *Child) int { return strings.Compare(a.key, b.key) })
	if i > 0 && i < (len(children) - 1) {
		children[i - 1].right = children[i].left
		children[i + 1].left = children[i].right
	}
	if i == 0  && len(children) > 1 {
		children[i + 1].left = children[i].right
	}
	if i == len(children) - 1  && len(children) > 1 {
		children[i - 1].right = children[i].left
	}
	if len(children) > n.m {
		pivot_index := len(children) / 2
		low_children := children[0: pivot_index]
		high_children := children[pivot_index:]
		low_node := newInternalNode(n.m, low_children, n.parent)
		high_node := newInternalNode(n.m, high_children, n.parent)
		if n.parent == nil {
			n.parent = newRootNode(n.m)
		}
		return n.parent.Balance(children[pivot_index].key, low_node, high_node)
	}
	low.SetParent(n)
	high.SetParent(n)
	n.children = children
	return true
}

func (n *InternalNode) Insert(k string, v uint64) bool {
	i, _ := slices.BinarySearchFunc(n.keys, k, func(a, b *Child) int {
		return strings.Compare(a.key, b.key)
	})
	if i >= len(n.children) {
		i = len(n.children) - 1
	}
	pivot_key := n.children[i].key
	if strings.Compare(k, pivot_key) <= 0 {
		return n.children[i].left.Insert(k, v)
	}
	return n.children[i].right.Insert(k, v)
}

func (n *LeafNode) Balance(k string, low TreeNode, high TreeNode) bool {
	return true
}

func (n *LeafNode) Insert(k string, v uint64) bool {
	children := append(n.children, &Child {k, v, nil, nil })
	slices.SortFunc(children, func(a,b *Child) int {
		return strings.Compare(a.key, b.key)
	})
	if len(children) > n.m {
		pivot_index := len(children) / 2
		low_records := children[0: pivot_index]
		high_records := children[pivot_index:]
		low_node := newLeafNode(n.m, low_records, nil)
		high_node := newLeafNode(n.m, high_records, nil)
		return n.parent.Balance(children[pivot_index].key, low_node, high_node)
	}
	n.children = children
	return true
}

func (n *InternalNode) Find(k string) (uint64, error) {
	i, found := slices.BinarySearchFunc(n.children, &Child {k, 0, nil, nil }, func(a, b *Child) int {
		return strings.Compare(a.key, b.key)
	})
	if !found && i >= len(n.children) {
		i = len(n.children) - 1
	}
	pivot_key := n.children[i].key
	if strings.Compare(k, pivot_key) < 0 {
		return n.children[i].left.Find(k)
	}
	return n.children[i].right.Find(k)
}

func (n *LeafNode) Find(k string) (uint64, error) {
	i, found := slices.BinarySearchFunc(n.children, &Child{k, 0, nil, nil}, func(a, b *Child) int {
		return strings.Compare(a.key, b.key)
	})
	fmt.Printf("Find %v\n", n.children)
	if !found {
		return 0, &NotFoundError{ fmt.Sprintf("No record found for %s",k) }
	}
	return n.children[i].value, nil
}*/
