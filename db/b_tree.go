package db

import (
	"fmt"
	"slices"
	"strings"
)

type TreeNode interface {
	Insert(k string, v uint64) bool
	Find(k string) (uint64, error)
	Balance(k string, low TreeNode, high TreeNode) bool
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

type RootNode struct {
	m int
	child TreeNode
}

type InternalNode struct {
	m int
	children []*Child
	parent TreeNode
}

type LeafNode struct {
	m int
	children []*Child
	parent TreeNode
}

func newRootNode(m int) *RootNode {
	return &RootNode {m, nil}
}

func newInternalNode(m int, children []*Child, parent TreeNode) TreeNode {
	return &InternalNode {
		m, children, parent,
	}
}

func (n *RootNode) Balance(k string, low TreeNode, high TreeNode) bool {
	n.child = newInternalNode(n.m, make([]*Child, 0), nil)
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
	n.child = newLeafNode(n.m, children, nil)
	return n.child.Insert(k, v)
}

func (n *InternalNode) Balance(k string, low TreeNode, high TreeNode) bool {
	children := append(n.children, &Child{ k, 0, low, high})
	slices.SortFunc(children, func(a,b *Child) int {
		return strings.Compare(a.key, b.key)
	})
	if len(children) >= n.m {
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
	return true
}

func (n *InternalNode) Insert(k string, v uint64) bool {
	i, _ := slices.BinarySearchFunc(n.children, &Child {k, 0, nil, nil}, func(a, b *Child) int {
		return strings.Compare(a.key, b.key)
	})
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
	if len(children) >= n.m {
		pivot_index := len(children) / 2
		low_records := children[0: pivot_index]
		high_records := children[pivot_index:]
		low_node := newLeafNode(n.m, low_records, n.parent)
		high_node := newLeafNode(n.m, high_records, n.parent)
		if n.parent == nil {
			n.parent = newRootNode(n.m)
		}
		return n.parent.Balance(children[pivot_index].key, low_node, high_node)
	}
	return true
}

func (n *InternalNode) Find(k string) (uint64, error) {
	i, _ := slices.BinarySearchFunc(n.children, &Child {k, 0, nil, nil }, func(a, b *Child) int {
		return strings.Compare(a.key, b.key)
	})
	pivot_key := n.children[i].key
	if strings.Compare(k, pivot_key) <= 0 {
		return n.children[i].left.Find(k)
	}
	return n.children[i].right.Find(k)
}

func (n *LeafNode) Find(k string) (uint64, error) {
	i, found := slices.BinarySearchFunc(n.children, &Child{k, 0, nil, nil}, func(a, b *Child) int {
		return strings.Compare(a.key, b.key)
	})
	if !found {
		return 0, &NotFoundError{ fmt.Sprintf("No record found for %s",k) }
	}
	return n.children[i].value, nil
}

func newLeafNode(m int, children []*Child, parent TreeNode) TreeNode {
	return &LeafNode {
		m, children, parent,
	}
}
