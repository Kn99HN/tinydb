package db

import (
	"fmt"
	//"slices"
	"strings"
)

type SearchMode int

const (
	VALUE SearchMode = iota
	CHILDREN
)

type TreeNode interface {
	Insert(k string, v string) bool
	Find(k string) (string, error)
	Balance() bool
	SetParent(p TreeNode)
	SetChild(p TreeNode)
	String() string
	AddKey(k string)
	AddValue(v string)
	AddChild(c TreeNode)
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

func (r RootNode) String() string {
	return r.child.String()
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
		m, make([]TreeNode, 0), make([]string, 0), nil,
	}
}

func newLeafNode(m int) TreeNode {
	return &LeafNode {
		m, nil, make([]string, 0), make([]string, 0),
	}
}

func (n *RootNode) SetChild(c TreeNode) {
	n.child = c
}
func (n *LeafNode) SetChild(c TreeNode) {}
func (n *InternalNode) SetChild(c TreeNode) {}

func (n *RootNode) AddKey(k string) {}
func (n *LeafNode) AddKey(k string) {
	n.keys = append(n.keys, k)
}
func (n *InternalNode) AddKey(k string) {
	n.keys = append(n.keys, k)
}

func (n *RootNode) AddValue(v string) {}
func (n *LeafNode) AddValue(v string) {
	n.values = append(n.values, v)
}
func (n *InternalNode) AddValue(v string) {}


func (n *RootNode) AddChild(c TreeNode) {}
func (n *LeafNode) AddChild(c TreeNode) {}
func (n *InternalNode) AddChild(c TreeNode) {
	n.children = append(n.children, c)
}



func (n *RootNode) SetParent(p TreeNode) {}

func (n *LeafNode) SetParent(p TreeNode) {
	n.parent = p
}

func (n *InternalNode) SetParent(p TreeNode) {
	n.parent = p
}

func BinarySearch(keys []string, k string, low int, high int, mode SearchMode) (int, bool) {
	mid := (high + low) / 2
	if mid < 0 || mid > len(keys) {
		mid = len(keys) - 1
	}
	pivot := keys[mid]
	res := strings.Compare(pivot, k)
	if (high - low) == 1 {
		if res <= 0 {
			if mode == CHILDREN {
				return mid + 1, res == 0
			}
			return mid, res == 0
		}
		return mid, false
	}
	if res <= 0 {
		return BinarySearch(keys, k, mid, high, mode)
	}
	return BinarySearch(keys, k, low, mid, mode)
}

func QuickSort(keys[] string, values []string, children []TreeNode) {
	QuickSortHelper(keys, values, children, 0, len(keys) - 1)
}

func QuickSortHelper(keys[] string, values []string, children []TreeNode, low int, high int) {
	if (high - low) < 1 { return }
	if (high - low) == 1 && strings.Compare(keys[high], keys[low]) == 0 {
		return
	}
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
			// swap values
			if values != nil {
				tmp_val := values[j]
				values[j] = values[i]
				values[i] = tmp_val
			}
			// swap children
			if children != nil {
				tmp_child_1 := children[j]
				tmp_child_2 := children[j + 1]
				children[i] = tmp_child_1
				children[i + 1] = tmp_child_2
			}
			if i == partition_index {
				partition_index = j
			} else if j == partition_index {
				partition_index = i
			}
			i++
			j--
		}
		if !ltp { i++ }
		if !gtp { j--}
	}
	QuickSortHelper(keys, values, children, low, partition_index)
	QuickSortHelper(keys, values, children, partition_index + 1, high)
}



func (n *RootNode) Balance() bool {
	return true
}

func (n *RootNode) Find(k string) (string, error) {
	return n.child.Find(k)
}

func (n *RootNode) Insert(k string, v string) bool {
	if n.child != nil {
		return n.child.Insert(k, v)
	}
	n.child = newLeafNode(n.m)
	n.child.SetParent(n)
	return n.child.Insert(k, v)
}

/*
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
}*/

func (n *LeafNode) Balance() bool {
	QuickSort(n.keys, n.values, nil)
	left_node := newLeafNode(n.m)
	right_node := newLeafNode(n.m)
	mid := len(n.keys) / 2
	for i := 0; i < mid; i++ {
		left_node.AddKey(n.keys[i])
		left_node.AddValue(n.values[i])
	}
	for i := mid; i < len(n.keys); i++ {
		right_node.AddKey(n.keys[i])
		right_node.AddValue(n.values[i])
	}
	internal_node := newInternalNode(n.m)
	internal_node.AddKey(n.keys[mid])
	internal_node.AddChild(left_node)
	internal_node.AddChild(right_node)
	internal_node.SetParent(n.parent)
	n.parent.SetChild(internal_node)
	left_node.SetParent(internal_node)
	right_node.SetParent(internal_node)
	return true
}

func (n *InternalNode) Balance() bool {
	return true
}

func (n *InternalNode) Insert(k string, v string) bool {
	return true
}

func (n *LeafNode) Insert(k string, v string) bool {
	if (len(n.keys) + 1) >= n.m {
		n.keys = append(n.keys, k)
		n.values = append(n.values, v)
		n.Balance()
		n.SetParent(nil)
	}
	n.keys = append(n.keys, k)
	n.values = append(n.values, v)
	QuickSort(n.keys, n.values, nil)
	return true
}

func (n *InternalNode) Find(k string) (string, error) {
	i, _ := BinarySearch(n.keys, k, 0, len(n.keys), CHILDREN)
	return n.children[i].Find(k)
}

func (n *LeafNode) Find(k string) (string, error) {
	i, found := BinarySearch(n.keys, k, 0, len(n.keys), VALUE)
	if !found {
		return "", &NotFoundError{ fmt.Sprintf("No record found for %s",k) }
	}
	return n.values[i], nil
}
