package db

import (
	"fmt"
	//"slices"
	"strings"
	"iter"
)

type SearchMode int
type Direction int

const (
	VALUE SearchMode = iota
	CHILDREN
)

const (
	LEFT Direction = iota
	RIGHT
	SPLIT
)

type IndexRecord struct {
	k string
	v string
}

type TreeNode interface {
	Insert(k string, v string) bool
	Find(k string) (string, error)
	Balance() bool
	SetParent(p TreeNode)
	String() string
	AddKey(k string)
	SetKey(k string, i int)
	AddValue(v string)
	AddChild(c TreeNode)
	UpsertKeys([]string)
	UpsertChildren([]TreeNode)
	UpsertValues([]string)
	NeedBalance() bool
	IsRootNode() bool
	All() iter.Seq[TreeNode]
	GetIndexRecord() []*IndexRecord
	BinarySearchKey(v string) (int, bool)
}

type NotFoundError struct {
	message string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s", e.message)
}

func (r RootNode) String() string {
	return r.child.String()
}

func (t InternalNode) String() string {
	child_string := "I-node {"
	for _, c := range(t.keys) {
		child_string += fmt.Sprintf("%s,", c)
	}
	child_string += "}, {"

	for _, c := range(t.children) {
		child_string += fmt.Sprintf("%s,", c)
	}
	child_string += "}"
	return fmt.Sprintf("{ %d, %s }", t.m, child_string)
}

func (l LeafNode) String() string {
	child_string := "L-node {"
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
	left_sibling *LeafNode
	right_sibling *LeafNode
}

func newRootNode(m int) *RootNode {
	return &RootNode {m, nil}
}

func newInternalNode(m int) *InternalNode {
	return &InternalNode {
		m, make([]TreeNode, 0), make([]string, 0), nil,
	}
}

func newLeafNode(m int) *LeafNode {
	return &LeafNode {
		m, nil, make([]string, 0), make([]string, 0), nil, nil,
	}
}

func (n *RootNode) IsRootNode() bool { return true }
func (n *LeafNode) IsRootNode() bool { return false }
func (n *InternalNode) IsRootNode() bool { return false }

func (n *RootNode) SetKey(k string, i int) {}
func (n *LeafNode) SetKey(k string, i int) { n.keys[i] = k }
func (n *InternalNode) SetKey(k string, i int) { n.keys[i] = k }


func (n *RootNode) AddKey(k string) {}
func (n *LeafNode) AddKey(k string) {
	n.keys = append(n.keys, k)
}
func (n *InternalNode) AddKey(k string) {
	n.keys = append(n.keys, k)
}

func (n *RootNode) UpsertKeys(k []string) {}
func (n *LeafNode) UpsertKeys(k []string) {
	n.keys = make([]string, len(k))
	copy(n.keys, k)
}
func (n *InternalNode) UpsertKeys(k []string) {
	n.keys = make([]string, len(k))
	copy(n.keys, k)
}

func (n *RootNode) UpsertValues(k []string) {}
func (n *LeafNode) UpsertValues(k []string) {
	n.values = make([]string, len(k))
	copy(n.values, k)
}
func (n *InternalNode) UpsertValues(k []string) {
}

func (n *RootNode) UpsertChildren(c []TreeNode) {}
func (n *LeafNode) UpsertChildren(c []TreeNode) {
}
func (n *InternalNode) UpsertChildren(c []TreeNode) {
	n.children = make([]TreeNode, len(c))
	copy(n.children, c)
}

func (n *RootNode) NeedBalance() bool { return false }
func (n *LeafNode) NeedBalance() bool {
	return len(n.keys) == n.m
}
func (n *InternalNode) NeedBalance() bool {
	return len(n.keys) == n.m
}

func (n *RootNode) AddValue(v string) {}
func (n *LeafNode) AddValue(v string) {
	n.values = append(n.values, v)
}
func (n *InternalNode) AddValue(v string) {}


func (n *RootNode) AddChild(c TreeNode) {
	n.child = c
}
func (n *LeafNode) AddChild(c TreeNode) {}
func (n *InternalNode) AddChild(c TreeNode) {
	n.children = append(n.children, c)
}

func (n *LeafNode) SetLeftSibling(s *LeafNode) {
	n.left_sibling = s
}

func (n *LeafNode) SetRightSibling(s *LeafNode) {
	n.right_sibling = s
}

func (n *LeafNode) GetRotationDirection() Direction {
	if n.left_sibling == nil  && len(n.right_sibling.keys) < (n.m / 2) {
		return RIGHT
	}
	if len(n.left_sibling.keys) < (n.m / 2) {
		return LEFT
	}
	return SPLIT
}


func (n *RootNode) GetIndexRecord() []*IndexRecord { return []*IndexRecord{} }
func (n *LeafNode) GetIndexRecord() []*IndexRecord {
	records := []*IndexRecord{}
	for i, k := range n.keys {
		records = append(records, &IndexRecord{ k, n.values[i]})
	}
	return records
}
func (n *InternalNode) GetIndexRecord() []*IndexRecord { return []*IndexRecord{} }


func (n *RootNode) SetParent(p TreeNode) {}
func (n *LeafNode) SetParent(p TreeNode) {
	n.parent = p
}
func (n *InternalNode) SetParent(p TreeNode) {
	n.parent = p
}

func BinarySearch(keys []string, k string, low int, high int, mode SearchMode) (int, bool) {
	if len(keys) < 1  {
		return 0, false
	}
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
	pivot := keys[partition_index]
	for i <= j {
		gte_partition := false
		for i < len(keys) {
			low_val := keys[i]
			gte_partition = strings.Compare(low_val, pivot) >= 0
			if gte_partition { break }
			i++
		}
		lte_partition := false
		for j >= 0 {
			high_val := keys[j]
			lte_partition = strings.Compare(high_val, pivot) <= 0
			if lte_partition { break }
			j--
		}
		if gte_partition && lte_partition && i <= j {
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
			i++
			j--
		}
	}
	QuickSortHelper(keys, values, children, low, j)
	QuickSortHelper(keys, values, children, i, high)
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

func (n *InternalNode) BinarySearchKey(k string) (int, bool) {
	if len(n.keys) < 1  {
		return 0, false
	}
	low := 0
	high := len(n.keys)
	mid := (high + low) / 2
	if mid < 0 || mid > len(n.keys) {
		mid = len(n.keys) - 1
	}
	pivot := n.keys[mid]
	res := strings.Compare(pivot, k)
	if (high - low) == 1 {
		if res <= 0 {
			return mid, res == 0
		}
		return mid, false
	}
	if res <= 0 {
		return n.BinarySearchKey(k)
	}
	return n.BinarySearchKey(k)
}



func (n *LeafNode) BinarySearchKey(k string) (int, bool) {
	if len(n.keys) < 1  {
		return 0, false
	}
	low := 0
	high := len(n.keys)
	mid := (high + low) / 2
	if mid < 0 || mid > len(n.keys) {
		mid = len(n.keys) - 1
	}
	pivot := n.keys[mid]
	res := strings.Compare(pivot, k)
	if (high - low) == 1 {
		if res <= 0 {
			return mid, res == 0
		}
		return mid, false
	}
	if res <= 0 {
		return n.BinarySearchKey(k)
	}
	return n.BinarySearchKey(k)
}

func (n *RootNode) BinarySearchKey(k string) (int, bool) {
 return 0, false
}

/*

General rotation
- Move the biggest key to the parent if moving from left -> right
- Move the smallest key to the parent if moving from right -> left
*/
func (n *LeafNode) Rotate(direction Direction) {
	switch direction {
		case LEFT:
		 current_key := n.keys[0]
		 current_value := n.values[0]
		 n.keys = n.keys[1:]
		 n.values = n.values[1:]
		 n.left_sibling.AddKey(current_key)
		 n.left_sibling.AddKey(current_value)
		 rotated_key_index, _ := n.parent.BinarySearchKey(current_key)
		 n.parent.SetKey(n.keys[0], rotated_key_index)
   case RIGHT:
		 last_index := len(n.keys) - 1
		 current_key := n.keys[last_index]
		 current_value := n.values[last_index]
		 n.keys = n.keys[:last_index]
		 n.values = n.values[:last_index]
		 n.right_sibling.AddKey(current_key)
		 n.right_sibling.AddKey(current_value)
		 rotated_key_index, _ := n.BinarySearchKey(current_key)
		 n.parent.SetKey(n.keys[0], rotated_key_index)
	}
}

func (n *LeafNode) Balance() bool {
	QuickSort(n.keys, n.values, nil)
	mid := len(n.keys) / 2
	mid_key := n.keys[mid]
	left_keys := n.keys[0:mid]
	left_values := n.values[0:mid]
	right_keys := n.keys[mid:]
	right_values := n.values[mid:]
	n.UpsertKeys(left_keys)
	n.UpsertValues(left_values)

	right_node := newLeafNode(n.m)
	right_node.UpsertKeys(right_keys)
	right_node.UpsertValues(right_values)

	var internal_node TreeNode
	if n.parent != nil && !n.parent.IsRootNode() {
		internal_node = n.parent
	} else {
		internal_node = newInternalNode(n.m)
		internal_node.SetParent(n.parent)
		n.parent.AddChild(internal_node)
		internal_node.AddChild(n)
		n.SetParent(internal_node)
	}
	internal_node.AddKey(mid_key)
	internal_node.AddChild(right_node)
	right_node.SetParent(internal_node)
	n.SetRightSibling(right_node)
	right_node.SetLeftSibling(n)

	if n.parent != nil && n.parent.NeedBalance() {
		n.parent.Balance()
	}
	return true
}

func (n *InternalNode) Balance() bool {
	QuickSort(n.keys, nil, n.children)
	mid := len(n.keys) / 2
	mid_key := n.keys[mid]
	left_keys := n.keys[0:mid]
	left_children := n.children[0:mid+1]
	right_keys := n.keys[mid+1:]
	right_children := n.children[mid+1:]

	right_node := newInternalNode(n.m)
	right_node.UpsertKeys(right_keys)
	right_node.UpsertChildren(right_children)
	for _, v := range(right_children) {
		v.SetParent(right_node)
	}

	// This modifies the underlying array of the slices
	n.UpsertKeys(left_keys)
	n.UpsertChildren(left_children)

	var internal_node TreeNode
	if n.parent != nil && !n.parent.IsRootNode() {
		internal_node = n.parent
	} else {
		internal_node = newInternalNode(n.m)
		internal_node.SetParent(n.parent)
		n.parent.AddChild(internal_node)
		internal_node.AddChild(n)
		n.SetParent(internal_node)
	}
	internal_node.AddKey(mid_key)
	internal_node.AddChild(right_node)
	right_node.SetParent(internal_node)

	if n.parent != nil && n.parent.NeedBalance() {
		n.parent.Balance()
	}
	return true
}

func (n *InternalNode) Insert(k string, v string) bool {
	i, _ := BinarySearch(n.keys, k, 0, len(n.keys), CHILDREN)
	return n.children[i].Insert(k, v)
}

func (n *LeafNode) Insert(k string, v string) bool {
	i, exist := BinarySearch(n.keys, k, 0, len(n.keys), VALUE)
	if exist {
		n.values[i] = v
		return true
	}
	if (len(n.keys) + 1) >= n.m {
		direction := n.GetRotationDirection()
		if !n.parent.IsRootNode() && direction != SPLIT {
			n.Rotate(direction)
		}
		n.keys = append(n.keys, k)
		n.values = append(n.values, v)
		n.Balance()
		return true
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

func (root *RootNode) All() iter.Seq[TreeNode]{
	return func(yield func(TreeNode) bool) {
		for n := range root.child.All() {
			yield(n)
		}
	}
}

func (n *InternalNode) All() iter.Seq[TreeNode]{
	return func(yield func(TreeNode) bool) {
		for _, c := range n.children {
			yield(c)
		}
	}
}

func (n *LeafNode) All() iter.Seq[TreeNode]{
	return func(yield func(TreeNode) bool) {
		yield(n)
	}
}
