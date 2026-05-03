package db

import (
	//"fmt"
	"slices"
	"strings"
	//"iter"
)

type NodeType int

const (
 LEAF NodeType = iota
 INTERNAL
)

type BTree struct {
	root *BTreeNode
}

type BTreeNode struct {
 m int
 children []*BTreeNode
 keys []string
 values []string
 sibling *BTreeNode
}

type splitResult struct {
	promoted_key string
	right *BTreeNode
}

func initBTree(m int) *BTree {
	return &BTree{ root: initBTreeNode(m) }
}

func (r *BTree) Find(s string) (string, bool) {
	return r.root.find(s)
}

func (r *BTree) Insert(s string, v string) {
	r.root.insert(s, v)
}

func initBTreeNode(m int) *BTreeNode {
	return &BTreeNode{ m,
	 make([]*BTreeNode, 0, m),
	 make([]string, 0, m - 1), make([]string, 0, m - 1), nil}
}

func (n *BTreeNode) find(s string) (string, bool) {
	idx, found := slices.BinarySearch(n.keys, s)
	key_idx := idx
	max_number_of_keys := (*n).m - 1
	if idx >= max_number_of_keys {
	 key_idx -= 1
	}
	current_children := len((*n).children)
	var child *BTreeNode
	if idx < current_children {
		child = (*n).children[idx]
	}
	if child == nil && found { return n.values[key_idx], true }
	if child == nil && !found { return "", false }
	return child.find(s)
}


func (n *BTreeNode) insert(s string, v string) (*splitResult, bool) {
 idx, _ := slices.BinarySearch(n.keys, s)
 key_idx := idx
 max_number_of_keys := (*n).m - 1
 if idx >= max_number_of_keys {
		key_idx -= 1
 }
 current_children := len((*n).children)
 var child *BTreeNode
 if idx < current_children {
		child = (*n).children[idx]
 }
 if child == nil {
		n.keys, n.values, _ = insertSorted(n.keys, n.values, s, LEAF)
		current_children = len((*n).keys)
		if current_children == max_number_of_keys {
			return split(n), true
		}
		return nil, false
 }
 split_result, is_split := child.insert(s, v)
 if is_split {
		var i int
		n.keys, n.values, i = insertSorted(n.keys, n.values, split_result.promoted_key, INTERNAL)
		current_children = len((*n).keys)
		n.children[i + 1] = split_result.right
		if current_children == max_number_of_keys {
			return split(n), true
		}
		return nil, false
 }
 return nil, false
}

func split(n *BTreeNode) *splitResult {
	idx := len((*n).keys) / 2
	promoted_key := (*n).keys[idx]
	right_keys := (*n).keys[idx:]
	(*n).keys = (*n).keys[0:idx]
	right_values := (*n).values[idx:]
	(*n).values = (*n).values[0:idx]
	right_node := initBTreeNode((*n).m)
	right_node.keys = right_keys
	right_node.values = right_values
	return &splitResult{ promoted_key, right_node }
}

func insertSorted(keys []string, 
	vals []string, val string, t NodeType) ([]string, []string, int) {
	keys = append(keys, "0")        // grow by 1
  i := len(keys) - 1
	for i > 0 && strings.Compare(keys[i-1], val) > 0 {
	 keys[i] = keys[i-1]       // shift right
	 if t == LEAF {
	 	vals[i] = vals[i - 1]
	 }
	 i--
	}
	keys[i] = val
	if t == LEAF {
		vals[i] = val
		return keys, vals, i
	}
	return keys, vals, i
}
