package db

import (
	"testing"
)

func TestInsertAndFind(t *testing.T) {
	root := newRootNode(2)
	root.Insert("1", 1)
	root.Insert("2", 2)

	actual, _ := root.Find("2")

	if actual != 2 {
		t.Errorf("Expected %v. Actual %v", 2, actual)
	}
}

func TestInsertAndFind(t *testing.T) {
	root := newRootNode(2)
	root.Insert("1", 1)
	root.Insert("2", 2)
	root.Insert("3", 3)

	actual, _ := root.Find("2")

	expected_root := newRootNode(2)
	c1 := newChild("1", 1, nil, nil)
	c2 := newChild("2", 2, nil, nil)
	c3 := newChild("3", 3, nil, nil)
	left := newLeafNode(2, []*Child{c1, c2}, nil)
	right := newLeafNode(2, []*Child{c3}, nil)
	child := newChild("2", 0, left, right)
	i_node := newInternalNode(2, []*Child{child}, root)
	left.parent = i_node
	right.parent = i_node
	expected_root.child = i_node

	if actual != 3 {
		t.Errorf("Expected %v. Actual %v", 3, actual)
	}

	if !reflect.DeepEqual(root, expected_root) {
		t.Errorf("Expected %v. Actual %v", root, expected_root)
	}
}
