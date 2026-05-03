package db

import (
	"testing"
	"reflect"
)

func TestFindReturnsValueSucceeds(t *testing.T) {
	n := initBTreeNode(3)
	
	n.keys = append(n.keys, "1")
	n.keys = append(n.keys, "2")
	n.values = append(n.values, "One")
	n.values = append(n.values, "Two")
	
	actual, _ := n.find("1")
	expected := "One"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}


func TestFindNotFoundReturnsFalse(t *testing.T) {
	n := initBTreeNode(3)
	n.keys = append(n.keys, "1")
	n.keys = append(n.keys, "2")
	n.values = append(n.values, "One")
	n.values = append(n.values, "Two")
	
	_, actual := n.find("3")
	expected := false

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}


func TestFindWithChildrenLessThanReturnsValue(t *testing.T) {
	n1 := initBTreeNode(3)
	n1.keys = append(n1.keys, "2")

	n2 := initBTreeNode(3)
	n2.keys = append(n2.keys, "1")
	n2.keys = append(n2.keys, "2")
	n2.values = append(n2.values, "One")
	n2.values = append(n2.values, "Two")

	n1.children = append(n1.children, n2)
	
	actual, _ := n1.find("2")
	expected := "Two"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}

func TestFindWithChildrenGreaterThanReturnsValue(t *testing.T) {
	n1 := initBTreeNode(3)
	n1.keys = append(n1.keys, "2")

	n2 := initBTreeNode(3)
	n2.keys = append(n2.keys, "1")
	n2.keys = append(n2.keys, "2")
	n2.values = append(n2.values, "One")
	n2.values = append(n2.values, "Two")

	n3 := initBTreeNode(3)
	n3.keys = append(n3.keys, "3")
	n3.keys = append(n3.keys, "4")
	n3.values = append(n3.values, "Three")
	n3.values = append(n3.values, "Four")


	n1.children = append(n1.children, n2)
	n1.children = append(n1.children, n3)
	
	actual, _ := n1.find("3")
	expected := "Three"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}

func TestFindValueInsertion(t *testing.T) {
	n := initBTreeNode(4)
	n.insert("1", "One")
	n.insert("2", "Two")
	
	actual, _ := n.find("1")
	expected := "One"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}

func TestFindValueInsertionSplit(t *testing.T) {
	n := initBTreeNode(4)
	n.insert("1", "One")
	n.insert("2", "Two")
	actual_split, is_split := n.insert("3", "Three")

	expected_node := initBTreeNode(4)
	expected_node.insert("3", "Three")

	expected_split_result := &splitResult { "2", expected_node }

	if is_split != true {
		t.Errorf("Expected to split")
	}

	if !reflect.DeepEqual(actual_split, expected_split_result) {
		t.Errorf("Expected %v. Actual %v", expected_split_result, actual_split)
	}

	if !reflect.DeepEqual(actual_split.right, n.sibling) {
		t.Errorf("Expected %v. Actual %v", n.sibling, actual_split.right)
	}
}

func TestBTreeInsertFindSucceeds(t *testing.T) {
	tr := initBTree(4)
	tr.Insert("1", "One")
	tr.Insert("2", "Two")

	actual, _ := tr.Find("1")
	expected := "One"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", actual, expected)
	}
}

func TestBTreeInsertThenSplitAndFindSucceeds(t *testing.T) {
	tr := initBTree(4)
	tr.Insert("1", "One")
	tr.Insert("2", "Two")
	tr.Insert("3", "Three")
	tr.Insert("4", "Four")

	expected_tr := initBTree(4)
	n1 := initBTreeNode(4)
	n1.keys = append(n1.keys, "1")
	n1.keys = append(n1.keys, "2")
	n1.values = append(n1.values, "One")
	n1.values = append(n1.values, "Two")

	n2 := initBTreeNode(4)
	n2.keys = append(n2.keys, "3")
	n2.keys = append(n2.keys, "4")
	n2.values = append(n2.values, "Three")
	n2.values = append(n2.values, "Four")

	n1.sibling = n2

	root := initBTreeNode(4)
	root.keys = append(root.keys, "2")

	root.children = append(root.children, n1)
	root.children = append(root.children, n2)

	expected_tr.root = root

	actual, _ := tr.Find("4")
	expected := "Four"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", actual, expected)
	}

	if !reflect.DeepEqual(tr, expected_tr) {
		t.Errorf("Expected %v. Actual %v", expected_tr, tr)
	}
}


