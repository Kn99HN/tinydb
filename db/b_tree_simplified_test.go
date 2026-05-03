package db

import (
	"testing"
	//"fmt"
)

func TestFindReturnsValueSucceeds(t *testing.T) {
	n := initBTree(3)
	n.keys = append(n.keys, "1")
	n.keys = append(n.keys, "2")
	n.values = append(n.values, "One")
	n.values = append(n.values, "Two")
	
	actual, _ := n.Find("1")
	expected := "One"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}


func TestFindNotFoundReturnsFalse(t *testing.T) {
	n := initBTree(3)
	n.keys = append(n.keys, "1")
	n.keys = append(n.keys, "2")
	n.values = append(n.values, "One")
	n.values = append(n.values, "Two")
	
	_, actual := n.Find("3")
	expected := false

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}


func TestFindWithChildrenLessThanReturnsValue(t *testing.T) {
	n1 := initBTree(3)
	n1.keys = append(n1.keys, "2")

	n2 := initBTree(3)
	n2.keys = append(n2.keys, "1")
	n2.keys = append(n2.keys, "2")
	n2.values = append(n2.values, "One")
	n2.values = append(n2.values, "Two")

	n1.children = append(n1.children, n2)
	
	actual, _ := n1.Find("2")
	expected := "Two"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}

func TestFindWithChildrenGreaterThanReturnsValue(t *testing.T) {
	n1 := initBTree(3)
	n1.keys = append(n1.keys, "2")

	n2 := initBTree(3)
	n2.keys = append(n2.keys, "1")
	n2.keys = append(n2.keys, "2")
	n2.values = append(n2.values, "One")
	n2.values = append(n2.values, "Two")

	n3 := initBTree(3)
	n3.keys = append(n3.keys, "3")
	n3.keys = append(n3.keys, "4")
	n3.values = append(n3.values, "Three")
	n3.values = append(n3.values, "Four")


	n1.children = append(n1.children, n2)
	n1.children = append(n1.children, n3)
	
	actual, _ := n1.Find("3")
	expected := "Three"

	if actual != expected {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}

