package db

import (
	"testing"
	"slices"
	"strings"
	"math/rand"
	"time"
	//"fmt"
	//"reflect"
)

/*
func TestInsertAndFind(t *testing.T) {
	root := newRootNode(2)
	root.Insert("1", 1)
	root.Insert("2", 2)

	actual, _ := root.Find("2")

	if actual != 2 {
		t.Errorf("Expected %v. Actual %v", 2, actual)
	}
}

func TestInsertAndFindTwoLevels(t *testing.T) {
	root := newRootNode(2)
	root.Insert("1", 1)
	root.Insert("2", 2)
	root.Insert("3", 3)

	actual, _ := root.Find("3")

	expected_root := newRootNode(2)
	c1 := newChild("1", 1, nil, nil)
	c2 := newChild("2", 2, nil, nil)
	c3 := newChild("3", 3, nil, nil)
	left := newLeafNode(2, []*Child{c1}, nil)
	right := newLeafNode(2, []*Child{c2, c3}, nil)
	child := newChild("2", 0, left, right)
	i_node := newInternalNode(2, []*Child{child}, root)
	left.SetParent(i_node)
	right.SetParent(i_node)
	expected_root.child = i_node

	if actual != 3 {
		t.Errorf("Expected %v. Actual %v", 3, actual)
	}

	if !reflect.DeepEqual(root, expected_root) {
		t.Errorf("Expected %v. Actual %v", root, expected_root)
	}
}

func TestInsertAndFindThreeLevels(t *testing.T) {
	root := newRootNode(2)
	root.Insert("1", 1)
	root.Insert("2", 2)
	root.Insert("3", 3)
	root.Insert("4", 4)

	actual, _ := root.Find("2")

	expected_root := newRootNode(2)
	c1 := newChild("1", 1, nil, nil)
	c2 := newChild("2", 2, nil, nil)
	c3 := newChild("3", 3, nil, nil)
	c4 := newChild("4", 4, nil, nil)
	l1 := newLeafNode(2, []*Child{c1}, nil)
	l2 := newLeafNode(2, []*Child{c2}, nil)
	l3 := newLeafNode(2, []*Child{c3, c4}, nil)
	i1 := newChild("2", 0, l1, l2)
	i2 := newChild("3", 0, l2, l3)
	i_node := newInternalNode(2, []*Child{i1, i2}, root)
	l1.SetParent(i_node)
	l2.SetParent(i_node)
	l3.SetParent(i_node)
	expected_root.child = i_node

	if actual != 2 {
		t.Errorf("Expected %v. Actual %v", 2, actual)
	}

	if !reflect.DeepEqual(root, expected_root) {
		t.Errorf("Expected %v. Actual %v", root, expected_root)
	}
}

func TestInsertAndFindMultipleInternalNodes(t *testing.T) {
	root := newRootNode(2)
	root.Insert("1", 1)
	root.Insert("2", 2)
	root.Insert("3", 3)
	root.Insert("4", 4)
	root.Insert("5", 5)

	actual, _ := root.Find("4")

	expected_root := newRootNode(2)
	c1 := newChild("1", 1, nil, nil)
	c2 := newChild("2", 2, nil, nil)
	c3 := newChild("3", 3, nil, nil)
	c4 := newChild("4", 4, nil, nil)
	//c5 := newChild("5", 4, nil, nil)
	l1 := newLeafNode(2, []*Child{c1}, nil)
	l2 := newLeafNode(2, []*Child{c2}, nil)
	l3 := newLeafNode(2, []*Child{c3, c4}, nil)
	i1 := newChild("2", 0, l1, l2)
	i2 := newChild("3", 0, l2, l3)
	i_node := newInternalNode(2, []*Child{i1, i2}, root)
	l1.SetParent(i_node)
	l2.SetParent(i_node)
	l3.SetParent(i_node)
	expected_root.child = i_node

	if actual != 4 {
		t.Errorf("Expected %v. Actual %v", 2, actual)
	}

	if !reflect.DeepEqual(root, expected_root) {
		t.Errorf("Expected %v. Actual %v", root, expected_root)
	}
}*/

func TestBinarySearchLesserOrGreater(t *testing.T) {
	keys := []string{"a", "c", "e", "g", "i"}
	actual_index := BinarySearch(keys, "b", 0, len(keys))
	expected_index := 1

	if actual_index != expected_index {
		t.Errorf("Expected %d. Actual %d", expected_index, actual_index)
	}
}

func TestBinarySearchEqual(t *testing.T) {
	keys := []string{"a", "c", "e", "g", "i"}
	actual_index := BinarySearch(keys, "c", 0, len(keys))
	expected_index := 2

	if actual_index != expected_index {
		t.Errorf("Expected %d. Actual %d", expected_index, actual_index)
	}
}

func TestBinarySearchEdge(t *testing.T) {
	keys := []string{"a", "c", "e", "g", "i"}
	actual_index := BinarySearch(keys, "j", 0, len(keys))
	expected_index := len(keys)

	if actual_index != expected_index {
		t.Errorf("Expected %d. Actual %d", expected_index, actual_index)
	}
}

func TestQuickSort(t *testing.T) {
	keys := generateStringArrays(1, 5)
	expected_sorted_keys := make([]string, len(keys))
	copy(expected_sorted_keys, keys)
	slices.SortFunc(expected_sorted_keys, func(a, b string) int {
			return strings.Compare(strings.ToLower(a), strings.ToLower(b))
	})
	QuickSort(keys)

	if !slices.Equal(expected_sorted_keys, keys) {
		t.Errorf("Expected %v. Actual %v", expected_sorted_keys, keys)
	}
}

func TestQuickSortTwoItems(t *testing.T) {
	keys := generateStringArrays(1, 2)
	expected_sorted_keys := make([]string, len(keys))
	copy(expected_sorted_keys, keys)
	slices.SortFunc(expected_sorted_keys, func(a, b string) int {
			return strings.Compare(strings.ToLower(a), strings.ToLower(b))
	})
	QuickSort(keys)

	if !slices.Equal(expected_sorted_keys, keys) {
		t.Errorf("Expected %v. Actual %v", expected_sorted_keys, keys)
	}
}

func generateString(m int) string {
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	charset := "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, m)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	// Using a strings.Builder can be more efficient than direct string
	// concatenation
	var sb strings.Builder
	sb.Write(b)
	return sb.String()
}

func generateStringArrays(m int, n int) []string {
	vals := make([]string, n)
	for i := 0; i < n; i++ {
		s := generateString(m)
		vals[i] = s
	}
	return vals
}

