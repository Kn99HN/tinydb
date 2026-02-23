package db

import (
	"reflect"
	"testing"
	"slices"
	"os"
	"log"
	//"fmt"
)

type StaticScanNode struct {
	r []Record
	i int
}

func initStaticScan(movies []Record) Iterator {
	return &StaticScanNode{ movies, 0 }
}

func (s *StaticScanNode) next() *Record {
	index := (*s).i
	max := len((*s).r)
	if index >= max {
		return nil
	}
	ret := (*s).r[index]
	(*s).i = (*s).i + 1
	return &ret
}

func makeMovies() []Record {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 := makeRecord("2", "Movie 2", "2", "2")
	m3 := makeRecord("3", "Movie 3", "3", "3")

	return []Record{ m1, m2, m3 }
}

func staticScanConstructor(p NodeParser, n *Node) Iterator {
	return initStaticScan(makeMovies())
}

func TestScanNode(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 := makeRecord("2", "Movie 2", "2", "2")
	movies := []Record{m1, m2}
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)

	r1 := s.next()
	if !reflect.DeepEqual(*r1, m1) {
		t.Errorf("Expected %s. Actual %s", m1, r1)
	}
	r2 := s.next()
	if !reflect.DeepEqual(*r2, m2) {
		t.Errorf("Expected %s. Actual %s", m2, r2)
	}
}

func TestChildNode(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 :=  makeRecord("2", "Movie 2", "2", "2")
	movies := []Record{ m1, m2}
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)
	l := initLimitNode(2, s)

	r1 := l.next()
	if !reflect.DeepEqual(*r1, m1) {
		t.Errorf("Expected %s. Actual %s", m1, r1)
	}
	r2 := l.next()
	if !reflect.DeepEqual(*r2, m2) {
		t.Errorf("Expected %s. Actual %s", m2, r2)
	}
	r3 := l.next()
	if r3 != nil {
		t.Errorf("Expected %s. Actual %s", m2, r2)
	}
}

func TestSelectionNode(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 :=  makeRecord("2", "Movie 2", "2", "2")
	movies := []Record{ m1, m2 }
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)

	left := initPredicateExpression("Id", EQ, "1")
	expression := initPredicateExpressions(left, AND, nil)

	sel := initSelectionNode(expression, s)

	r1 := sel.next()
	if !reflect.DeepEqual(*r1, m1)  {
		t.Errorf("Expected %s. Actual %s", m1, r1)
	}
	r2 := sel.next()
	if r2 != nil {
		t.Errorf("Expected %s. Actual %s", m2, r2)
	}
}

func TestProjectionNode(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 :=  makeRecord("2", "Movie 2", "2", "2")
	movies := []Record{ m1, m2 }
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)

	p := initProjectionNode([]string{"Name"}, s)

	r1 := p.next()
	expected_v, _ := (*r1).getColumn("Name")
	actual_v, _ := m1.getColumn("Name")
	if expected_v != actual_v {
		t.Errorf("Expected %s. Actual %s", m1, r1)
	}
	if expected_v, _ = (*r1).getColumn("Id"); expected_v != "" {
		t.Errorf("Expected %s. Actual %s", m1, r1)
	}
}

func TestSortNode(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 := makeRecord("2", "Movie 2", "2", "2")
	m3 := makeRecord("3", "Movie 3", "3", "3")
	movies := []Record{
		m3, m1, m2,
	}
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)

	sort := initSortNode(s, []SortTuple{
		SortTuple{"Id", ASC},
	})

	r1 := sort.next()
	if !reflect.DeepEqual(*r1, m1) {
		t.Errorf("Expected %s. Actual %s", m1, r1)
	}
	r2 := sort.next()
	if !reflect.DeepEqual(*r2, m2)  {
		t.Errorf("Expected %s. Actual %s", m2, r2)
	}
	r3 := sort.next()
	if !reflect.DeepEqual(*r3, m3) {
		t.Errorf("Expected %s. Actual %s", m3, r3)
	}
}

func TestSelectionNodeAndPredicate(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "2")
	m2 :=  makeRecord("2", "Movie 2", "1", "1")
	movies := []Record{ m1, m2 }
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)

	left := initPredicateExpression("Id", EQ, "1")
	right := initPredicateExpressions(initPredicateExpression("Year", EQ, "2"), AND, nil)
	pexpressions := initPredicateExpressions(left, AND, right)

	sel := initSelectionNode(pexpressions, s)

	r1 := sel.next()
	if !reflect.DeepEqual(*r1, m1) {
		t.Errorf("Expected %s. Actual %s", m1, r1)
	}
	r2 := sel.next()
	if r2 != nil {
		t.Errorf("Expected %s. Actual %s", m2, r2)
	}
}

func TestSelectionNodeOrPredicate(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 :=  makeRecord("2", "Movie 2", "2", "2")
	m3 :=  makeRecord("3", "Movie 3", "3", "3")
	movies := []Record{ m1, m2, m3 }
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)

	left := initPredicateExpression("Id", EQ, "1")
	right := initPredicateExpressions(initPredicateExpression( "Id", EQ, "2" ), AND, nil)
	pexpressions := initPredicateExpressions(left, OR, right)

	sel := initSelectionNode(pexpressions, s)

	r1 := sel.next()
	if !reflect.DeepEqual(*r1, m1) {
		t.Errorf("Expected %s. Actual %s", m1, r1)
	}
	r2 := sel.next()
	if !reflect.DeepEqual(*r2, m2) {
		t.Errorf("Expected %s. Actual %s", m2, r2)
	}
	r3 := sel.next()
	if r3 != nil {
		t.Errorf("Expected %s. Actual %s", m2, r2)
	}
}

func TestCountNode(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 := makeRecord("2", "Movie 1", "2", "2")
	m3 := makeRecord("3", "Movie 1", "3", "3")
	movies := []Record{
		m1, m2, m3,
	}
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)
	c := initCountNode(s, []string{"Name"})

	r1 := c.next()
	expected_c1 := Record{ values: map[string]string{
		"Name": "Movie 1",
		"Count": "3",
	}}
	if !reflect.DeepEqual(*r1, expected_c1) {
		t.Errorf("Expected %s. Actual %s", expected_c1, r1)
	}
}

func TestCountNodeCompositeKey(t *testing.T) {
	m1 := makeRecord("1", "Movie 1", "1", "1")
	m2 := makeRecord("2", "Movie 1", "1", "2")
	m3 := makeRecord("3", "Movie 1", "3", "3")
	movies := []Record{
		m1, m2, m3,
	}
	scanner := initStaticScan(movies)
	s := initScanNode(scanner)
	c := initCountNode(s, []string{"Name", "Id"})
	sort := initSortNode(c, []SortTuple{ SortTuple{ "Name", ASC, }, SortTuple{ "Id", ASC, }  } )

	r1 := sort.next()
	expected_c1 := Record{ values: map[string]string{
		"Name": "Movie 1",
		"Id": "1",
		"Count": "2",
	}}
	
	if !reflect.DeepEqual(*r1, expected_c1) {
		t.Errorf("Expected %s. Actual %s",  expected_c1, r1)
	}

	r2 := sort.next()
	expected_c2 := Record{ values: map[string]string{
		"Name": "Movie 1",
		"Id": "3",
		"Count": "1",
	}}
	if !reflect.DeepEqual(*r2, expected_c2) {
		t.Errorf("Expected %s. Actual %s", expected_c2, r2)
	}

}

func TestGenerateTree(t *testing.T) {
	b := ` {"head": { "name": "SCAN", "args": ["movies"], "child": null } }`
	s := &Node{ "SCAN", []interface{}{"movies"}, nil }
	e_t := &Tree { s }

	a_t := generateTree(b)

	if !reflect.DeepEqual(a_t,e_t) {
		t.Errorf("Expected %s. Actual %s", e_t, a_t)
	}
}

func TestGenerateQueryTree(t *testing.T) {
	b := ` {"head": { "name": "SCAN", "args": {}, "child": {
		"name": "STATIC_SCAN"
	}} }`
	Registry["STATIC_SCAN"] = staticScanConstructor
	defer delete(Registry, "STATIC_SCAN")
	a_t := generateTree(b)
	query_t := transformToQueryTree(a_t)
	scanner :=  initStaticScan(makeMovies())
	expected_query_t := initScanNode(scanner)

	if !reflect.DeepEqual(expected_query_t, query_t) {
		t.Errorf("Expected %#v. Actual %#v", expected_query_t, query_t)
	}
}

func TestGenerateTreeProjection(t *testing.T) {
	b := ` {"head": { "name": "PROJECTION", "args": ["Name", "Id"], "child": null } }`
	s := &Node{ "PROJECTION", []interface{}{"Name", "Id"}, nil }
	e_t := &Tree { s }

	a_t := generateTree(b)
	if !reflect.DeepEqual(a_t,e_t) {
		t.Errorf("Expected %v. Actual %v", e_t, a_t)
	}
}

func TestGenerateQueryTreeProjection(t *testing.T) {
	b := ` {"head": { "name": "PROJECTION", "args": ["Name", "Id"], "child": {
		"name": "STATIC_SCAN"
	}} }`
	a_t := generateTree(b)
	query_t := transformToQueryTree(a_t)
	expected_query_t := initProjectionNode([]string{"Name", "Id"}, nil)

	if !reflect.DeepEqual(query_t, expected_query_t) {
		t.Errorf("Expected %#v. Actual %#v", expected_query_t, query_t)
	}
}

func TestGenerateQueryTreeProjectionWithChild(t *testing.T) {
	b := ` {"head": { "name": "PROJECTION", "args": ["Name", "Id"], "child": {
			"name": "SCAN", "args": {}, "child": {
				"name": "STATIC_SCAN"
			} }} }`
	Registry["STATIC_SCAN"] = staticScanConstructor
	defer delete(Registry, "STATIC_SCAN")
	a_t := generateTree(b)
	query_t := transformToQueryTree(a_t)
	
	scanner := initStaticScan(makeMovies())
	scan_node := initScanNode(scanner)
	expected_query_t := initProjectionNode([]string{"Name", "Id"}, scan_node)

	if !reflect.DeepEqual(query_t, expected_query_t) {
		t.Errorf("Expected %#v. Actual %#v", expected_query_t, query_t)
	}
}

func TestEvaluateQueryTreeProjectionWithChild(t *testing.T) {
	b := ` {"head": { "name": "PROJECTION", "args": ["Name", "Id"], "child": {
			"name": "SCAN", "args": {}, "child": {
				"name": "STATIC_SCAN"
			}}} }`
	Registry["STATIC_SCAN"] = staticScanConstructor
	defer delete(Registry, "STATIC_SCAN")
	a_t := generateTree(b)
	actual_query_t := transformToQueryTree(a_t)
	
	scanner := initStaticScan(makeMovies())
	scan_node := initScanNode(scanner)
	expected_query_t := initProjectionNode([]string{"Name", "Id"}, scan_node)

	for expected := expected_query_t.next(); expected != nil; expected = expected_query_t.next() {
		actual := actual_query_t.next()
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected %#v. Actual %#v", expected, actual)
		}
	}
}


func TestGenerateTreeLimit(t *testing.T) {
	b := ` {"head": { "name": "LIMIT", "args": ["1"], "child": null } }`
	s := &Node{ "LIMIT", []interface{}{"1"}, nil }
	e_t := &Tree { s }

	a_t := generateTree(b)
	if !reflect.DeepEqual(a_t,e_t) {
		t.Errorf("Expected %v. Actual %v", e_t, a_t)
	}
}

func TestGenerateQueryTreeLimit(t *testing.T) {
	b := ` {"head": { "name": "LIMIT", "args": ["1"], "child": null } }`
	a_t := generateTree(b)
	query_t := transformToQueryTree(a_t)
	expected_query_t := initLimitNode(1, nil)

	if !reflect.DeepEqual(expected_query_t, query_t) {
		t.Errorf("Expected %#v. Actual %#v", expected_query_t, query_t)
	}
}

func TestEvaluateQueryLimit(t *testing.T) {
	b := ` {"head": { "name": "LIMIT", "args": ["1"], "child": {
		"name": "SCAN", "args": {}, "child": {
			"name": "STATIC_SCAN"
		}
	} } }`
	Registry["STATIC_SCAN"] = staticScanConstructor
	defer delete(Registry, "STATIC_SCAN")
	a_t := generateTree(b)
	actual_query_t := transformToQueryTree(a_t)
	
	scanner := initStaticScan(makeMovies())
	scan_node := initScanNode(scanner)
	expected_query_t := initLimitNode(1, scan_node)

	expected := expected_query_t.next()
	actual := actual_query_t.next()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %#v. Actual %#v", expected, actual)
	}

	if actual_query_t.next() != nil {
		t.Errorf("Expected nil. Actual %#v", actual)
	}
}

func TestGenerateTreeCount(t *testing.T) {
	b := ` {"head": { "name": "COUNT", "args": ["Name"], "child": null } }`
	s := &Node{ "COUNT", []interface{}{"Name"}, nil }
	e_t := &Tree { s }

	a_t := generateTree(b)
	if !reflect.DeepEqual(a_t,e_t) {
		t.Errorf("Expected %v. Actual %v", e_t, a_t)
	}
}

func TestGenerateQueryTreeCount(t *testing.T) {
	b := ` {"head": { "name": "COUNT", "args": ["Name"], "child": null } }`
	a_t := generateTree(b)
	query_t := transformToQueryTree(a_t)
	expected_query_t := initCountNode(nil, []string{"Name"})

	if !reflect.DeepEqual(expected_query_t, query_t) {
		t.Errorf("Expected %#v. Actual %#v", expected_query_t, query_t)
	}
}

func TestEvaluateQueryCount(t *testing.T) {
	b := ` {"head": { "name": "COUNT", "args": ["Name"], "child": {
		"name": "SCAN", "args": {}, "child": { "name": "STATIC_SCAN" }
	} } }`
	Registry["STATIC_SCAN"] = staticScanConstructor
	defer delete(Registry, "STATIC_SCAN")
	a_t := generateTree(b)
	actual_query_t := transformToQueryTree(a_t)
	
	scanner := initStaticScan(makeMovies())
	scan_node := initScanNode(scanner)
	expected_query_t := initCountNode(scan_node, []string{"Name"})

	expected_arr := make([]Record, 0)
	for v := expected_query_t.next(); v!= nil; v = expected_query_t.next() {
		expected_arr = append(expected_arr, *v)
		slices.SortFunc(expected_arr, func(a, b Record) int {
			if a.values["Name"] < b.values["Name"] { return -1 }
			if a.values["Id"] < b.values["Id"] { return -1 }
			if a.values["Year"] < b.values["Year"] { return -1 }
			return 0
		})
	}
	actual_arr := make([]Record, 0)
	for v := actual_query_t.next(); v!= nil; v = actual_query_t.next() {
		actual_arr = append(actual_arr, *v)
		slices.SortFunc(actual_arr, func(a, b Record) int {
			if a.values["Name"] < b.values["Name"] { return -1 }
			if a.values["Id"] < b.values["Id"] { return -1 }
			if a.values["Year"] < b.values["Year"] { return -1 }
			return 0
		})
	}

	if !reflect.DeepEqual(expected_arr, actual_arr) {
		t.Errorf("Expected %#v. Actual %#v", expected_arr, actual_arr)
	}

}

func TestGenerateTreeSelectionSingleton(t *testing.T) {
	b := ` {"head": { "name": "SELECTION", "args": [["Id", "EQ", "1"]], "child": null } }`
	s := &Node{ "SELECTION", []interface{}{[]interface{}{"Id", "EQ", "1"}}, nil }
	e_t := &Tree { s }

	a_t := generateTree(b)
	if !reflect.DeepEqual(a_t,e_t) {
		t.Errorf("Expected %v. Actual %v", e_t, a_t)
	}
}

func TestGenerateQueryTreeSelectionSingleton(t *testing.T) {
	b := ` {"head": { "name": "SELECTION", "args": {"AND": {
		"EQ": ["Id", "1"]
	}}, "child": null } }`
	a_t := generateTree(b)
	query_t := transformToQueryTree(a_t)
	left := initPredicateExpression("Id", EQ, "1")
	exp := initPredicateExpressions(left, AND, nil)
	expected_query_t := initSelectionNode(exp, nil)

	if !reflect.DeepEqual(expected_query_t, query_t) {
		t.Errorf("Expected %#v. Actual %#v", expected_query_t, query_t)
	}
}

func TestEvaluateSelectionQueryTreeSingleton(t *testing.T) {
	b := ` {"head": { "name": "SELECTION", "args": {"AND": {
		"EQ": ["Id", "1"]
	}}, "child": {
		"name": "SCAN", "args": {}, "child": {
			"name": "STATIC_SCAN"
		}
	} } }`
	Registry["STATIC_SCAN"] = staticScanConstructor
	defer delete(Registry, "STATIC_SCAN")
	a_t := generateTree(b)
	actual_query_t := transformToQueryTree(a_t)
	left := initPredicateExpression("Id", EQ, "1")
	exp := initPredicateExpressions(left, AND, nil)
	
	scanner := initStaticScan(makeMovies())
	scan_node := initScanNode(scanner)
	expected_query_t := initSelectionNode(exp, scan_node)

	expected := expected_query_t.next()
	actual := actual_query_t.next()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %#v. Actual %#v", expected, actual)
	}

	if actual = actual_query_t.next(); actual != nil {
		t.Errorf("Expected nil. Actual %#v", actual)
	}
}

func TestGenerateTreeSelectionMultipleExpressions(t *testing.T) {
	b := ` {"head": { "name": "SELECTION", "args": {
		"AND": {
			"EQ": ["Id", "1"],
			 "OR": {
				"EQ": ["Year", "1"]
			 }
		}
	}, "child": null } }`
	s := &Node{ "SELECTION", map[string]interface{}{
		"AND": map[string]interface{}{ "EQ": []interface{}{"Id", "1"},
			"OR": map[string]interface{}{"EQ": []interface{}{"Year", "1"}}}}, nil}
	e_t := &Tree { s }

	a_t := generateTree(b)
	if !reflect.DeepEqual(a_t,e_t) {
		t.Errorf("Expected %v. Actual %v", e_t, a_t)
	}
}

func TestGenerateQueryTreeSelectionMultipleExpressions(t *testing.T) {
	b := ` {"head": { "name": "SELECTION", "args": {
		"AND": {
			"EQ": ["Id", "1"],
			"OR": {
				"EQ": ["Year", "1"]
			}
		}}, "child": null } }`
	a_t := generateTree(b)
	query_t := transformToQueryTree(a_t)
	left := initPredicateExpression("Id", EQ, "1")
	right_exp := initPredicateExpression("Year", EQ, "1")
	right := initPredicateExpressions(right_exp, OR, nil)
	exp := initPredicateExpressions(left, AND, right)
	expected_query_t := initSelectionNode(exp, nil)

	if !reflect.DeepEqual(expected_query_t, query_t) {
		t.Errorf("Expected %v. Actual %v", expected_query_t, query_t)
	}
}

func TestGenerateQueryTreeSelectionMultipleExpr(t *testing.T) {
	b := ` {"head": { "name": "SELECTION", "args": {
		"AND": {
			"EQ": ["Id", "1"],
			"OR": {
				"EQ": ["Year", "2"]
			}
		}}, "child": {
			"name": "SCAN", "args": {}, "child": {
				"name": "STATIC_SCAN"
			}
		}} }`
	Registry["STATIC_SCAN"] = staticScanConstructor
	defer delete(Registry, "STATIC_SCAN")
	a_t := generateTree(b)
	actual_query_t := transformToQueryTree(a_t)
	left := initPredicateExpression("Id", EQ, "1")
	right_exp := initPredicateExpression("Year", EQ, "1")
	right := initPredicateExpressions(right_exp, OR, nil)
	exp := initPredicateExpressions(left, AND, right)

	scanner := initStaticScan(makeMovies())
	scan_node := initScanNode(scanner)
	expected_query_t := initSelectionNode(exp, scan_node)

	expected := expected_query_t.next()
	actual := actual_query_t.next()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}

	expected = expected_query_t.next()
	actual = actual_query_t.next()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v. Actual %v", expected, actual)
	}
}

func TestGenerateTreeSortNode(t *testing.T) {
	b := `{"head": { "name": "SORT", "args": {
		"sorted_args": ["Id:ASC"]
	}, "child": null } }`
	s := &Node{ "SORT", map[string]interface{}{
		"sorted_args": []interface{}{"Id:ASC"},
		}, nil}
	e_t := &Tree { s }

	a_t := generateTree(b)
	if !reflect.DeepEqual(a_t,e_t) {
		t.Errorf("Expected %v. Actual %v", e_t, a_t)
	}
}

func TestGenerateQueryTreeSortNode(t *testing.T) {
	b := `{"head": { "name": "SORT", "args": {
		"sorted_args": ["Id:DESC"]
	}, "child": {
		"name": "SCAN", "args": {}, "child": {
			"name": "STATIC_SCAN"
		}
	}} }`
	Registry["STATIC_SCAN"] = staticScanConstructor
	defer delete(Registry, "STATIC_SCAN")
	a_t := generateTree(b)
	actual_query_t := transformToQueryTree(a_t)

	scanner := initStaticScan(makeMovies())
	scan_node := initScanNode(scanner)
	sort_node := initSortNode(scan_node, []SortTuple{
		SortTuple{ "Id", DESC },
	})

	for n := actual_query_t.next(); n != nil; n = actual_query_t.next() {
		e := sort_node.next()
		if !reflect.DeepEqual(n, e) {
			t.Errorf("Expected %v. Actual %v", e, n)
		}
	}
}

func TestFileScanNodeGroup(t *testing.T) {
	const dir = "./test"
	const perm = 0750
	
	func() {
		err := os.Mkdir(dir, perm)
		if err != nil && !os.IsExist(err) {
			log.Fatal(err)
		}
		wr := initStorageWriter(dir, 0)
		records := makeMovies()
		d := make([]Data, 0)
		for _, r := range(records) {
			size := len(r.key)
			cols := make([]Column, 0)
			for name, val := range(r.values) {
				size += len(name) + len(val)
				cols = append(cols, Column{ name, val })
			}
			d = append(d, Data{ r.key, cols, uint32(size) })
		}
		for _, r := range(d) {
			succeeded := wr.Write(&r)
			if !succeeded {
				t.Errorf("Failed to write data")
			}
		}
		wr.Flush()
	}()
	
	// Ensure cleanup happens even if a test panics
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}()

	t.Run("SubTest1", func(t *testing.T) {
		b := `{"head": { "name": "SCAN", "args": {}, "child": {
			"name": "FILE_SCAN", "args": {"dir": "test", "file_number": "0"}
		}} }`
		a_t := generateTree(b)
		actual_query_t := transformToQueryTree(a_t)
		
		reader := initStorageReader(dir, 0)
		fscan_node := initFileScanNode(reader)
		scan_node := initScanNode(fscan_node)
		
		for n := actual_query_t.next(); n != nil; n = actual_query_t.next() {
			e := scan_node.next()
			if !reflect.DeepEqual(n, e) {
				t.Errorf("Expected %v. Actual %v", e, n)
			}
		}
	})
}



