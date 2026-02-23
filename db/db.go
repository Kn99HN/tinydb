package db

import (
	"fmt"
	"slices"
	"encoding/json"
	"strconv"
	"strings"
	"log"
)


type Record struct {
	key string
	values map[string]string
}

func (r Record) String() string {
	output := r.key
	for k, v := range(r.values) {
		output += fmt.Sprintf("%s: %s,", k, v)
	}
	return output
}

func (r Record) getColumn(column string) (string, error) {
	v, ok := r.values[column]
	if !ok {
		return "", fmt.Errorf("No column %s for record", column)
	}
	return v, nil
}

func makeRecord(row_key string, name string, id string, year string) Record {
	return Record{ key: row_key, values: map[string]string {
		"Name": name,
		"Id": id,
		"Year": year,
	}}
}

type Iterator interface {
	next() *Record
}

type ScanNode struct {
	child *Iterator
}

func initScanNode(scanner Iterator) *ScanNode {
 return &ScanNode{ &scanner }
}

func (s *ScanNode) next() *Record {
	return (*s.child).next()
}

type LimitNode struct {
	limit uint32
	i uint32
	child *Iterator
}

func initLimitNode(limit uint32, child Iterator) *LimitNode {
	// assuming 1 iterator at this time for simplicity
	return &LimitNode{ limit, 0, &child}
}

func (l *LimitNode) next() *Record {
	if (*l).i == (*l).limit { return nil }
	r := (*l.child).next()
	(*l).i += 1
	return r
}

type Op int
type CompOp int

const (
	OR Op = iota
	AND
)

const (
	EQ CompOp = iota
	LT
	GT
	LT_E
	GT_E
)

type predicateExpression struct {
	left string
	compOp CompOp
	right string
}

type predicateExpressions struct {
	left *predicateExpression
	op Op
	right *predicateExpressions
}

type SelectionNode struct {
	predicate *predicateExpressions
	child *Iterator
}

func initSelectionNode(predicate *predicateExpressions, child Iterator) *SelectionNode {
	return &SelectionNode { predicate, &child}
}

func evaluatePredicate(p *predicateExpression, r *Record) bool {
	v, err := r.getColumn(p.left)
	if err != nil {
		panic(err)
	}
	switch p.compOp {
		case EQ:
			return v == p.right
		case LT:
			return v < p.right
		case GT:
			return v > p.right
		case LT_E:
			return v <= p.right
		case GT_E:
			return v >= p.right
	}
	return false
}

func evaluatePredicates(p *predicateExpressions, r *Record) bool {
	left := evaluatePredicate(p.left, r)
	var right bool
	if p.right == nil {
		right = true
	} else {
		right = evaluatePredicates(p.right, r)
	}
	if p.op == OR {
		return left || right
	}
	return left && right
}

func (p *SelectionNode) next() *Record {
	for r := (*p.child).next(); r != nil; r = (*p.child).next() {
		if (evaluatePredicates(p.predicate, r)) {
			return r
		}
	}
	return nil
}

func initPredicateExpression(left string, compOp CompOp, right string) *predicateExpression {
	return &predicateExpression{ left, compOp, right }
}

func initPredicateExpressions(left *predicateExpression, op Op, right *predicateExpressions) *predicateExpressions {
	return &predicateExpressions{left, op, right }
}

type ProjectionNode struct {
	cols []string
	child *Iterator
}

func initProjectionNode(cols []string, child Iterator) *ProjectionNode {
	return &ProjectionNode{ cols, &child }
}

func (p *ProjectionNode) next() *Record {
	n := (*p.child).next()
	if n == nil {
		return n
	}
	r := &Record {values: make(map[string]string, 0)}
	for _, col := range(p.cols) {
		v, ok := n.values[col]
		if ok {
			r.values[col] = v
		} else {
			panic(fmt.Sprintf("There is no %s in record %s", col, n))
		}
	}
	return r
}

type SortOrder int

const (
	ASC SortOrder = iota
	DESC
)

type SortPredicate func(*Record, *Record) int

type SortNode struct {
	sorted []*Record
	i uint32
	predicates []SortPredicate
	child *Iterator
	done bool
}

func generatePredicate(col string, order SortOrder) SortPredicate {
	sort_func := func(a, b *Record) int {
		a_v, _ := a.getColumn(col)
		b_v, _ := b.getColumn(col)
		return strings.Compare(a_v, b_v)
	}
	return sort_func
}


type SortTuple struct {
	col string
	order SortOrder
}

func initSortNode(child Iterator, sortTuples []SortTuple) *SortNode {
	records := make([]*Record, 0)
	predicates := make([]SortPredicate, 0)
	for _, v := range(sortTuples) {
		predicates = append(predicates, generatePredicate(v.col, v.order))
	}
	return &SortNode{ records, 0, predicates, &child, false }
}

func (s *SortNode) next() *Record {
	if s.done {
		i := s.i
		records := s.sorted
		if i >= uint32(len(records)) {
			return nil
		}
		s.i += 1
		return records[i]
	}
	for r := (*s.child).next(); r != nil; r = (*s.child).next() {
		s.sorted = append(s.sorted, r)
	}
	for _, predicates := range(s.predicates) {
		slices.SortFunc(s.sorted, predicates)
	}
	s.done = true
	i := s.i
	s.i += 1
	return s.sorted[i]
}

type CountNode struct {
	agg_output []Record
	i int
	child *Iterator
	cols []string
	done bool
}

func initCountNode(child Iterator, cols []string) *CountNode {
	return &CountNode{ make([]Record, 0), 0, &child, cols, false }
}

func (c *CountNode) next() *Record {
	if c.done {
		if c.i >= len(c.agg_output) {
			return nil
		}
		r := c.agg_output[c.i]
		c.i += 1
		return &r
	}
	count := make(map[string]int)
	for v := (*c.child).next(); v != nil; v = (*c.child).next() {
		c_key := ""
		for i, col := range(c.cols) {
			k, err := v.getColumn(col)
			if err != nil {
				panic(err)
			}
			if i > 0 {
				c_key += ","
			}
			c_key += fmt.Sprintf("%s:%s", col, k)
		}
		count[c_key] += 1
	}
	for k, v := range(count) {
		r := Record{ values: make(map[string]string) }
		parts := strings.Split(k, ",")
		for _, part := range(parts) {
			values := strings.Split(part, ":")
			if len(values) != 2 {
				panic(fmt.Sprintf("More than 2 entries for a single column, value pairt in %s", part))
			}
			r.values[values[0]] = values[1]
		}
		r.values["Count"] = fmt.Sprintf("%d", v)
		c.agg_output = append(c.agg_output, r)
	}
	c.done = true
	r := c.agg_output[c.i]
	c.i += 1
	return &r
}

/*
{
	head: {
		name: PROJECTION
		args: ["name"],
  PROJECTION: {
    args: ["name"],
		child: {
      SELECTION: {
				args: [["id", "equals", "5"]],
				child: {
					SCAN: {
						args: ["movies"]
						child: nil
					}
				}
			}
		}
	}
}

args: {
  "AND": {
	   "EQUALS": ["id", "5"],
		 "LT": []
	}
}

name: "SORT",
args: ["col:ASC", "col:DESC", ..."]

multiple

args: {
  OR: {
   AND: {
     EQUALS: ["id", "5"],
		 AND: {
       EQUALS: ["id", "2"], // implicit nil
		 }
	 },
	 AND: []
	}
}

id EQ 5 AND (id EQ 2 AND nil) OR (id EQ nil)

A JSON representation of the query where the first element represent
the argument and the right represent the node that needed to be the child
*/

type Tree struct {
	Head *Node `json:"head"`
}

type Node struct {
	Name string `json:"name"`
	Args interface{} `json:"args"`
	Child *Node `json:"child"`
}

func (t Tree) String() string {
	return t.Head.String()
}

func (n Node) String() string {
	if(n.Child == nil) {
		return "Node name: " + n.Name + fmt.Sprintf(", Args: %#v", n.Args)
	}
	return "Node name: " + n.Name + n.Child.String()
}

func generateTree(input string) *Tree {
	var t Tree
	err := json.Unmarshal([]byte(input), &t)
	if err != nil {
		fmt.Printf("Error")
	}
	return &t
}

type NodeConstructor func(p NodeParser, n *Node) Iterator

type Engine struct {
	Registry map[string]NodeConstructor
}

type NodeParser interface {
	Parse(n *Node) Iterator
}

func (e Engine) Parse(n *Node) Iterator {
	if n == nil { return nil }
	c, ok := e.Registry[n.Name]
	if !ok { return nil }
	return c(e, n)
}

func fileScanConstructor(p NodeParser, n *Node) Iterator {
	return initFileScanNode(parseFileScanNodeArgs(n.Args))
}

func scanNodeConstructor(p NodeParser, n *Node) Iterator {
	c := p.Parse(n.Child)
	return initScanNode(c)
}

func projectionNodeConstructor(p NodeParser, n *Node) Iterator {
	c := p.Parse(n.Child)
	return initProjectionNode(parseProjectionNodeArgs(n.Args), c)
}

func limitNodeConstructor(p NodeParser, n *Node) Iterator {
	c := p.Parse(n.Child)
	return initLimitNode(parseLimitNodeArg(n.Args), c)
}

func selectionNodeConstructor(p NodeParser, n *Node) Iterator {
	c := p.Parse(n.Child)
	return initSelectionNode(parseSelectionNodeArgs(n.Args), c)
}

func sortNodeConstructor(p NodeParser, n *Node) Iterator {
	c := p.Parse(n.Child)
	return initSortNode(c, parseSortNodeArgs(n.Args))
}

func countNodeConstructor(p NodeParser, n *Node) Iterator {
	c := p.Parse(n.Child)
	return initCountNode(c, parseCountNodeArgs(n.Args))
}

var Registry = map[string]NodeConstructor {
	"FILE_SCAN": fileScanConstructor,
	"SCAN": scanNodeConstructor,
	"PROJECTION": projectionNodeConstructor,
	"LIMIT": limitNodeConstructor,
	"SELECTION": selectionNodeConstructor,
	"SORT": sortNodeConstructor,
	"COUNT": countNodeConstructor,
}

func transformToQueryTree(input *Tree) Iterator {
	e := Engine{ Registry }
	return e.Parse(input.Head)
}

func parseSortNodeArgs(args interface{}) []SortTuple {
	tuples, ok := args.([]interface{})
	if !ok || len(tuples) == 0 {
		return []SortTuple{}
	}
	sort_tuples := make([]SortTuple, 0)
	for _, tuple := range(tuples) {
		t := tuple.(string)
		splits := strings.Split(t, ":")
		col := splits[0]
		var sort_order SortOrder
		if splits[1] == "ASC" {
			sort_order = ASC
		} else if splits[2] == "DESC" {
			sort_order = DESC
		}
		sort_tuples = append(sort_tuples, SortTuple{ col, sort_order })
	}
	return sort_tuples
}

func parseLimitNodeArg(args interface{}) uint32 {
	lim, ok := args.([]interface{})
	if !ok {
		return 0
	}
	if len(lim) > 1 {
		return 0
	}
	lim_s, ok := lim[0].(string)
	if !ok {
		return 0
	}
	i, err := strconv.Atoi(lim_s)
	if err != nil {
		return 0
	}
	return uint32(i)
}

func parseCountNodeArgs(args interface{}) []string {
	cols := make([]string, 0)
	arr, ok := args.([]interface{})
	if !ok {
		return nil
	}
	for _, v := range(arr) {
		s, s_ok := v.(string)
		if !s_ok {
			return nil
		}
		cols = append(cols, s)
	}
	return cols
}


func parseProjectionNodeArgs(args interface{}) []string {
	cols := make([]string, 0)
	arr, ok := args.([]interface{})
	if !ok {
		return nil
	}
	for _, v := range(arr) {
		s, s_ok := v.(string)
		if !s_ok {
			return nil
		}
		cols = append(cols, s)
	}
	return cols
}

func toStringSlice(a interface{}) []string {
	pa, _ := a.([]interface{})
	arr := make([]string, len(pa))
	for i, v := range(pa) {
		s, _ := v.(string)
		arr[i] = s
	}
	return arr
}

/*
EQUALS: ["id", "5"]
*/
func parsePredicate(v map[string]interface{}) *predicateExpression {
	eq_args, ok := v["EQ"]
	if ok {
		peq_args := toStringSlice(eq_args)
		return &predicateExpression{ peq_args[0], EQ, peq_args[1] }
	}

	lt_args, ok := v["LT"]
	if ok {
		plt_args := toStringSlice(lt_args)
		return &predicateExpression{ plt_args[0], LT, plt_args[1] }
	}

	gt_args, ok := v["GT"]
	if ok {
		pgt_args := toStringSlice(gt_args)
		return &predicateExpression{ pgt_args[0], GT, pgt_args[1] }
	}

	lteq_args, ok := v["LT_E"]
	if ok {
		plteq_args := toStringSlice(lteq_args)
		return &predicateExpression{ plteq_args[0], LT_E, plteq_args[1] }
	}

	gteq_args, ok := v["GT_E"]
	if ok {
		pgteq_args := toStringSlice(gteq_args)
		return &predicateExpression{ pgteq_args[0], GT_E, pgteq_args[1] }
	}

	return nil
}

func isPredicate(args interface{}) bool {
	arr := args.([]interface{})
	for _, v := range(arr) {
		_, ok := v.(string)
		if !ok {
			return false
		}
	}
	return true
}

/*
args: {
  "AND": {
	   "EQUALS": ["id", "5"],
		 "LT": []
	}
}

multiple

args: {
  OR: {
     EQUALS: ["id", "5"],
		 AND: {
       EQUALS: ["id", "5"],
			 OR: {
 					predicateExpr
			 }
		 }
	 },
}

*/
func parsePredicates(args map[string]interface{}) *predicateExpressions {
	or_args, or_ok := args["OR"]
	and_args, and_ok := args["AND"]
	var predicate map[string]interface{} 
	if or_ok {
		predicate, _ = or_args.(map[string]interface{})
	}
	if and_ok {
		predicate, _ = and_args.(map[string]interface{})
	}
	var op Op
	if or_ok {
		op = OR
	}
	if and_ok {
		op = AND
	}
	if !or_ok && !and_ok {
		panic(fmt.Sprintf("Failed to parse selection node for %v", args))
	}
	if (or_ok && len(predicate) == 1) || (and_ok && len(predicate) == 1) {
		return &predicateExpressions{ parsePredicate(predicate), op, nil }
	}
	return &predicateExpressions{ parsePredicate(predicate), op,
	parsePredicates(predicate) }
}

func parseSelectionNodeArgs(args interface{}) *predicateExpressions {
	margs, ok := args.(map[string]interface{})
	if !ok {
		return nil
	}
	return parsePredicates(margs)
}

func parseFileScanNodeArgs(args interface{}) *StorageReader {
	margs, ok := args.(map[string]interface{})
	if !ok {
		return nil
	}
	dir, ok := margs["dir"]
	if !ok {
		log.Fatal("Missing argument dir for filescan node")
	}
	file_number, ok := margs["file_number"]
	if !ok {
		log.Fatal("Missing argument file_number for filescan node")
	}
	asserted_dir, ok := dir.(string)
	if !ok {
		log.Fatal("Invalid argument dir for filescan node")
	}
	asserted_file_number, ok := file_number.(string)
	if !ok {
		log.Fatal("Invalid argument file_number for filescan node")
	}
	num, err  := strconv.Atoi(asserted_file_number)
	if err != nil {
		log.Fatal("Invalid argument file number for filescan node. Expect an int")
	}
	reader := initStorageReader(asserted_dir, num)
	return reader
}

func initFileScanNode(reader *StorageReader) Iterator {
	return &FileScan{ reader, 0 }
}

type FileScan struct {
	reader *StorageReader
	offset int64
}

func (r *FileScan) next() *Record {
	data, offset := (*r).reader.ReadRow(r.offset)
	if data == nil {
		return nil
	}
	(*r).offset = offset
	ret := &Record{}
	ret.key = data.row_key
	ret.values = make(map[string]string)
	for _, col := range(data.cols) {
		ret.values[col.name] = col.col
	}
	return ret
}
