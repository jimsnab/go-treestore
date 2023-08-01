package treestore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func bytesToFloat(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func floatToBytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func (node *AvlNode[T]) isFloat(float float64) bool {
	if node == nil {
		return false
	}
	return bytes.Equal(node.key, floatToBytes(float))
}

func checkTree[T float64 | int](tree *AvlTree[T]) bool {
	if !tree.isValid() {
		return false
	}

	count := 0
	tree.Iterate(func(node *AvlNode[T]) bool {
		key := T(bytesToFloat(node.key))
		if key != node.value {
			return false
		}
		count++
		return true
	})

	return count == tree.nodes
}

func TestAvlInsertLL(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Set(floatToBytes(20), 20)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Set(floatToBytes(10), 10)
	tree.printTreeBalance("-----------------")
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlInsertLR(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Set(floatToBytes(10), 10)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Set(floatToBytes(20), 20)
	tree.printTreeBalance("-----------------")
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlInsertRL(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(10), 10)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Set(floatToBytes(30), 30)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Set(floatToBytes(20), 20)
	tree.printTreeBalance("-----------------")
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlInsertRR(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(10), 10)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Set(floatToBytes(20), 20)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Set(floatToBytes(30), 30)
	tree.printTreeBalance("-----------------")
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlMultiLevel(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(2461), 2461)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(1902), 1902)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(2657), 2657)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(7812), 7812)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(4865), 4865)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(7999), 7999)

	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlMultiLevel2(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(686), 686)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(959), 959)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(1522), 1522)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(7275), 7275)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(7537), 7537)
	tree.printTreeBalance("-----------------")
	tree.Set(floatToBytes(5749), 5749)

	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlMultiLevel3(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7150), 7150)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(6606), 6606)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2879), 2879)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(6229), 6229)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5222), 5222)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7150), 7150)

	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlMultiLevel4(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5499), 5499)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7982), 7982)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7434), 7434)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2050), 2050)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2142), 2142)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(6523), 6523)

	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlMultiLevel5(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2249), 2249)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5158), 5158)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(6160), 6160)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(4987), 4987)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(896), 896)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(658), 658)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7425), 7425)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7866), 7866)

	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}
}

func TestAvlDeleteRoot(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Delete(floatToBytes(30))
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	if tree.root != nil {
		t.Fatal("not deleted")
	}
}

func TestAvlDeleteLeft(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	tree.Set(floatToBytes(20), 20)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Delete(floatToBytes(20))
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	if tree.countEach() != 1 {
		t.Fatal("not deleted")
	}
}

func TestAvlDeleteRight(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	tree.Set(floatToBytes(40), 40)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Delete(floatToBytes(40))
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	if tree.countEach() != 1 {
		t.Fatal("not deleted")
	}
}

func TestAvlDeletePromoteLeft(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	tree.Set(floatToBytes(20), 20)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Delete(floatToBytes(30))
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	if tree.countEach() != 1 {
		t.Fatal("not deleted")
	}

	if !bytes.Equal(tree.root.key, floatToBytes(20)) {
		t.Fatal("unexpected root key")
	}
}

func TestAvlDeletePromoteRight(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	tree.Set(floatToBytes(40), 40)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Delete(floatToBytes(30))
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	if tree.countEach() != 1 {
		t.Fatal("not deleted")
	}

	if !bytes.Equal(tree.root.key, floatToBytes(40)) {
		t.Fatal("unexpected root key")
	}
}

func TestAvlDeletePromoteLeftFull(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	tree.Set(floatToBytes(20), 20)
	tree.Set(floatToBytes(40), 40)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	tree.Delete(floatToBytes(30))
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	if tree.countEach() != 2 {
		t.Fatal("not deleted")
	}

	if !bytes.Equal(tree.root.key, floatToBytes(20)) {
		t.Fatal("unexpected root key")
	}
}

func TestAvlInsertDelete5(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2460), 2460)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7435), 7435)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2460), 2460)

	if !checkTree(tree) {
		t.Fatal("tree invalid")
	}

	tree.printTreeBalance("------------")
	tree.Delete(floatToBytes(-2460))

	if !checkTree(tree) {
		t.Fatal("tree invalid")
	}

	tree.printTreeBalance("------------")
	tree.Delete(floatToBytes(2460))

	if !checkTree(tree) {
		t.Fatal("tree invalid")
	}
}

func TestAvlInsertDelete6(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7472), 7472)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2576), 2576)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2813), 2813)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5622), 5622)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7109), 7109)
	tree.printTreeBalance("------------")
	tree.Delete(floatToBytes(2576))
	tree.printTreeBalance("------------")

	if !checkTree(tree) {
		t.Fatal("tree invalid")
	}
}

func TestAvlInsertDelete22(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(743), 743)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(6999), 6999)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7700), 7700)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5829), 5829)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5898), 5898)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7508), 7508)
	tree.printTreeBalance("------------")
	tree.Delete(floatToBytes(5898))
	tree.printTreeBalance("------------")
	tree.Delete(floatToBytes(6999))
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5096), 5096)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5766), 5766)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(7801), 7801)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(5557), 5557)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(6492), 6492)
	tree.printTreeBalance("------------")
	tree.Delete(floatToBytes(5766))
	tree.printTreeBalance("------------")
	tree.Delete(floatToBytes(743))
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(4230), 4230)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2066), 2066)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(1668), 1668)
	tree.printTreeBalance("------------")
	tree.Delete(floatToBytes(5829))
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(3929), 3929)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2455), 2455)
	tree.printTreeBalance("------------")
	tree.Set(floatToBytes(2580), 2580)
	tree.printTreeBalance("------------")

	if !checkTree(tree) {
		t.Fatal("tree invalid")
	}

}

func testInsertDelete(worst int) (out []int) {
	history := make([]int, 0, 1024)
	historyPtr := &history

	defer func() {
		if r := recover(); r != nil {
			out = *historyPtr
		}
	}()

	tree := newAvlTree[int]()
	numbers := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		op := rand.Intn(4)
		var v int
		if op == 0 && len(numbers) > 0 {
			n := rand.Intn(len(numbers))
			v = -numbers[n]
			if n+1 < len(numbers) {
				numbers = append(numbers[0:n], numbers[n+1:]...)
			} else {
				numbers = numbers[0:n]
			}
		} else {
			v = rand.Intn(8192) + 1
		}

		*historyPtr = append(*historyPtr, v)
		if v > 0 {
			numbers = append(numbers, v)
			tree.Set(floatToBytes(float64(v)), v)
		} else {
			tree.Delete(floatToBytes(float64(-v)))
		}
		if !checkTree(tree) {
			if worst == 0 || len(*historyPtr) < worst {
				out = *historyPtr
			}
			break
		}
	}

	return
}

func TestAvlInsertDeleteRandom(t *testing.T) {
	var worst []int

	// set the number of passes to a high number and run with a longer timeout
	// for a more complete stress test

	for pass := 0; pass < 100; pass++ {
		worst = testInsertDelete(len(worst))
		if len(worst) > 0 {
			break
		}
	}

	if worst != nil {
		tree := newAvlTree[float64]()
		for _, v := range worst {
			fmt.Println("tree.printTreeBalance(\"------------\")")
			if v > 0 {
				fmt.Printf("tree.Set(floatToBytes(%v), %v)\n", v, v)
			} else {
				fmt.Printf("tree.Delete(%v)\n", -v)
			}
			tree.Set(floatToBytes(float64(v)), float64(v))
		}
		tree.printTreeBalance("------imbalanced------")
		fmt.Printf("%d steps\n", len(worst))
		t.Fatal("tree invalid")
	}
}

func TestAvlFindOne(t *testing.T) {
	tree := newAvlTree[float64]()

	tree.Set(floatToBytes(30), 30)
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	match := tree.Find(floatToBytes(30))
	if match == nil {
		t.Fatal("not found")
	}

	match = tree.Find(floatToBytes(29))
	if match != nil {
		t.Fatal("found")
	}

	match = tree.Find(floatToBytes(31))
	if match != nil {
		t.Fatal("found")
	}
}

func TestAvlFindStress(t *testing.T) {
	formatter := func(node *AvlNode[float64]) string { return fmt.Sprintf("%v", bytesToFloat(node.key)) }
	tree := newAvlTree[float64]()

	numbers := make([]int, 0, 2500)

	for i := 0; i < 25; i++ {
		n := rand.Intn(20000)
		_, added := tree.SetKey(floatToBytes(float64(n)))
		if added {
			numbers = append(numbers, n)
		}
	}

	sort.Ints(numbers)

	for index, n := range numbers {
		match := tree.Find(floatToBytes(float64(n)))
		if match == nil {
			tree.printTree(fmt.Sprintf("can't find %v:::", n), formatter)
			t.Fatal("no match")
		}

		if index > 0 && numbers[index-1] != n-1 {
			match := tree.Find(floatToBytes(float64(n - 1)))
			if match != nil {
				tree.printTree(fmt.Sprintf("shouldn't find -1 %v:::", n-1), formatter)
				t.Fatal("left match")
			}
		}

		if index < len(numbers)-1 && numbers[index+1] != n+1 {
			match := tree.Find(floatToBytes(float64(n + 1)))
			if match != nil {
				tree.printTree(fmt.Sprintf("shouldn't find +1 %v:::", n+1), formatter)
				t.Fatal("right match")
			}
		}
	}
}

func TestAvlSetKey(t *testing.T) {
	tree := newAvlTree[float64]()

	key, added := tree.SetKey(floatToBytes(30))
	if !added {
		t.Fatal("key added")
	}
	key.value = 30
	if !checkTree(tree) {
		tree.printTreeBalance("-----------------")
		t.Fatal("tree invalid")
	}

	key, added = tree.SetKey(floatToBytes(30))
	if added {
		t.Fatal("key not added")
	}
	if key.value != 30 {
		t.Fatal("wrong value")
	}
}

func TestAvlFindLeft(t *testing.T) {
	tree := newAvlTree[float64]()

	match := tree.FindLeft(floatToBytes(30))
	if match != nil {
		t.Fatal("empty tree")
	}

	tree.SetKey(floatToBytes(30))

	match = tree.FindLeft(floatToBytes(30))
	if match == nil {
		t.Fatal("no exact match")
	}

	match = tree.FindLeft(floatToBytes(31))
	if match == nil {
		t.Fatal("31 return 30")
	}

	match = tree.FindLeft(floatToBytes(29))
	if match != nil {
		t.Fatal("29 return nil")
	}

	tree.SetKey(floatToBytes(20))
	match = tree.FindLeft(floatToBytes(29))
	if !match.isFloat(20) {
		t.Fatal("29 return 20")
	}

	match = tree.FindLeft(floatToBytes(19))
	if match != nil {
		t.Fatal("19 return nil")
	}

	tree.SetKey(floatToBytes(40))
	match = tree.FindLeft(floatToBytes(39))
	if !match.isFloat(30) {
		t.Fatal("39 return 30")
	}

	match = tree.FindLeft(floatToBytes(41))
	if !match.isFloat(40) {
		t.Fatal("41 return 40")
	}
}

func TestAvlFindLeftStress(t *testing.T) {
	formatter := func(node *AvlNode[float64]) string { return fmt.Sprintf("%v", bytesToFloat(node.key)) }

	for pass := 0; pass < 2500; pass++ {
		tree := newAvlTree[float64]()
		limit := rand.Intn(130)
		numbers := make([]int, 0, limit)

		for i := 0; i < limit; i++ {
			n := rand.Intn(20000) + 1
			_, added := tree.SetKey(floatToBytes(float64(n)))
			if added {
				numbers = append(numbers, n)
			}
		}

		sort.Ints(numbers)

		for index, n := range numbers {
			match := tree.FindLeft(floatToBytes(float64(n - 1)))
			if index == 0 {
				if match != nil {
					tree.printTree(fmt.Sprintf("first number %v match not nil:::", n-1), formatter)
					t.Fatal("lowest number not nil")
				}
			} else {
				expected := numbers[index-1]
				if !match.isFloat(float64(expected)) {
					if match == nil {
						tree.printTree(fmt.Sprintf("searching for %v, expecting %v, got nil:::", n-1, expected), formatter)
						t.Fatal("exact match unexpected")
					} else if expected != n-1 {
						tree.printTree(fmt.Sprintf("searching for %v, expecting %v, got %v:::", n-1, expected, bytesToFloat(match.key)), formatter)
						t.Fatal("exact match unexpected")
					}
				}
			}
		}
	}
}

func TestAvlFindRight(t *testing.T) {
	tree := newAvlTree[float64]()

	match := tree.FindRight(floatToBytes(30))
	if match != nil {
		t.Fatal("empty tree")
	}

	tree.SetKey(floatToBytes(30))

	match = tree.FindRight(floatToBytes(30))
	if match == nil {
		t.Fatal("no exact match")
	}

	match = tree.FindRight(floatToBytes(29))
	if match == nil {
		t.Fatal("29 return 30")
	}

	match = tree.FindRight(floatToBytes(31))
	if match != nil {
		t.Fatal("31 return nil")
	}

	tree.SetKey(floatToBytes(40))
	match = tree.FindRight(floatToBytes(39))
	if !match.isFloat(40) {
		t.Fatal("39 return 40")
	}

	match = tree.FindRight(floatToBytes(41))
	if match != nil {
		t.Fatal("41 return nil")
	}

	tree.SetKey(floatToBytes(20))
	match = tree.FindRight(floatToBytes(21))
	if !match.isFloat(30) {
		t.Fatal("21 return 30")
	}

	match = tree.FindRight(floatToBytes(19))
	if !match.isFloat(20) {
		t.Fatal("19 return 20")
	}
}

func TestAvlFindRightStress(t *testing.T) {
	formatter := func(node *AvlNode[float64]) string { return fmt.Sprintf("%v", bytesToFloat(node.key)) }

	for pass := 0; pass < 2500; pass++ {
		tree := newAvlTree[float64]()
		limit := rand.Intn(130)
		numbers := make([]int, 0, limit)

		for i := 0; i < limit; i++ {
			n := rand.Intn(20000) + 1
			_, added := tree.SetKey(floatToBytes(float64(n)))
			if added {
				numbers = append(numbers, n)
			}
		}

		sort.Ints(numbers)

		for index, n := range numbers {
			match := tree.FindRight(floatToBytes(float64(n - 1)))

			expected := numbers[index]
			if index > 0 && numbers[index-1] == n-1 {
				expected = n - 1
			}

			if !match.isFloat(float64(expected)) {
				if match == nil {
					tree.printTree(fmt.Sprintf("searching for %v, expecting %v, got nil:::", n-1, expected), formatter)
					t.Fatal("exact match unexpected")
				} else {
					tree.printTree(fmt.Sprintf("searching for %v, expecting %v, got %v:::", n-1, expected, bytesToFloat(match.key)), formatter)
					t.Fatal("exact match unexpected")
				}
			}

			match = tree.FindRight(floatToBytes(float64(n + 1)))
			if index == (len(numbers) - 1) {
				if match != nil {
					tree.printTree(fmt.Sprintf("last number %v match not nil:::", n+1), formatter)
					t.Fatal("highest number not nil")
				}
			} else {
				expected = numbers[index+1]

				if !match.isFloat(float64(expected)) {
					if match == nil {
						tree.printTree(fmt.Sprintf("searching for %v, expecting %v, got nil:::", n+1, expected), formatter)
						t.Fatal("exact match unexpected")
					} else {
						tree.printTree(fmt.Sprintf("searching for %v, expecting %v, got %v:::", n+1, expected, bytesToFloat(match.key)), formatter)
						t.Fatal("exact match unexpected")
					}
				}
			}
		}
	}
}
