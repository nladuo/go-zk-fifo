//some algorism
package fifo

import (
	"log"
	"strconv"
	"strings"
)

func getMinIndex(strs []string, prefix string) int {
	index := 0
	min := 999999999
	for i := 0; i < len(strs); i++ {
		strVal := strings.TrimPrefix(strs[i], prefix)
		num, err := strconv.Atoi(strVal)
		if err != nil {
			log.Println("fifo.getMinIndex() , conversion err")
			panic(err)
		}
		if num < min {
			min = num
			index = i
		}

	}
	return index

}
