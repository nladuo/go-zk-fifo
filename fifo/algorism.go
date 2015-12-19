package fifo

import (
	"fmt"
	"strconv"
	"strings"
)

func GetMinIndex(strs []string, prefix string) int {
	index := 0
	min := 2147483648
	for i := 0; i < len(children); i++ {
		strVal := strings.TrimPrefix(strs[i], prefix)
		num, err := strconv.Atoi(strVal)
		if err != nil {
			fmt.Println("get min index , conversion err")
			panic(err)
		}
		if num < min {
			min = num
			index = i
		}

	}
	return index

}
