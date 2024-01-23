package entry

import (
	"sort"
)

func MapToArray(uniqueKVs map[string][]byte) []Pair[string, []byte] {
	// 2. Sorted key set
	var keys []string
	for k := range uniqueKVs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < (keys[j])
	})

	// 3. Map -> Array
	var result []Pair[string, []byte]
	for _, k := range keys {
		pair := Pair[string, []byte]{Key: k, Val: uniqueKVs[k]}
		result = append(result, pair)
	}
	return result
}
