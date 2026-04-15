package cli

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// parseSplitSize parses a positive byte size with optional binary unit suffix.
func parseSplitSize(v string) (int64, error) {
	value := strings.TrimSpace(v)
	if value == "" {
		return 0, fmt.Errorf("option --split-size requires a positive byte size")
	}

	index := 0
	for index < len(value) && value[index] >= '0' && value[index] <= '9' {
		index++
	}
	numberText := value[:index]
	unitText := strings.ToUpper(strings.TrimSpace(value[index:]))
	number, err := strconv.ParseInt(numberText, 10, 64)
	if err != nil || number <= 0 {
		return 0, fmt.Errorf("option --split-size requires a positive byte size")
	}

	multiplier, ok := splitSizeUnits[unitText]
	if !ok {
		return 0, fmt.Errorf("option --split-size requires a positive byte size")
	}
	if number > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("option --split-size value is too large")
	}
	return number * multiplier, nil
}

var splitSizeUnits = map[string]int64{
	"":    1,
	"B":   1,
	"K":   1024,
	"M":   1024 * 1024,
	"G":   1024 * 1024 * 1024,
	"T":   1024 * 1024 * 1024 * 1024,
	"KB":  1000,
	"MB":  1000 * 1000,
	"GB":  1000 * 1000 * 1000,
	"TB":  1000 * 1000 * 1000 * 1000,
	"KIB": 1024,
	"MIB": 1024 * 1024,
	"GIB": 1024 * 1024 * 1024,
	"TIB": 1024 * 1024 * 1024 * 1024,
}
