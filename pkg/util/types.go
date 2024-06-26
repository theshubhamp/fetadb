package util

import (
	"fmt"
)

func ToString(val any) string {
	switch v := val.(type) {
	case string:
		return v
	}

	return fmt.Sprintf("%v", val)
}
