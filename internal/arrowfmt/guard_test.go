package arrowfmt

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// banned are the calls that turn an Arrow value into text on arrow-go's own
// layout. A release of arrow-go changed that layout once, and every sink that
// called one of these wrote a different string.
var banned = []string{
	".ValueStr(",
	"GetOneForMarshal(",
	"array.RecordToJSON(",
}

// bannedCalls returns the line number and text of each banned call in src.
func bannedCalls(src string) []string {
	var found []string
	for n, line := range strings.Split(src, "\n") {
		for _, call := range banned {
			if strings.Contains(line, call) {
				found = append(found, fmt.Sprintf("line %d: %s", n+1, strings.TrimSpace(line)))
				break
			}
		}
	}
	return found
}

func TestArrowfmtGuard_FindsEachBannedCall(t *testing.T) {
	src := "a := col.ValueStr(0)\nb := col.GetOneForMarshal(1)\nerr := array.RecordToJSON(rec, w)\nc := arrowfmt.Text(col, 0)\n"

	assert.Equal(t, 3, len(bannedCalls(src)))
	assert.Equal(t, "line 1: a := col.ValueStr(0)", bannedCalls(src)[0])
}

func TestArrowfmtGuard_NoDirectArrowTextOutsideThePackage(t *testing.T) {
	var violations []string
	for _, root := range []string{"..", filepath.Join("..", "..", "turbostats")} {
		if _, err := os.Stat(root); err != nil {
			continue
		}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == "arrowfmt" {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, v := range bannedCalls(string(src)) {
				violations = append(violations, path+" "+v)
			}
			return nil
		})
		assert.NoError(t, err)
	}
	if len(violations) > 0 {
		t.Fatalf("render Arrow values through internal/arrowfmt:\n%s", strings.Join(violations, "\n"))
	}
}
