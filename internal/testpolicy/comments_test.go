package testpolicy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Protects the repository-wide one-line behavior comment contract for test-like functions.
func TestBehaviorComments(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	set := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(set, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || !isTestLike(function.Name.Name) {
				continue
			}
			if function.Doc == nil || len(function.Doc.List) != 1 || !strings.HasPrefix(function.Doc.List[0].Text, "// Protects ") {
				t.Errorf("%s:%d: %s requires exactly one // Protects comment", path, set.Position(function.Pos()).Line, function.Name.Name)
				continue
			}
			if set.Position(function.Doc.End()).Line+1 != set.Position(function.Pos()).Line {
				t.Errorf("%s:%d: %s comment must be immediately above the function", path, set.Position(function.Pos()).Line, function.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func isTestLike(name string) bool {
	return strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Fuzz") || strings.HasPrefix(name, "Benchmark") || strings.HasPrefix(name, "Example")
}
