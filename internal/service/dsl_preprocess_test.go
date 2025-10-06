package service

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

func TestConvertStandaloneDumpAndLogCalls(t *testing.T) {
	engine, err := newDSLEngine(config.DSLConfig{}, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("newDSLEngine: %v", err)
	}

	expression := `
let value_a = 1;
dump(value_a);
log("debug", value_a); // note: keep comment
value_a
`

	processed, err := engine.preprocess(expression)
	if err != nil {
		t.Fatalf("preprocess: %v", err)
	}

	if !strings.Contains(processed, "let __statement0 = dump(value_a);") {
		t.Fatalf("converted expression missing dump statement: %q", processed)
	}
	if !strings.Contains(processed, "let __statement1 = log(\"debug\", value_a);") {
		t.Fatalf("converted expression missing log statement: %q", processed)
	}

	if _, err := engine.compileExpression(expression); err != nil {
		t.Fatalf("compileExpression: %v", err)
	}
}

func TestConvertStandaloneCallIgnoresHttpUrls(t *testing.T) {
	line := "    dump(\"http://example.com\");"
	converted, ok := convertStandaloneCallLine(line, 0)
	if !ok {
		t.Fatalf("expected conversion to succeed")
	}
	if !strings.Contains(converted, "http://example.com") {
		t.Fatalf("expected URL to be preserved, got %q", converted)
	}
}

func TestRewriteValidationValueCalls(t *testing.T) {
	cases := map[string]string{
		`value("a")`:                      `value_fn("a")`,
		` value ( "b" ) `:                 ` value_fn ( "b" ) `,
		`value/*comment*/(\"c\")`:         `value/*comment*/(\"c\")`,
		`logger.value("d")`:               `logger.value("d")`,
		`valueX("e")`:                     `valueX("e")`,
		`"value(ignored)"`:                `"value(ignored)"`,
		`// value("comment")`:             `// value("comment")`,
		`value`:                           `value`,
		`value + value("f")`:              `value + value_fn("f")`,
		`let v = value("g"); log(value);`: `let v = value_fn("g"); log(value);`,
	}

	for input, want := range cases {
		got := rewriteValidationValueCalls(input)
		if got != want {
			t.Fatalf("rewriteValidationValueCalls(%q) = %q, want %q", input, got, want)
		}
	}
}
