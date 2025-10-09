package service

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/timzifer/modbus_processor/internal/config"
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

func TestConvertStandaloneCallLinePreservesFormatting(t *testing.T) {
	line := "\tlog(\"info\"); // trailing"
	converted, ok := convertStandaloneCallLine(line, 3)
	if !ok {
		t.Fatalf("expected conversion to succeed")
	}
	expected := "\tlet __statement3 = log(\"info\"); // trailing"
	if converted != expected {
		t.Fatalf("unexpected conversion result: %q", converted)
	}
}

func TestConvertStandaloneCallLineRejectsInvalidInputs(t *testing.T) {
	cases := []string{
		"log(1)",                     // missing semicolon
		"value := log(1);",           // assignment should be ignored
		"// log(1);",                 // comment only
		"log(1); extra",              // extra tokens
		"log(1) // no semicolon",     // no semicolon before comment
		"log(1); /* block comment*/", // block comment not handled
	}
	for _, line := range cases {
		if converted, ok := convertStandaloneCallLine(line, 0); ok {
			t.Fatalf("expected %q to stay untouched, got %q", line, converted)
		}
	}
}

func TestParseStandaloneCall(t *testing.T) {
	if name, normalized := parseStandaloneCall("log(\"x\")"); name != "log" || normalized != "log(\"x\")" {
		t.Fatalf("unexpected parse result: %q %q", name, normalized)
	}

	negatives := []string{"", "value", "value(", "value)"}
	for _, input := range negatives {
		if name, normalized := parseStandaloneCall(input); name != "" || normalized != "" {
			t.Fatalf("expected empty result for %q", input)
		}
	}
}

func TestSplitCodeAndComment(t *testing.T) {
	code, comment := splitCodeAndComment("value(\"a//b\") // comment")
	if code != "value(\"a//b\") " {
		t.Fatalf("unexpected code part: %q", code)
	}
	if comment != "// comment" {
		t.Fatalf("unexpected comment part: %q", comment)
	}
}

func TestLeadingWhitespace(t *testing.T) {
	if got := leadingWhitespace("    value"); got != "    " {
		t.Fatalf("unexpected leading whitespace: %q", got)
	}
	if got := leadingWhitespace("\t\tvalue"); got != "\t\t" {
		t.Fatalf("unexpected tab whitespace: %q", got)
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

func TestRewriteValueCallsInCodeIgnoresStringsAndProperties(t *testing.T) {
	input := `value("a") + logger.value("b") + "value(should_stay)"`
	got := rewriteValueCallsInCode(input)
	want := `value_fn("a") + logger.value("b") + "value(should_stay)"`
	if got != want {
		t.Fatalf("rewriteValueCallsInCode = %q, want %q", got, want)
	}
}

func TestShouldRewriteValueCall(t *testing.T) {
	cases := map[string]bool{
		"value(":        true,
		" value (":      true,
		"logger.value(": false,
		"value_extra(":  false,
	}
	for input, want := range cases {
		idx := strings.Index(input, "value")
		if idx < 0 {
			t.Fatalf("test case %q missing 'value'", input)
		}
		if got := shouldRewriteValueCall(input, idx); got != want {
			t.Fatalf("shouldRewriteValueCall(%q) = %v, want %v", input, got, want)
		}
	}
}
