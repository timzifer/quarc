package service

import (
	"fmt"
	"strings"
	"unicode"
)

// convertStandaloneCalls rewrites standalone dump/log statements into
// assignments so that the DSL parser treats them as regular expressions.
//
// Without this rewrite the parser would reject calls like `log(value)` when
// they appear on their own line because the grammar expects every statement
// to yield a value. Wrapping the call into a dummy assignment keeps side
// effects (logging) intact while satisfying the parser.
func convertStandaloneCalls(input string) string {
	if strings.TrimSpace(input) == "" {
		return input
	}
	var builder strings.Builder
	counter := 0
	lines := strings.Split(input, "\n")
	for idx, line := range lines {
		if idx > 0 {
			builder.WriteByte('\n')
		}
		converted, ok := convertStandaloneCallLine(line, counter)
		if ok {
			builder.WriteString(converted)
			counter++
			continue
		}
		builder.WriteString(line)
	}
	return builder.String()
}

// convertStandaloneCallLine attempts to rewrite a single line containing a
// standalone log/dump call. The function preserves indentation and trailing
// comments so the resulting statement keeps its original readability.
func convertStandaloneCallLine(line string, counter int) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return "", false
	}
	codePart, comment := splitCodeAndComment(line)
	codePart = strings.TrimSpace(codePart)
	if codePart == "" || !strings.HasSuffix(codePart, ";") {
		return "", false
	}
	call := strings.TrimSpace(codePart[:len(codePart)-1])
	name, normalized := parseStandaloneCall(call)
	if name == "" {
		return "", false
	}
	if name != "dump" && name != "log" {
		return "", false
	}
	indent := leadingWhitespace(line)
	suffix := ""
	if strings.TrimSpace(comment) != "" {
		suffix = " " + strings.TrimSpace(comment)
	}
	return fmt.Sprintf("%slet __statement%d = %s;%s", indent, counter, normalized, suffix), true
}

func parseStandaloneCall(input string) (string, string) {
	if input == "" {
		return "", ""
	}
	openIdx := strings.IndexRune(input, '(')
	if openIdx <= 0 {
		return "", ""
	}
	name := strings.TrimSpace(input[:openIdx])
	if !isValidIdentifier(name) {
		return "", ""
	}
	args := strings.TrimSpace(input[openIdx:])
	if !strings.HasSuffix(args, ")") {
		return "", ""
	}
	depth := 0
	for idx, r := range args {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && idx != len(args)-1 {
				return "", ""
			}
		}
		if depth < 0 {
			return "", ""
		}
	}
	if depth != 0 {
		return "", ""
	}
	return name, fmt.Sprintf("%s%s", name, args)
}

func splitCodeAndComment(line string) (string, string) {
	inSingle := false
	inDouble := false
	escaped := false
	for idx := 0; idx < len(line)-1; idx++ {
		ch := line[idx]
		if escaped {
			escaped = false
			continue
		}
		switch ch {
		case '\\':
			escaped = true
			continue
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '/':
			if !inSingle && !inDouble && line[idx+1] == '/' {
				return line[:idx], line[idx:]
			}
		}
	}
	return line, ""
}

func leadingWhitespace(input string) string {
	for idx, r := range input {
		if r == '\n' || r == '\r' {
			return input[:idx]
		}
		if !unicode.IsSpace(r) {
			return input[:idx]
		}
	}
	return input
}

// rewriteValidationValueCalls replaces occurrences of `value(...)` with
// `value_fn(...)` inside validation expressions. This avoids conflicts with
// the helper that exposes the computed result of the parent expression while
// leaving comments and string literals untouched.
func rewriteValidationValueCalls(input string) string {
	if strings.TrimSpace(input) == "" {
		return input
	}
	var builder strings.Builder
	lines := strings.Split(input, "\n")
	for idx, line := range lines {
		if idx > 0 {
			builder.WriteByte('\n')
		}
		code, comment := splitCodeAndComment(line)
		builder.WriteString(rewriteValueCallsInCode(code))
		builder.WriteString(comment)
	}
	return builder.String()
}

// rewriteValueCallsInCode walks through the provided code segment and replaces
// helper invocations while carefully tracking string/escape state so that
// embedded text remains unchanged.
func rewriteValueCallsInCode(code string) string {
	if code == "" {
		return code
	}
	var builder strings.Builder
	inSingle := false
	inDouble := false
	escaped := false
	for idx := 0; idx < len(code); {
		ch := code[idx]
		if escaped {
			builder.WriteByte(ch)
			escaped = false
			idx++
			continue
		}
		switch ch {
		case '\\':
			if inSingle || inDouble {
				escaped = true
			}
			builder.WriteByte(ch)
			idx++
			continue
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		}
		if !inSingle && !inDouble && strings.HasPrefix(code[idx:], "value") {
			if shouldRewriteValueCall(code, idx) {
				builder.WriteString("value_fn")
				idx += len("value")
				continue
			}
		}
		builder.WriteByte(ch)
		idx++
	}
	return builder.String()
}

// shouldRewriteValueCall verifies that a `value` identifier at the supplied
// index is a standalone helper invocation (instead of a property access or a
// different identifier) before triggering the rewrite.
func shouldRewriteValueCall(code string, idx int) bool {
	if idx > 0 {
		prev := code[idx-1]
		if isIdentifierRune(prev) || prev == '.' {
			return false
		}
	}
	end := idx + len("value")
	if end < len(code) {
		next := code[end]
		if isIdentifierRune(next) {
			return false
		}
	}
	// Skip whitespace between identifier and opening parenthesis.
	for end < len(code) {
		ch := code[end]
		if ch == '(' {
			return true
		}
		if !isSkippableWhitespace(ch) {
			return false
		}
		end++
	}
	return false
}

func isIdentifierRune(ch byte) bool {
	if ch >= 'a' && ch <= 'z' {
		return true
	}
	if ch >= 'A' && ch <= 'Z' {
		return true
	}
	if ch >= '0' && ch <= '9' {
		return true
	}
	return ch == '_'
}

func isSkippableWhitespace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\r', '\n', '\f', '\v':
		return true
	default:
		return false
	}
}
