package service

import (
	"fmt"
	"strings"
	"unicode"
)

func convertIfBlocks(input string) (string, error) {
	var builder strings.Builder
	for idx := 0; idx < len(input); {
		ch := input[idx]
		switch ch {
		case '"', '\'', '`':
			end, err := scanStringLiteral(input, idx)
			if err != nil {
				return "", err
			}
			builder.WriteString(input[idx : end+1])
			idx = end + 1
			continue
		}
		if isIfKeyword(input, idx) {
			converted, next, err := parseIfBlock(input, idx)
			if err != nil {
				return "", err
			}
			builder.WriteString(converted)
			idx = next
			continue
		}
		builder.WriteByte(ch)
		idx++
	}
	return builder.String(), nil
}

func parseIfBlock(input string, start int) (string, int, error) {
	idx := skipWhitespace(input, start)
	if !strings.HasPrefix(input[idx:], "if") || !isWordBoundary(input, idx+2) {
		return "", start, fmt.Errorf("expected if keyword")
	}
	idx += 2
	idx = skipWhitespace(input, idx)
	condStart := idx
	braceIdx, err := findNextBrace(input, idx)
	if err != nil {
		return "", start, err
	}
	condition := strings.TrimSpace(input[condStart:braceIdx])
	if condition == "" {
		return "", start, fmt.Errorf("if condition must not be empty")
	}
	blockStart := braceIdx
	blockEnd, err := scanBalancedBraces(input, blockStart)
	if err != nil {
		return "", start, err
	}
	thenBody := strings.TrimSpace(input[blockStart+1 : blockEnd])
	next := skipWhitespace(input, blockEnd+1)
	if next >= len(input) || !strings.HasPrefix(input[next:], "else") || !isWordBoundary(input, next+4) {
		return "", start, fmt.Errorf("if expression must include an else block")
	}
	next += 4
	next = skipWhitespace(input, next)

	var elseExpr string
	switch {
	case strings.HasPrefix(input[next:], "if") && isWordBoundary(input, next+2):
		var elseConverted string
		var err error
		elseConverted, next, err = parseIfBlock(input, next)
		if err != nil {
			return "", start, err
		}
		elseExpr = elseConverted
	case next < len(input) && input[next] == '{':
		elseEnd, err := scanBalancedBraces(input, next)
		if err != nil {
			return "", start, err
		}
		elseBody := strings.TrimSpace(input[next+1 : elseEnd])
		convertedElse, err := convertIfBlocks(elseBody)
		if err != nil {
			return "", start, err
		}
		elseExpr = fmt.Sprintf("(%s)", convertedElse)
		next = elseEnd + 1
	default:
		return "", start, fmt.Errorf("else block must be followed by another if or a block")
	}

	convertedThen, err := convertIfBlocks(thenBody)
	if err != nil {
		return "", start, err
	}
	ternary := fmt.Sprintf("((%s) ? (%s) : %s)", condition, convertedThen, elseExpr)
	return ternary, next, nil
}

func scanBalancedBraces(input string, start int) (int, error) {
	if start >= len(input) || input[start] != '{' {
		return start, fmt.Errorf("expected '{'")
	}
	depth := 0
	for idx := start; idx < len(input); idx++ {
		ch := input[idx]
		switch ch {
		case '"', '\'', '`':
			end, err := scanStringLiteral(input, idx)
			if err != nil {
				return 0, err
			}
			idx = end
			continue
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return idx, nil
			}
		}
	}
	return 0, fmt.Errorf("unterminated block")
}

func scanStringLiteral(input string, start int) (int, error) {
	quote := rune(input[start])
	escaped := false
	for idx := start + 1; idx < len(input); idx++ {
		ch := rune(input[idx])
		if quote == '`' {
			if ch == quote {
				return idx, nil
			}
			continue
		}
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == quote {
			return idx, nil
		}
	}
	return 0, fmt.Errorf("unterminated string literal")
}

func findNextBrace(input string, start int) (int, error) {
	depth := 0
	for idx := start; idx < len(input); idx++ {
		ch := input[idx]
		switch ch {
		case '"', '\'', '`':
			end, err := scanStringLiteral(input, idx)
			if err != nil {
				return 0, err
			}
			idx = end
			continue
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '{':
			if depth == 0 {
				return idx, nil
			}
		}
	}
	return 0, fmt.Errorf("missing '{' after if condition")
}

func skipWhitespace(input string, start int) int {
	idx := start
	for idx < len(input) {
		if !unicode.IsSpace(rune(input[idx])) {
			break
		}
		idx++
	}
	return idx
}

func isWordBoundary(input string, idx int) bool {
	if idx < 0 || idx >= len(input) {
		return true
	}
	r := rune(input[idx])
	return !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_')
}

func isIfKeyword(input string, idx int) bool {
	if idx < 0 || idx+2 > len(input) {
		return false
	}
	if idx > 0 {
		prev := rune(input[idx-1])
		if unicode.IsLetter(prev) || unicode.IsDigit(prev) || prev == '_' {
			return false
		}
	}
	if strings.HasPrefix(input[idx:], "if") && isWordBoundary(input, idx+2) {
		return true
	}
	return false
}
