package service

import "testing"

func TestConvertIfBlocks(t *testing.T) {
	input := `if value("a") > 10 {
                success(1)
        } else if value("a") < 0 {
                fail("logic.range", "negative")
        } else {
                success(2)
        }`

	converted, err := convertIfBlocks(input)
	if err != nil {
		t.Fatalf("convertIfBlocks: %v", err)
	}
	expected := `((value("a") > 10) ? (success(1)) : ((value("a") < 0) ? (fail("logic.range", "negative")) : (success(2))))`
	if converted != expected {
		t.Fatalf("converted expression mismatch:\n got: %s\nwant: %s", converted, expected)
	}
}

func TestConvertIfBlocksStringLiterals(t *testing.T) {
	input := `if value("mode") == "if" {
                success("if { braces }")
        } else {
                success("ok")
        }`
	converted, err := convertIfBlocks(input)
	if err != nil {
		t.Fatalf("convertIfBlocks: %v", err)
	}
	expected := `((value("mode") == "if") ? (success("if { braces }")) : (success("ok")))`
	if converted != expected {
		t.Fatalf("converted expression mismatch:\n got: %s\nwant: %s", converted, expected)
	}
}

func TestConvertIfBlocksIgnoresComments(t *testing.T) {
	input := `if enabled {
                // if this comment mentions if it should be ignored
                success(1)
        } else {
                success(2) # else if comment at end
        }`
	converted, err := convertIfBlocks(input)
	if err != nil {
		t.Fatalf("convertIfBlocks: %v", err)
	}
	expected := `((enabled) ? (// if this comment mentions if it should be ignored
                success(1)) : (success(2) # else if comment at end))`
	if converted != expected {
		t.Fatalf("converted expression mismatch:\n got: %s\nwant: %s", converted, expected)
	}
}

func TestConvertIfBlocksAllowsCommentBetweenBlocks(t *testing.T) {
	input := `if condition {
                success(1)
        } /* if in block comment */ else {
                success(0)
        }`
	converted, err := convertIfBlocks(input)
	if err != nil {
		t.Fatalf("convertIfBlocks: %v", err)
	}
	expected := `((condition) ? (success(1)) : (success(0)))`
	if converted != expected {
		t.Fatalf("converted expression mismatch:\n got: %s\nwant: %s", converted, expected)
	}
}
