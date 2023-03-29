package gparse

import (
	"net/url"
	"strings"
	"testing"
)

func TestParseEmail(t *testing.T) {
	text := `X-Foobar: 1000
Date: Mon, 23 Jun 2015 11:40:36 -0400
From: Gopher <from@example.com>
To: Another Gopher <to@example.com>
Subject: Spam & Eggs

Message body
`
	title, err := ParsePlain(text)
	expected := "Spam & Eggs"
	if err != nil || title != expected {
		t.Fatalf("ParsePlain(.): expected %q, <nil>; got %q, %v", expected, title, err)
	}
}

func TestParsePlain(t *testing.T) {
	text := `
subject matter

hello there!
`
	title, err := ParsePlain(text)
	expected := "subject matter"
	if err != nil || title != expected {
		t.Fatalf("ParsePlain(.): expected %q, <nil>; got %q, %v", expected, title, err)
	}
}

func TestParsePage(t *testing.T) {
	inputText := `
Hello!

# Important Stuff

something

anything

# Another important thing

=> /foo Foo
=> gemini://example.net/spam Spam & All
`
	contentType := "text/gemini"
	base, _ := url.Parse("gemini://example.org/abc/xyz")
	body := []byte(inputText)
	result, err := ParsePage(body, base, contentType)

	if err != nil {
		t.Fatal("ParsePage(.) returned an error:", err)
	}

	expectedText := `Hello!
Important Stuff
something
anything
Another important thing
Foo
Spam & All
`
	if result.Text != expectedText {
		t.Fatal("ParsePage(.), expected:\n", inputText, "\nGot:\n", result.Text)
	}

	expectedLinks := []Link{
		{
			Url:  "gemini://example.org/foo",
			Text: "Foo",
		},
		{
			Url:  "gemini://example.net/spam",
			Text: "Spam & All",
		},
	}
	if len(result.Links) != len(expectedLinks) {
		t.Fatalf("Did not get expected links; got %d instead of %d", len(result.Links), len(expectedLinks))
	}

	if result.Links[0] != expectedLinks[0] {
		t.Fatalf("Invalid link; got %s instead of %s", result.Links[0], expectedLinks[0])
	}

	if result.Links[1] != expectedLinks[1] {
		t.Fatalf("Invalid link; got %s instead of %s", result.Links[1], expectedLinks[1])
	}

	expectedTitle := "Important Stuff"
	if result.Title != expectedTitle {
		t.Fatalf("Expected title %s, got %s.", expectedTitle, result.Title)
	}
}

func TestParseGemtext(t *testing.T) {
	text := `
<PRE>cool ascii art
----
|  |
----
<PRE>

# H1
This doc is all about h1.
<PRE>
foobar
<PRE>

## References
some refs:
=> /refs Refs
=> https://example.com/foobar foobar
=> gemini://example.org/ref

## Conclusion
All in all very good.

=> /spam/eggs Spam & Eggs
`
	text = strings.Replace(text, "<PRE>", "```", -1)
	base, _ := url.Parse("gemini://example.net/base")
	gt := ParseGemtext(text, base)

	expectedHeadings := []Heading{
		{
			Level: 1,
			Text:  "H1",
		},
		{
			Level: 2,
			Text:  "References",
		},
		{
			Level: 2,
			Text:  "Conclusion",
		},
	}

	if len(gt.Headings) != len(expectedHeadings) {
		t.Fatalf("Expected %d headings; got %d.", len(expectedHeadings), len(gt.Headings))
	}

	for i := 0; i < len(expectedHeadings); i++ {
		if gt.Headings[i] != expectedHeadings[i] {
			t.Fatalf("Heading %d mismatch: expected=%v got=%v", i, expectedHeadings[i], gt.Headings[i])
		}
	}

	expectedLinks := []Link{
		{
			Url:  "gemini://example.net/refs",
			Text: "Refs",
		},
		{
			Url:  "gemini://example.org/ref",
			Text: "",
		},
		{
			Url:  "gemini://example.net/spam/eggs",
			Text: "Spam & Eggs",
		},
	}

	if len(gt.Links) != len(expectedLinks) {
		t.Fatalf("Expected %d links; got %d.", len(expectedLinks), len(gt.Links))
	}

	for i := 0; i < len(expectedLinks); i++ {
		if gt.Links[i] != expectedLinks[i] {
			t.Fatalf("Link %d mismatch: expected=%v got=%v", i, expectedLinks[i], gt.Links[i])
		}
	}

	expectedTitle := "H1"
	if gt.Title != expectedTitle {
		t.Fatalf("Expected title: %s; got %s.", expectedTitle, gt.Title)
	}

	expectedText := `cool ascii art
H1
This doc is all about h1.
foobar
References
some refs:
Refs
Conclusion
All in all very good.
Spam & Eggs
`
	if gt.Text != expectedText {
		t.Fatalf("Gemtext output text:\nexpected=%q\n     got=%q", expectedText, gt.Text)
	}
}

func TestParseGemtextSpaceStripping(t *testing.T) {
	// this is a regression test. we used to extract "gemini #spam" as the title
	// string due to excessive stripping of whitespaces.
	text := `## foobar
 #gemini #spam
hi
`
	u, _ := url.Parse("gemini://example.org")
	r := ParseGemtext(text, u)
	if r.Title != "foobar" {
		t.Fatalf("Expected title 'foobar'; got: %s", r.Title)
	}
}

func TestParseRfcTwoLineTitle(t *testing.T) {
	text := `Network Working Group                                         S. Deering
Request for Comments: 2460                                         Cisco
Obsoletes: 1883                                                R. Hinden
Category: Standards Track                                          Nokia
                                                           December 1998


                  Internet Protocol, Version 6 (IPv6)
                             Specification

Status of this Memo

   This document specifies an Internet standards track protocol for the
   Internet community, and requests discussion and suggestions for
   improvements.  Please refer to the current edition of the "Internet
   Official Protocol Standards" (STD 1) for the standardization state
   and status of this protocol.  Distribution of this memo is unlimited.

Copyright Notice

   Copyright (C) The Internet Society (1998).  All Rights Reserved.

Abstract

   This document specifies version 6 of the Internet Protocol (IPv6),
   also sometimes referred to as IP Next Generation or IPng.`

	title := parseRfc(text)
	expected := "RFC 2460 - Internet Protocol, Version 6 (IPv6) Specification"
	if title != expected {
		t.Fatalf("Expected RFC title '%s', got '%s'", expected, title)
	}
}

func TestParseRfcOneLineTitle(t *testing.T) {
	text := `Internet Engineering Task Force (IETF)                      W. Eddy, Ed.
STD: 7                                                       MTI Systems
Request for Comments: 9293                                   August 2022
Obsoletes: 793, 879, 2873, 6093, 6429, 6528,
           6691
Updates: 1011, 1122, 5961
Category: Standards Track
ISSN: 2070-1721


                  Transmission Control Protocol (TCP)

Abstract

   This document specifies the Transmission Control Protocol (TCP).  TCP
   is an important transport-layer protocol in the Internet protocol
   stack, and it has continuously evolved over decades of use and growth
   of the Internet.  Over this time, a number of changes have been made
   to TCP as it was specified in RFC 793, though these have only been
   documented in a piecemeal fashion.  This document collects and brings
   those changes together with the protocol specification from RFC 793.
   This document obsoletes RFC 793, as well as RFCs 879, 2873, 6093,
   6429, 6528, and 6691 that updated parts of RFC 793.  It updates RFCs
   1011 and 1122, and it should be considered as a replacement for the
   portions of those documents dealing with TCP requirements.  It also
   updates RFC 5961 by adding a small clarification in reset handling
   while in the SYN-RECEIVED state.  The TCP header control bits from
   RFC 793 have also been updated based on RFC 3168.

Status of This Memo

   This is an Internet Standards Track document.

   This document is a product of the Internet Engineering Task Force`

	title := parseRfc(text)
	expected := "RFC 9293 - Transmission Control Protocol (TCP)"
	if title != expected {
		t.Fatalf("Expected RFC title '%s', got '%s'", expected, title)
	}
}

func TestParseRfcNoMatch(t *testing.T) {
	text := `foobar`

	title := parseRfc(text)
	if title != "" {
		t.Fatalf("Expected non-matching text to return an empty title, instead got: %s", title)
	}
}
