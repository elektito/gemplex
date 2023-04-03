package gparse

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"github.com/PuerkitoBio/purell"
	"github.com/retarus/whatlanggo"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/transform"
)

const (
	maxTitleLength = 72
)

type Link struct {
	Url  string
	Text string
}

type Heading struct {
	Level int
	Text  string
}

type Page struct {
	Text     string
	Links    []Link
	Headings []Heading
	Title    string
	Lang     string
	Kind     string
}

var (
	headingRe *regexp.Regexp
	linkRe    *regexp.Regexp
	preRe     *regexp.Regexp
	rfcRe     *regexp.Regexp
)

func init() {
	headingRe = regexp.MustCompile("^(#+) *(?P<heading>.+) *$")
	linkRe = regexp.MustCompile("^=> *(?P<linkurl>.*?)(?: +(?P<linktext>.+))? *$")
	preRe = regexp.MustCompile("^``` *(?P<prealt>.*)? *$")
	rfcRe = regexp.MustCompile(`(?s)Request for Comments: (?P<rfc>\d+)(?P<rest>.+)(?:Status of this Memo|Abstract)`)
}

func ParsePlain(text string) (title string, kind string, err error) {
	// if it's an email, parse it and use the subject line as title
	r := strings.NewReader(text)
	msg, err := mail.ReadMessage(r)
	if err == nil {
		kind = "email"

		title = msg.Header.Get("Subject")
		if title != "" {
			return
		}
	}

	// if it's an rfc, parse it and get the rfc title
	if len(text) > 1024 {
		title = parseRfc(text[:1024])
		if title != "" {
			kind = "rfc"
			return
		}
	}

	err = nil
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		title = strings.TrimSpace(line)
		if title != "" && isMostlyAlphanumeric(title) {
			break
		}
	}

	return
}

func ParseGemtext(text string, base *url.URL) (result Page) {
	var s strings.Builder

	firstLine := ""
	inPre := false
	preText := ""
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimRight(line, " ")

		matches := preRe.FindStringSubmatch(line)
		if len(matches) > 0 {
			if !inPre && matches[1] != "" {
				altText := matches[1]
				s.WriteString(altText + "\n")
			}
			if !inPre {
				preText = ""
			}
			if inPre {
				// we're trying not to index ascii art, but do index normal text
				// in a pre block
				if looksLikeText(preText) {
					s.WriteString(preText)
				}
			}
			inPre = !inPre
			continue
		}

		if inPre {
			if isMostlyAlphanumeric(line) {
				preText += line + "\n"
			}
			continue
		}

		matches = headingRe.FindStringSubmatch(line)
		if len(matches) > 0 {
			heading := Heading{
				Level: len(matches[1]),
				Text:  matches[2],
			}
			result.Headings = append(result.Headings, heading)
			s.WriteString(heading.Text + "\n")
			continue
		}

		matches = linkRe.FindStringSubmatch(line)
		if len(matches) > 0 {
			link := Link{
				Url:  matches[1],
				Text: matches[2],
			}

			u, err := url.Parse(link.Url)
			if err != nil {
				continue
			}
			u = base.ResolveReference(u)
			u, err = normalizeUrl(u)
			if err != nil {
				continue
			}
			if u.Scheme != "gemini" {
				continue
			}
			link.Url = u.String()

			result.Links = append(result.Links, link)

			if link.Text != "" {
				s.WriteString(link.Text + "\n")
			}

			continue
		}

		if line != "" {
			if firstLine == "" && isMostlyAlphanumeric(line) {
				firstLine = line
			}
			s.WriteString(line + "\n")
		}
	}

	result.Text = s.String()

	for _, heading := range result.Headings {
		result.Title = heading.Text
		if heading.Level == 1 && isMostlyAlphanumeric(heading.Text) {
			break
		}
	}

	if result.Title == "" {
		result.Title = firstLine
	}

	if result.Title == "" {
		for _, link := range result.Links {
			if isMostlyAlphanumeric(link.Text) {
				result.Title = link.Text
				break
			}
		}
	}

	result.Title = strings.TrimSpace(result.Title)

	if len(result.Title) > maxTitleLength {
		result.Title = result.Title[:maxTitleLength]

		if strings.HasSuffix(result.Title, " ") {
			result.Title = strings.TrimSpace(result.Title)
		} else if idx := strings.LastIndex(result.Title, " "); idx > 0 && idx > len(result.Title)-10 {
			// the last word is likely incomplete, so we'll cut it.
			result.Title = result.Title[:idx]
		}

		result.Title += "..."
	}

	result.Title = strings.ToValidUTF8(result.Title, "")

	return
}

func ParsePage(body []byte, base *url.URL, contentType string) (result Page, err error) {
	text, err := convertToString(body, contentType)
	if err != nil {
		fmt.Printf("Error converting to string: url=%s content-type=%s: %s\n", base.String(), contentType, err)
		return
	}

	switch {
	case strings.HasPrefix(contentType, "text/plain"):
		result.Text = text
		result.Title, result.Kind, err = ParsePlain(text)
		result.Lang = detectLang(text)
		return
	case strings.HasPrefix(contentType, "text/gemini"):
	case strings.HasPrefix(contentType, "text/markdown"):
	default:
		err = fmt.Errorf("Cannot process text type: %s", contentType)
		return
	}

	result = ParseGemtext(text, base)
	result.Lang = detectLang(result.Text)
	return
}

func detectLang(text string) string {
	info := whatlanggo.Detect(text)
	return info.Lang.Iso6391()
}

func parseRfc(text string) (title string) {
	m := rfcRe.FindStringSubmatch(text)
	if m == nil {
		return
	}

	rfcNum := m[1]
	rest := m[2]

	lines := strings.Split(rest, "\n")
	started := false
	for _, line := range lines {
		if !started {
			if line == "" {
				started = true
			}
			continue
		}

		if line == "" && title == "" {
			continue
		}

		if line == "" {
			break
		}

		if title == "" {
			title = strings.TrimSpace(line)
		} else {
			title += " " + strings.TrimSpace(line)
		}
	}

	title = "RFC " + rfcNum + " - " + title
	return
}

func looksLikeText(s string) bool {
	if !isMostlyAlphanumeric(s) {
		return false
	}

	words := strings.Fields(s)
	if len(words) == 0 {
		return false
	}

	maxLen := 0
	sum := 0
	for _, w := range words {
		if len(w) > maxLen {
			maxLen = len(w)
		}
		sum += len(w)
	}

	avg := float64(sum) / float64(len(words))
	if avg > 7 {
		return false
	}

	return true
}

func isMostlyAlphanumeric(s string) bool {
	if s == "" {
		return false
	}

	n := 0
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			n += 1
		}
	}

	return float64(n)/float64(len(s)) > 0.6
}

func convertToString(body []byte, contentType string) (s string, err error) {
	encoding, _, _ := charset.DetermineEncoding(body, contentType)

	reader := transform.NewReader(bytes.NewBuffer(body), encoding.NewEncoder())
	docBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("Error converting text encoding: %w", err)
		return
	}

	s = string(docBytes)

	// postgres doesn't like null character in strings, even though it's valid
	// utf-8.
	s = strings.ReplaceAll(s, "\x00", "")

	s = strings.ToValidUTF8(s, "")

	return
}

func normalizeUrl(u *url.URL) (outputUrl *url.URL, err error) {
	// remove default gemini port, since purell only supports doing this with
	// http and https.
	if u.Scheme == "gemini" && u.Port() == "1965" {
		u.Host = strings.ReplaceAll(u.Host, ":1965", "")
	}

	flags := purell.FlagLowercaseScheme |
		purell.FlagLowercaseHost |
		purell.FlagUppercaseEscapes |
		purell.FlagDecodeUnnecessaryEscapes |
		purell.FlagEncodeNecessaryEscapes |
		purell.FlagRemoveEmptyQuerySeparator |
		purell.FlagRemoveDotSegments |
		purell.FlagRemoveDuplicateSlashes |
		purell.FlagRemoveEmptyPortSeparator |
		purell.FlagRemoveUnnecessaryHostDots
	urlStr := purell.NormalizeURL(u, flags)

	outputUrl, err = url.Parse(urlStr)

	// make sure the root pages have a single slash as path (this seems more
	// frequently seen in the wild, and so there's less chance we'll have to
	// follow redirects from one to the other).
	if outputUrl.Path == "" {
		outputUrl.Path = "/"
	}

	return
}
