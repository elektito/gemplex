package gparse

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"git.sr.ht/~elektito/whatlanggo"
	"github.com/PuerkitoBio/purell"
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
	headingRe        = regexp.MustCompile("^(#+) *(?P<heading>.+) *$")
	linkRe           = regexp.MustCompile("^=> *(?P<linkurl>.*?)(?: +(?P<linktext>.+))? *$")
	preRe            = regexp.MustCompile("^``` *(?P<prealt>.*)? *$")
	rfcRe            = regexp.MustCompile(`(?s)Request for Comments: (?P<rfc>\d+)(?P<rest>.+)(?:Status of this Memo|Abstract)`)
	nonAlphanumSeqRe = regexp.MustCompile("(?m)[`\"~!@#$%\\^&*\\-_=+/|<>'()\\[\\]{},.;:\\\\ ]{5,}")
	spaceSeqRe       = regexp.MustCompile(`[ \t]{2,}`)
	newlineSeqRe     = regexp.MustCompile(`(?m)\n{2,}`)
	allWhitespaceRe  = regexp.MustCompile(`^\s+$`)
	ansiSeqRe        = regexp.MustCompile("[\u001B\u009B][[\\]()#;?]*(?:(?:(?:[a-zA-Z\\d]*(?:;[a-zA-Z\\d]*)*)?\u0007)|(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PRZcf-ntqry=><~]))") // from: https://github.com/acarl005/stripansi/blob/master/stripansi.go
)

func ParsePlain(text string) (result Page) {
	result.Text = text

	// if it's an email, parse it and use the subject line as title
	r := strings.NewReader(text)
	msg, err := mail.ReadMessage(r)
	if err == nil {
		result.Kind = "email"

		result.Title = msg.Header.Get("Subject")
		if result.Title != "" {
			ct := msg.Header.Get("Content-Type")

			// yes, I've seen upper case content-type headers! :)
			ct = strings.ToLower(ct)

			if ct != "" && !strings.HasPrefix(ct, "text/") && !strings.HasPrefix(ct, "multipart/") {
				result.Text = result.Title
			} else {
				body, err := io.ReadAll(msg.Body)
				if err == nil {
					result.Text = result.Title + "\n\n" + string(body)
				}
			}

			return
		}
	}

	// if it's an rfc, parse it and get the rfc title
	if len(text) > 1024 {
		result.Title = parseRfc(text[:1024])
		if result.Title != "" {
			result.Kind = "rfc"
			return
		}
	}

	lines := strings.Split(text, "\n")
	for _, line := range lines {
		result.Title = strings.TrimSpace(line)
		if result.Title != "" && isMostlyAlphanumeric(result.Title) {
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

		if len(line) > 0 && line[0] == '>' {
			line = line[1:]
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

			// a quick hacky fix for a mistake I've seen in some capsules.
			// clients usually handle //foo to mean the same thing as /foo, so
			// we do that too.
			if strings.HasPrefix(link.Url, "//") {
				link.Url = link.Url[1:]
			}

			u, err := url.Parse(link.Url)
			if err != nil {
				continue
			}
			u = base.ResolveReference(u)
			u, err = NormalizeUrl(u)
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
	result.Title = shortenTitleIfNeeded(result.Title)

	return
}

func ParsePage(body []byte, base *url.URL, contentType string) (result Page, err error) {
	text, err := convertToString(body, contentType)
	if err != nil {
		log.Printf("Error converting to string: url=%s content-type=%s: %s\n", base.String(), contentType, err)
		return
	}

	switch {
	case strings.HasPrefix(contentType, "text/plain"):
		result = ParsePlain(text)
	case strings.HasPrefix(contentType, "text/gemini"):
		fallthrough
	case strings.HasPrefix(contentType, "text/markdown"):
		result = ParseGemtext(text, base)
	default:
		err = fmt.Errorf("Cannot process text type: %s", contentType)
		return
	}

	// cleanup the text a little
	result.Text = ansiSeqRe.ReplaceAllLiteralString(result.Text, "")
	result.Text = nonAlphanumSeqRe.ReplaceAllLiteralString(result.Text, " ")
	result.Text = spaceSeqRe.ReplaceAllLiteralString(result.Text, " ")

	hadEllipses := strings.HasSuffix(result.Title, "...")
	result.Title = ansiSeqRe.ReplaceAllLiteralString(result.Title, "")
	result.Title = nonAlphanumSeqRe.ReplaceAllLiteralString(result.Title, " ")
	result.Title = spaceSeqRe.ReplaceAllLiteralString(result.Title, " ")
	result.Title = strings.Trim(result.Title, " \t")
	if hadEllipses && !strings.HasSuffix(result.Title, "...") {
		result.Title += "..."
	}

	// remove any whitespace only lines
	builder := strings.Builder{}
	for _, line := range strings.Split(result.Text, "\n") {
		if allWhitespaceRe.MatchString(line) {
			continue
		}
		builder.WriteString(line)
		builder.WriteRune('\n')
	}
	result.Text = builder.String()

	// remove consecutive newlines
	result.Text = newlineSeqRe.ReplaceAllLiteralString(result.Text, "\n")

	result.Title = strings.ToValidUTF8(result.Title, "")

	// detect text language
	result.Lang = detectLang(result.Text)

	return
}

func shortenTitleIfNeeded(title string) string {
	if len(title) <= maxTitleLength {
		return title
	}

	title = title[:maxTitleLength]

	if strings.HasSuffix(title, " ") {
		title = strings.TrimSpace(title)
	} else if idx := strings.LastIndex(title, " "); idx > 0 && idx > len(title)-10 {
		// the last word is likely incomplete, so we'll cut it.
		title = title[:idx]
	}

	title += "..."

	return title
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

func NormalizeUrl(u *url.URL) (outputUrl *url.URL, err error) {
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
