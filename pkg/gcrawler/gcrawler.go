package gcrawler

import (
	"fmt"
	"net/url"
	"strings"
)

var blacklistedDomains = map[string]bool{
	"hellomouse.net":        true,
	"mirrors.apple2.org.za": true,
	"godocs.io":             true,
	"git.skyjake.fi":        true,
	"taz.de":                true,
	"localhost":             true,
	"127.0.0.1":             true,
	"guardian.shit.cx":      true,
	"mastogem.picasoft.net": true,
	"gemini.techrights.org": true,
	"gemini.autonomy.earth": true,
}

var blacklistedPrefixes = []string{
	"gemini://gemi.dev/cgi-bin/",
	"gemini://kennedy.gemi.dev/archive/",
	"gemini://kennedy.gemi.dev/search",
	"gemini://kennedy.gemi.dev/mentions",
	"gemini://kennedy.gemi.dev/cached",
	"gemini://caolan.uk/cgi-bin/weather.py/wxfcs",
	"gemini://illegaldrugs.net/cgi-bin/",
	"gemini://hoagie.space/proxy/",
	"gemini://tlgs.one/v/",
	"gemini://tlgs.one/search",
	"gemini://tlgs.one/backlinks",
	"gemini://tlgs.one/add_seed",
	"gemini://tlgs.one/backlinks",
	"gemini://tlgs.one/api",
	"gemini://geminispace.info/search",
	"gemini://geminispace.info/v/",
	"gemini://gemini.bunburya.eu/remini/",
	"gemini://gem.graypegg.com/hn/",

	// the index pages allow the "offset" query argument to go higher and higher
	// while the content wraps around to the beginning. what's worse, since the
	// index is part of the page contents, it's actually slightly different each
	// time. so we'll be crawling this site forever, adding almost-the-same
	// content over and over again.
	"gemini://gemlog.stargrave.org/?",
}

// since we frequently need both the parsed and non-parsed form of the url,
// we'll be passing this url around so we only need to parse once, and not have
// to reassemble the parsed url either.
type PreparedUrl struct {
	Parsed    *url.URL
	NonParsed string
}

func (u PreparedUrl) String() string {
	return u.NonParsed
}

var _ fmt.Stringer = (*PreparedUrl)(nil)

func IsBlacklisted(u PreparedUrl) bool {
	if _, ok := blacklistedDomains[u.Parsed.Hostname()]; ok {
		return true
	}

	for _, prefix := range blacklistedPrefixes {
		if strings.HasPrefix(u.String(), prefix) {
			return true
		}
	}

	return false
}

func AddDomainToBlacklist(domain string) {
	blacklistedDomains[domain] = true
}

func AddPrefixToBlacklist(prefix string) {
	blacklistedPrefixes = append(blacklistedPrefixes, prefix)
}
