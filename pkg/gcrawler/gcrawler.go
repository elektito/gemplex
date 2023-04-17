package gcrawler

import (
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
}

func IsBlacklisted(link string, parsedLink *url.URL) bool {
	if _, ok := blacklistedDomains[parsedLink.Hostname()]; ok {
		return true
	}

	for _, prefix := range blacklistedPrefixes {
		if strings.HasPrefix(link, prefix) {
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
