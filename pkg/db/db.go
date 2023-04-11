package db

import (
	"database/sql"
	"net/url"

	"github.com/elektito/gemplex/pkg/config"
	"github.com/elektito/gemplex/pkg/gparse"
)

type UrlInfo struct {
	Url               string
	UrlId             int64
	UrlRank           float64
	HostRank          float64
	ContentId         int64
	ContentTitle      string
	Contents          []byte
	ContentsText      string
	ContentType       string
	ContentTypeArgs   string
	ContentLang       string
	ContentKind       string
	InternalLinks     []gparse.Link
	ExternalLinks     []gparse.Link
	InternalBacklinks []gparse.Link
	ExternalBacklinks []gparse.Link
}

func QueryUrl(urlStr string, substr bool) (info UrlInfo, err error) {
	db, err := sql.Open("postgres", config.GetDbConnStr())
	if err != nil {
		return
	}
	defer db.Close()

	var whereClause string
	if substr {
		whereClause = "u.url like '%' || $1 || '%'"
	} else {
		whereClause = "u.url = $1"
	}

	q := `
select u.url, u.id, u.rank, h.rank, c.id, c.title, c.content_type, c.content_type_args, c.content, c.content_text, c.lang, c.kind
from urls u
join hosts h on h.hostname = u.hostname
join contents c on u.content_id = c.id
where ` + whereClause

	row := db.QueryRow(q, urlStr)

	var cid sql.NullInt64
	var lang sql.NullString
	var kind sql.NullString
	err = row.Scan(
		&info.Url,
		&info.UrlId,
		&info.UrlRank,
		&info.HostRank,
		&cid,
		&info.ContentTitle,
		&info.ContentType,
		&info.ContentTypeArgs,
		&info.Contents,
		&info.ContentsText,
		&lang,
		&kind)
	if err != nil {
		return
	}

	if cid.Valid {
		info.ContentId = cid.Int64
	} else {
		info.ContentId = -1
	}

	info.ContentLang = lang.String
	info.ContentKind = kind.String

	u, err := url.Parse(info.Url)
	if err != nil {
		return
	}

	// links

	rows, err := db.Query(`
select u.url, links.text
from links
join urls u on u.id = dst_url_id
where src_url_id = $1
`, info.UrlId)
	if err != nil {
		return
	}

	for rows.Next() {
		var durl string
		var linkText string
		err = rows.Scan(&durl, &linkText)
		if err != nil {
			return
		}

		var du *url.URL
		du, err = url.Parse(durl)
		if err != nil {
			return
		}

		if du.Hostname() == u.Hostname() {
			info.InternalLinks = append(info.InternalLinks, gparse.Link{Text: linkText, Url: durl})
		} else {
			info.ExternalLinks = append(info.ExternalLinks, gparse.Link{Text: linkText, Url: durl})
		}
	}

	// backlinks

	rows, err = db.Query(`
select u.url, links.text
from links
join urls u on u.id = src_url_id
where dst_url_id = $1
`, info.UrlId)
	if err != nil {
		return
	}

	for rows.Next() {
		var durl string
		var linkText string
		err = rows.Scan(&durl, &linkText)
		if err != nil {
			return
		}

		var du *url.URL
		du, err = url.Parse(durl)
		if err != nil {
			return
		}

		if du.Hostname() == u.Hostname() {
			info.InternalBacklinks = append(info.InternalBacklinks, gparse.Link{Text: linkText, Url: durl})
		} else {
			info.ExternalBacklinks = append(info.ExternalBacklinks, gparse.Link{Text: linkText, Url: durl})
		}
	}

	return
}
