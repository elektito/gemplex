package mgmt

import (
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"net/http"
)

const rootPage = `
<html>
<body>
<form>
<input type="text" name="q">
<input type="submit">
<br>
<a href="/random">random</a>
</form>
</body>
</html>
`

var dbConnStr string

func Setup(connStr string) {
	dbConnStr = connStr
	http.HandleFunc("/", getRootPage)
	http.HandleFunc("/random", getRandomPage)
}

func getRandomPage(w http.ResponseWriter, r *http.Request) {
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("Error connecting to db: %s\nconnstr: %s", err, dbConnStr))
		return
	}
	defer db.Close()

	var minId int64
	var maxId int64
	err = db.QueryRow("select min(id), max(id) from urls").Scan(&minId, &maxId)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("Error reading from db: %s\nconnstr: %s\n", err, dbConnStr))
		return
	}

	var randId int64
	var url string
	for {
		randId = rand.Int63n(maxId-minId) + minId
		err = db.QueryRow("select url from urls where id = $1 and content_id is not null", randId).Scan(&url)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			io.WriteString(w, fmt.Sprintf("Error reading from db: %s\nconnstr: %s\n", err, dbConnStr))
			return
		}
		break
	}

	http.Redirect(w, r, "/?q="+url, 302)
}

func getRootPage(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Query().Get("q")
	if url == "" {
		io.WriteString(w, rootPage)
	} else {
		db, err := sql.Open("postgres", dbConnStr)
		if err != nil {
			io.WriteString(w, fmt.Sprintf("Error connecting to db: %s\nconnstr: %s", err, dbConnStr))
			return
		}
		defer db.Close()

		var contents string
		var content_type string
		var title string
		err = db.QueryRow(
			`select c.content, c.content_type, c.title from urls u
             join contents c on c.id = u.content_id
             where u.url = $1`,
			url,
		).Scan(&contents, &content_type, &title)
		if err != nil {
			io.WriteString(w, fmt.Sprintf("Error reading from db: %s\nconnstr: %s\n", err, dbConnStr))
			return
		}

		s := fmt.Sprintf(`
<html><body>
url: %s<br>
title: %s<br>
content-type: %s<br>
<hr>
<pre>
%s
</pre>
<a href="/">home</a>
<a href="/random">random</a>
</body></html>
`, url, title, content_type, contents)
		io.WriteString(w, s)
	}
}
