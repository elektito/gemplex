# Gemplex: A Gemini Search Engine

Gemplex is an experimental [Gemini][1] search engine currently available at
[gemplex.space][2].

Gemplex is written in Go. In order to build it, you need to have `go` available
on your machine (tested with Go 1.20). You can build all the required executable
by running `make`.

Gemplex consists of three executables: `gemplex` which is the main executable,
`gpcgi` which is a CGI script for exposing gemplex over Gemini, and `gpctl`
which provides some utilities.

You will need a postgres database before running Gemplex. See the README in the
`db` sub-directory for more details.

## gemplex executable

The `gemplex` executable accepts one or more of the following sub-commands:

 - `crawl`: Runs the crawler.
 - `rank`: Periodically performs the Page Rank algorithm on the URLs and links
   stored in the database and stores the results in the database.
 - `index`: Periodically indexes the contents of the database. At every point in
   time one of two indices (named "ping" and "pong") is active. Upon each
   re-indexing, the indexer switches the active index which is used by the
   search daemon.
 - `search`: Starts the search daemon which is normally accessed by the CGI
   script.
   
You can also pass the `all` pseudo-command to run all sub-commands at the same
time.

The `gemplex` executable can read its configuration from a toml formatted config
file. You can pass the address to this file using the `-config` flag.

## gpcgi executable

This executable is normally run using a CGI-capable Gemini server (for example,
[Hodhod][3] which is used to host the service at gemplex.space).

For testing purposes, you can pass the `-serve` argument to `gpcgi` which causes
a development Gemini server to be launched.

## gpctl executable

This executable provides a number of utilities to manage and monitor a Gemplex
installation. The following sub-commands are available:

 - `addseed`: Add a URL to the database.
 - `delhost`: Delete all URLs and links for a given hostname (that are not
   referenced by any other rows) from the database.
 - `index`: Indexes the database contents.
 - `pagerank`: Updates URL/host rankings in the database.
 - `reparse`: Re-parses all the pages stored in the database and extracts
   metadata from them (like title, language, etc.) and stores them back to the
   database. This can be useful if a change is made to the parsing routines and
   we want it applied back to the content that is already crawled and stored.
 - `url`: Displays information about a given URL.

[1]: https://gemini.circumlunar.space/
[2]: gemini://gemplex.space/
[3]: gemini://elektito.com/hodhod
