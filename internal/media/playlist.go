package media

import (
	"net/url"
	"regexp"
	"strings"
)

var reURIAttr = regexp.MustCompile(`URI="([^"]+)"`)

func withQuery(uri, query string) string {
	if query == "" {
		return uri
	}
	if strings.Contains(uri, "?") {
		return uri + "&" + query
	}
	return uri + "?" + query
}

// TokenQuery is the query string carried on every playlist URI.
func TokenQuery(token string) string {
	if token == "" {
		return ""
	}
	return "t=" + url.QueryEscape(token)
}

// AllowedVariants returns rungs a viewer capped at maxHeight may fetch; the smallest stays if all exceed it.
func AllowedVariants(master string, maxHeight int) map[string]bool {
	_, entries := parseMaster(master)
	allowed := map[string]bool{"audio": true}
	smallest := -1
	smallestDir := ""
	for _, e := range entries {
		dir := strings.Split(e.uri, "/")[0]
		if maxHeight <= 0 || e.height <= maxHeight {
			allowed[dir] = true
		}
		if smallest < 0 || e.height < smallest {
			smallest, smallestDir = e.height, dir
		}
	}
	if len(allowed) == 1 && smallestDir != "" {
		allowed[smallestDir] = true
	}
	return allowed
}

// RewriteMaster drops rungs above maxHeight (0 = no cap) and appends the token to every URI.
func RewriteMaster(master, token string, maxHeight int) string {
	allowed := AllowedVariants(master, maxHeight)
	q := TokenQuery(token)
	lines := strings.Split(strings.ReplaceAll(master, "\r\n", "\n"), "\n")
	out := make([]string, 0, len(lines))
	for i := 0; i < len(lines); i++ {
		l := lines[i]
		t := strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(t, "#EXT-X-STREAM-INF:"):
			j := i + 1
			for j < len(lines) && (strings.TrimSpace(lines[j]) == "" || strings.HasPrefix(strings.TrimSpace(lines[j]), "#")) {
				j++
			}
			if j >= len(lines) {
				i = j
				continue
			}
			uri := strings.TrimSpace(lines[j])
			if allowed[strings.Split(uri, "/")[0]] {
				out = append(out, l, withQuery(uri, q))
			}
			i = j
		case strings.HasPrefix(t, "#EXT-X-MEDIA:"):
			out = append(out, reURIAttr.ReplaceAllStringFunc(l, func(m string) string {
				u := reURIAttr.FindStringSubmatch(m)[1]
				return `URI="` + withQuery(u, q) + `"`
			}))
		default:
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

// RewriteMediaPlaylist appends the token to segment and init URIs.
func RewriteMediaPlaylist(pl, token string) string {
	q := TokenQuery(token)
	lines := strings.Split(strings.ReplaceAll(pl, "\r\n", "\n"), "\n")
	for i, l := range lines {
		t := strings.TrimSpace(l)
		switch {
		case t == "":
		case strings.HasPrefix(t, "#EXT-X-MAP:"):
			lines[i] = reURIAttr.ReplaceAllStringFunc(l, func(m string) string {
				u := reURIAttr.FindStringSubmatch(m)[1]
				return `URI="` + withQuery(u, q) + `"`
			})
		case !strings.HasPrefix(t, "#"):
			lines[i] = withQuery(t, q)
		}
	}
	return strings.Join(lines, "\n")
}
