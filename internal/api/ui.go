package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html/template"
	"io/fs"
	"net/http"
	"sync"
	"time"

	"github.com/slothnetworks/artemis/web"
	"golang.org/x/crypto/bcrypt"
)

// pageData is the data passed to every page template.
type pageData struct {
	Title      string
	Active     string
	User       string
	ShowLogout bool
}

// nav describes the rendered pages.
var pages = map[string]struct{ title, active string }{
	"dashboard": {"Dashboard", "dashboard"},
	"agents":    {"Agents", "agents"},
	"mesh":      {"Mesh", "mesh"},
	"alerts":    {"Alerts", "alerts"},
}

type ui struct {
	tmpl  map[string]*template.Template
	login *template.Template
}

func newUI() (*ui, error) {
	funcs := template.FuncMap{"dict": dict}
	base, err := template.New("base").Funcs(funcs).ParseFS(web.FS, "templates/layouts/*.html")
	if err != nil {
		return nil, err
	}
	u := &ui{tmpl: make(map[string]*template.Template)}
	for name := range pages {
		clone, err := base.Clone()
		if err != nil {
			return nil, err
		}
		if _, err := clone.ParseFS(web.FS, "templates/pages/"+name+".html"); err != nil {
			return nil, err
		}
		u.tmpl[name] = clone
	}
	u.login, err = template.New("login").ParseFS(web.FS, "templates/pages/login.html")
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (u *ui) render(w http.ResponseWriter, name string, data pageData) {
	t, ok := u.tmpl[name]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "base", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// dict builds a map from alternating key/value args (for template composition).
func dict(values ...any) (map[string]any, error) {
	if len(values)%2 != 0 {
		return nil, errors.New("dict: odd number of args")
	}
	m := make(map[string]any, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		k, ok := values[i].(string)
		if !ok {
			return nil, errors.New("dict: keys must be strings")
		}
		m[k] = values[i+1]
	}
	return m, nil
}

// Auth is the optional session-cookie authenticator. When User/Hash are empty
// the UI and API are open (dev mode).
type Auth struct {
	User string
	Hash string

	mu       sync.Mutex
	sessions map[string]time.Time
}

const sessionCookie = "artemis_session"

// NewAuth returns an authenticator. Empty user/hash means auth is disabled.
func NewAuth(user, hash string) *Auth {
	return &Auth{User: user, Hash: hash, sessions: make(map[string]time.Time)}
}

// Enabled reports whether authentication is enforced.
func (a *Auth) Enabled() bool { return a.User != "" && a.Hash != "" }

func (a *Auth) valid(r *http.Request) bool {
	if !a.Enabled() {
		return true
	}
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	exp, ok := a.sessions[c.Value]
	if !ok || time.Now().After(exp) {
		delete(a.sessions, c.Value)
		return false
	}
	return true
}

func (a *Auth) login(user, pass string) (string, bool) {
	if user != a.User || bcrypt.CompareHashAndPassword([]byte(a.Hash), []byte(pass)) != nil {
		return "", false
	}
	buf := make([]byte, 24)
	_, _ = rand.Read(buf)
	tok := hex.EncodeToString(buf)
	a.mu.Lock()
	a.sessions[tok] = time.Now().Add(12 * time.Hour)
	a.mu.Unlock()
	return tok, true
}

func (a *Auth) logout(tok string) {
	a.mu.Lock()
	delete(a.sessions, tok)
	a.mu.Unlock()
}

// guard wraps a handler, redirecting browsers to /login (or 401 for /api) when
// unauthenticated. No-op when auth is disabled.
func (a *Auth) guard(apiPath bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.valid(r) {
			next(w, r)
			return
		}
		if apiPath {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	}
}

func (s *Server) handleLoginPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.ui.login.ExecuteTemplate(w, "login.html", nil)
}

func (s *Server) handleLocalAuth(w http.ResponseWriter, r *http.Request) {
	var body struct{ Username, Password string }
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	tok, ok := s.auth.login(body.Username, body.Password)
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid username or password"})
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: tok, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.auth.logout(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) page(name string) http.HandlerFunc {
	meta := pages[name]
	return func(w http.ResponseWriter, _ *http.Request) {
		s.ui.render(w, name, pageData{Title: meta.title, Active: meta.active, User: s.auth.User, ShowLogout: s.auth.Enabled()})
	}
}

func staticHandler() http.Handler {
	sub, _ := fs.Sub(web.FS, "static")
	return http.StripPrefix("/static/", http.FileServer(http.FS(sub)))
}
