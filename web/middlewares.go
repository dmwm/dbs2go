package web

import (
	"crypto/sha1"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	limiter "github.com/ulule/limiter/v3"
	stdlib "github.com/ulule/limiter/v3/drivers/middleware/stdlib"
	memory "github.com/ulule/limiter/v3/drivers/store/memory"
	"github.com/vkuznet/dbs2go/dbs"
)

// LimiterMiddleware provides limiter middleware pointer
var LimiterMiddleware *stdlib.Middleware

// initialize Limiter middleware pointer
func initLimiter(period string) {
	log.Printf("limiter rate='%s'", period)
	// create rate limiter with 5 req/second
	rate, err := limiter.NewRateFromFormatted(period)
	if err != nil {
		panic(err)
	}
	store := memory.NewStore()
	instance := limiter.New(store, rate)
	LimiterMiddleware = stdlib.NewMiddleware(instance)
}

// helper to auth/authz incoming requests to the server
func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// perform authentication
		status := CMSAuth.CheckAuthnAuthz(r.Header)
		if !status {
			log.Printf("ERROR: fail to authenticate, HTTP headers %+v\n", r.Header)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if Config.Verbose > 2 {
			log.Printf("Auth layer status: %v headers: %+v\n", status, r.Header)
		}

		// check if user has proper roles to DBS (non GET) APIs
		if r.Method != "GET" && Config.CMSRole != "" && Config.CMSGroup != "" {
			status = CMSAuth.CheckCMSAuthz(r.Header, Config.CMSRole, Config.CMSGroup, "")
			if !status {
				log.Printf("ERROR: fail to authorize used with role=%v and group=%v, HTTP headers %+v\n", Config.CMSRole, Config.CMSGroup, r.Header)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}

		// Call the next handler
		next.ServeHTTP(w, r)
	})
}

// helper to validate incoming requests' parameters
func validateMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			next.ServeHTTP(w, r)
			return
		}
		// perform validation of input parameters
		err := dbs.Validate(r)
		if err != nil {
			uri, e := url.QueryUnescape(r.RequestURI)
			if e == nil {
				log.Printf("HTTP %s %s %v\n", r.Method, uri, err)
			} else {
				log.Printf("HTTP %s %v %v\n", r.Method, r.RequestURI, err)
			}
			responseMsg(w, r, err, http.StatusBadRequest)
			return
		}
		// Call the next handler
		next.ServeHTTP(w, r)
	})
}

// limit middleware limits incoming requests
func limitMiddleware(next http.Handler) http.Handler {
	return LimiterMiddleware.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	}))
}

// helper function to get hash of the string, provided by https://github.com/amalfra/etag
func getHash(str string) string {
	return fmt.Sprintf("%x", sha1.Sum([]byte(str)))
}

// Generates an Etag for given string, provided by https://github.com/amalfra/etag
func Etag(str string, weak bool) string {
	tag := fmt.Sprintf("\"%d-%s\"", len(str), getHash(str))
	if weak {
		tag = "W/" + tag
	}
	return tag
}

// response header middleware
func headerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goVersion := runtime.Version()
		tstamp := time.Now().Format("2006-02-01")
		server := fmt.Sprintf("dbs2go (%s %s)", goVersion, tstamp)
		w.Header().Add("Server", server)

		// settng Etag and its expiration
		if r.Method == "GET" && Config.Etag != "" && Config.CacheControl != "" {
			etag := Etag(Config.Etag, false)
			w.Header().Set("Etag", etag)
			w.Header().Set("Cache-Control", Config.CacheControl) // 5 minutes
			if match := r.Header.Get("If-None-Match"); match != "" {
				if strings.Contains(match, etag) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}

		next.ServeHTTP(w, r)
	})
}
