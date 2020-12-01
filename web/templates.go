package web

// templates module
//
// Copyright (c) 2020 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"bytes"
	"html/template"
	"path/filepath"

	"github.com/dmwm/das2go/config"
)

// consume list of templates and release their full path counterparts
func fileNames(tdir string, filenames ...string) []string {
	flist := []string{}
	for _, fname := range filenames {
		flist = append(flist, filepath.Join(tdir, fname))
	}
	return flist
}

// parse template with given data
func parseTmpl(tdir, tmpl string, data interface{}) string {
	buf := new(bytes.Buffer)
	filenames := fileNames(tdir, tmpl)
	funcMap := template.FuncMap{
		// The name "oddFunc" is what the function will be called in the template text.
		"oddFunc": func(i int) bool {
			if i%2 == 0 {
				return true
			}
			return false
		},
		// The name "inListFunc" is what the function will be called in the template text.
		"inListFunc": func(a string, list []string) bool {
			check := 0
			for _, b := range list {
				if b == a {
					check += 1
				}
			}
			if check != 0 {
				return true
			}
			return false
		},
	}
	t := template.Must(template.New(tmpl).Funcs(funcMap).ParseFiles(filenames...))
	err := t.Execute(buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

// ServerTemplates structure
type ServerTemplates struct {
	top, bottom, pagination, files, searchForm, cards, serverError, keys, zero, record, status string
}

// Tmpl method for ServerTemplates structure
func (q ServerTemplates) Tmpl(tdir, tfile string, tmplData map[string]interface{}) string {
	if q.top != "" {
		return q.top
	}
	q.top = parseTmpl(config.Config.Templates, tfile, tmplData)
	return q.top
}
