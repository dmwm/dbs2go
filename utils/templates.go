package utils

import (
	"bytes"
	"path/filepath"
	"text/template"
)

// consume list of templates and release their full path counterparts
func fileNames(tdir string, filenames ...string) []string {
	flist := []string{}
	for _, fname := range filenames {
		flist = append(flist, filepath.Join(tdir, fname))
	}
	return flist
}

// ParseTmpl parses template with given data
func ParseTmpl(tdir, tmpl string, data interface{}) (string, error) {
	buf := new(bytes.Buffer)
	filenames := fileNames(tdir, tmpl)
	t := template.Must(template.ParseFiles(filenames...))
	err := t.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), err
}
