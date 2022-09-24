package main_test

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEndToEnd(t *testing.T) {
	r := newRunner(t)
	defer r.close()

	const (
		accounts = `1,account1,HR
2,account2,Dev
4,account4,HR
3,account3,PR
`
		departments = `10,HR,Human Resources
12,PR,Public Relations
11,Dev,Development
`
		department_ext = `Development,2
Human Resources,2b
Public Relations,3a
Marketing,1b
Accounting,1a
`
	)

	var (
		accountsCSV      = r.path("accounts.csv")
		departmentsCSV   = r.path("departments.csv")
		departmentExtCSV = r.path("department_ext.csv")
	)

	data := map[string]string{
		accountsCSV:      accounts,
		departmentsCSV:   departments,
		departmentExtCSV: department_ext,
	}
	for name, content := range data {
		f, err := os.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := io.WriteString(f, content); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		title string
		args  []string
		stdin io.Reader
		want  []string
	}{
		{
			title: "join accounts no changes",
			args:  []string{accountsCSV},
			want:  strings.Split(strings.TrimRight(accounts, "\n"), "\n"),
		},
		{
			title: "join accounts from stdin no changes",
			args:  []string{"-x"},
			stdin: bytes.NewBufferString(accounts),
			want:  strings.Split(strings.TrimRight(accounts, "\n"), "\n"),
		},
		{
			title: "join accounts from stdin no changes with target",
			args:  []string{"-x", "-t", "1.2"},
			stdin: bytes.NewBufferString(accounts),
			want: []string{
				"account1",
				"account2",
				"account3",
				"account4",
			},
		},
		{
			title: "join accounts and department",
			args:  []string{"-k", "1.3=2.2", accountsCSV, departmentsCSV},
			want: []string{
				"1,account1,HR,10,HR,Human Resources",
				"2,account2,Dev,11,Dev,Development",
				"3,account3,PR,12,PR,Public Relations",
				"4,account4,HR,10,HR,Human Resources",
			},
		},
		{
			title: "join accounts and department with target",
			args:  []string{"-k", "1.3=2.2", "-t", "-1.2,2.3", accountsCSV, departmentsCSV},
			want: []string{
				"1,account1,Human Resources",
				"2,account2,Development",
				"3,account3,Public Relations",
				"4,account4,Human Resources",
			},
		},
		{
			title: "join accounts from stdin and department with target",
			args:  []string{"-x", "-k", "1.3=2.2", "-t", "2.1,1.1,2.3", departmentsCSV},
			stdin: bytes.NewBufferString(accounts),
			want: []string{
				"10,1,Human Resources",
				"11,2,Development",
				"10,4,Human Resources",
				"12,3,Public Relations",
			},
		},
		{
			title: "join accounts, departments and department_ext",
			args:  []string{"-k", "1.3=2.2,2.3=3.1", accountsCSV, departmentsCSV, departmentExtCSV},
			want: []string{
				"1,account1,HR,10,HR,Human Resources,Human Resources,2b",
				"2,account2,Dev,11,Dev,Development,Development,2",
				"3,account3,PR,12,PR,Public Relations,Public Relations,3a",
				"4,account4,HR,10,HR,Human Resources,Human Resources,2b",
			},
		},
		{
			title: "join accounts from stdin, departments and department_ext with target",
			args:  []string{"-x", "-k", "1.3=2.2,2.3=3.1", "-t", "-1.2,-2.2,3.1-", departmentsCSV, departmentExtCSV},
			stdin: bytes.NewBufferString(accounts),
			want: []string{
				"1,account1,10,HR,Human Resources,2b",
				"2,account2,11,Dev,Development,2",
				"3,account3,12,PR,Public Relations,3a",
				"4,account4,10,HR,Human Resources,2b",
			},
		},
	} {
		tc := tc
		t.Run(tc.title, func(t *testing.T) {
			var got bytes.Buffer
			cmd := newCommand(r.runnable, tc.args...).setStdout(&got)
			if tc.stdin != nil {
				cmd.setStdin(tc.stdin)
			}
			if err := cmd.run(); err != nil {
				t.Fatalf("%v", err)
			}
			ss := strings.Split(strings.TrimRight(got.String(), "\n"), "\n")
			sort.Strings(ss)
			sort.Strings(tc.want)
			assert.Equal(t, tc.want, ss)
		})
	}
}

type runner struct {
	dir      string
	runnable string
}

func newRunner(t *testing.T) *runner {
	t.Helper()
	r := &runner{}
	r.init(t)
	return r
}

func (r *runner) init(t *testing.T) {
	t.Helper()
	// make a directory to install joiny binary and other files for test.
	dir, err := os.MkdirTemp("", "joiny")
	if err != nil {
		t.Fatal(err)
	}
	r.dir = dir
	r.runnable = r.path("joiny")
	// build joiny binary
	if err := newCommand("go", "build", "-o", r.runnable).setStdout(os.Stdout).run(); err != nil {
		t.Fatal(err)
	}
}

func (r *runner) path(name string) string { return filepath.Join(r.dir, name) }
func (r *runner) close()                  { os.RemoveAll(r.dir) }

type command struct {
	cmd *exec.Cmd
}

func (c *command) run() error { return c.cmd.Run() }

func (c *command) setStdin(r io.Reader) *command {
	c.cmd.Stdin = r
	return c
}

func (c *command) setStdout(w io.Writer) *command {
	c.cmd.Stdout = w
	return c
}

func newCommand(name string, arg ...string) *command {
	c := exec.Command(name, arg...)
	c.Stderr = os.Stderr
	return &command{
		cmd: c,
	}
}
