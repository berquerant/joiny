package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/berquerant/joiny/cc/joinkey"
	"github.com/berquerant/joiny/cc/target"
	"github.com/berquerant/joiny/joiner"
	"github.com/berquerant/joiny/logx"
	"github.com/berquerant/joiny/temporary"
)

const usage = `Usage: joiny [flags] FILES...

Join files.

key is a join condition, like "1.2=2.3", means that join the 2nd column of the source 1 and
the 3rd column of the source 2.
files[0] is the source 1, files[1] is the source 2.
Default key joins by first columns, e.g. "1.1=2.1"

target is an output format, like "1.1,2.1-", means that the 1st column of the source 1 and
the all columns of the source 2.
Default target is the all columns.
The syntax is:
  natural := natural number
  location := natural "." natural  // source . column
  single := location
  left := location "-"  // left limited
  right := "-" location  // right limited
  interval := location "-" location  // left and right limited
  range := interval | right | left | single
  target := range {"," range}

e.g.
$ cat > account.csv <<EOS
1,account1,HR
2,account2,Dev
4,account4,HR
3,account3,PR
EOS
$ cat > department.csv <<EOS
10,HR,Human Resources
12,PR,Public Relations
11,Dev,Development
EOS
$ joiny -d "," -k "1.3=2.2" -t "1.1-,2.1-" account.csv department.csv
1,account1,HR,10,HR,Human Resources
2,account2,Dev,11,Dev,Development
4,account4,HR,10,HR,Human Resources
3,account3,PR,12,PR,Public Relations
$ joiny -d "," -k "1.3=2.2" -t "-1.2,2.3" account.csv department.csv
1,account1,Human Resources
2,account2,Development
4,account4,Human Resources
3,account3,Public Relations
$ joiny -d "," -k "1.3=2.2" -t "2.1,1.1,2.3" department.csv < account.csv
10,1,Human Resources
11,2,Development
10,4,Human Resources
12,3,Public Relations
$ joiny -k "1.3=2.2,2.3=3.1" account.csv department.csv department_ext.csv
1,account1,HR,10,HR,Human Resources,Human Resources,2b
4,account4,HR,10,HR,Human Resources,Human Resources,2b
2,account2,Dev,11,Dev,Development,Development,2
3,account3,PR,12,PR,Public Relations,Public Relations,3a

Read stdin when use -x flag, stdin is the source 1.

$ cat > department_ext.csv <<EOS
Development,2
Human Resources,2b
Public Relations,3a
Marketing,1b
Accounting,1a
EOS
$ joiny -x -d "," -k "1.3=2.2" -t "2.1,1.1,2.3" department.csv < account.csv | joiny -x -d "," -k "1.3=2.1" -t "1.1-,2.2" department_ext.csv
10,1,Human Resources,2b
11,2,Development,2
10,4,Human Resources,2b
12,3,Public Relations,3a
$ joiny -x -k '1.3=2.2,2.3=3.1' -t '-1.2,-2.2,3.1-' department.csv department_ext.csv < account.csv
1,account1,10,HR,Human Resources,2b
4,account4,10,HR,Human Resources,2b
2,account2,11,Dev,Development,2
3,account3,12,PR,Public Relations,3a

Flags:`

func Usage() {
	fmt.Fprintln(os.Stderr, usage)
	flag.PrintDefaults()
}

var (
	targetStr  = flag.String("t", "", "target")
	key        = flag.String("k", "", "key")
	delim      = flag.String("d", ",", "delimiter")
	readStdin  = flag.Bool("x", false, "read stdin")
	loadThread = flag.Int("j", 4, "number of threads to load files")
	cacheSize  = flag.Int("c", 1024, "max cache size for index")
	verbose    = flag.Int("v", 0, "verbose level")
)

func main() {
	flag.Usage = Usage
	flag.Parse()

	exitCode := func() int {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()

		var (
			doneC = make(chan struct{})
			err   error
		)
		go func() {
			defer close(doneC)
			if err = withFileList(ctx, run); err != nil {
				logx.G().Error("got error", logx.Err(err))
			}
		}()

		select {
		case <-ctx.Done():
			stop()
		case <-doneC:
		}

		if err != nil {
			return 1
		}
		return 0
	}()
	os.Exit(exitCode)
}

var (
	errNoSources = errors.New("NoSources")
)

func run(ctx context.Context, fs []io.ReadSeeker) error {
	if len(fs) < 1 {
		return errNoSources
	}
	jKey, err := parseKey(len(fs))
	if err != nil {
		return err
	}
	tgt, err := parseTarget(len(fs))
	if err != nil {
		return err
	}
	cache, err := joiner.NewCacheBuilder(
		fs,
		joiner.RelationListToLocationList(jKey.RelationList),
		*delim,
		*loadThread,
		*cacheSize,
	).Build(ctx)
	if err != nil {
		return err
	}
	sel := joiner.NewSelector(cache)
	join := joiner.New(joiner.NewRelationJoiner(cache))
	for row := range join.Join(ctx, jKey) {
		line, err := sel.Select(tgt, row.Sorted())
		if err != nil {
			logx.G().Error("Failed to select", logx.Err(err), logx.Any("row", row))
			continue
		}
		fmt.Println(line)
	}
	return nil
}

func withFileList(ctx context.Context, callback func(context.Context, []io.ReadSeeker) error) error {
	var (
		list     = flag.Args()
		fileList []io.ReadSeeker
		add      = func(r io.ReadSeeker) {
			fileList = append(fileList, r)
		}
	)

	if *readStdin {
		stdin, err := stdinToTempFile()
		if err != nil {
			return err
		}
		defer stdin.Close()
		add(stdin)
	}

	for _, x := range list {
		f, err := os.Open(x)
		if err != nil {
			return err
		}
		defer f.Close()
		add(f)
	}

	return callback(ctx, fileList)
}

func stdinToTempFile() (*temporary.File, error) {
	f, err := temporary.NewFile()
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(f, os.Stdin); err != nil {
		return nil, err
	}
	return f, nil
}

func parseTarget(n int) (*target.Target, error) {
	l := target.NewLexer(bytes.NewBufferString(getTarget(n)))
	l.Debug(*verbose)
	target.Parse(l)
	if err := l.Err(); err != nil {
		return nil, err
	}
	return l.Target, nil
}

func getTarget(n int) string {
	if *targetStr != "" {
		return *targetStr
	}
	ss := make([]string, n)
	for i := range ss {
		ss[i] = fmt.Sprintf("%d.1-", i+1)
	}
	return strings.Join(ss, ",")
}

func parseKey(n int) (*joinkey.JoinKey, error) {
	l := joinkey.NewLexer(bytes.NewBufferString(getKey(n)))
	l.Debug(*verbose)
	joinkey.Parse(l)
	if err := l.Err(); err != nil {
		return nil, err
	}
	return l.JoinKey, nil
}

func getKey(n int) string {
	if *key != "" {
		return *key
	}
	if n == 1 { // identity join
		return "1.1=1.1"
	}
	ss := make([]string, n-1)
	for i := range ss {
		ss[i] = fmt.Sprintf("%d.1=%d.1", i+1, i+2)
	}
	return strings.Join(ss, ",")
}
