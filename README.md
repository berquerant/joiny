# joiny

```
$ joiny -h
Usage: joiny [flags] FILES...

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

Flags:
  -d string
        delimiter (default ",")
  -k string
        key
  -t string
        target
  -v int
        verbose level
  -x    read stdin
```
