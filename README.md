# 1brc.pl
1 Billion Record Challenge in Perl

Original challenge for Java: <https://github.com/gunnarmorling/1brc>

I used the `generate.c` file ([Source repo](https://github.com/dannyvankooten/1brc/blob/main/create-sample.c)) to generate a billion measurements. Compile with `gcc generate.c -lm` and then run `./a.out 1000000000` to generate the measurements.txt file.

### Some numbers

My machine specs:

Model name: AMD first gen Ryzen 5 1600 (hexacore machine)

Memory usage stays low during the run.

Original baseline.pl solution from the forked repo ran on my computer in about 30 minutes:

```
real	29m40.231s
user	29m33.098s
sys		0m6.567s
```

With 4 processes, it runs in 5 minutes:

```
real	5m24.577s
user	21m28.761s
sys	0m6.857s
```

With 8, it runs a bit faster:

```
real	3m44.463s
user	26m50.545s
sys	0m7.720s
```

Since my machine is a hexacore and has hyperthreading, I can theorically go to 12 but I feel this was already good enough for running it.

The results file are in this repo. I diffed them with the baseline to make sure the output is exactly the same, so there are (presumably) no errors in my multi-processed version.
