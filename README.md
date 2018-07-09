# SNOWPLOW - Take home

Task description [here](https://gist.github.com/BenFradet/04c582c4760873e1c0c0ce2807624e39).

## build

```bash
$ sbt assembly
```

*NOTE: this is compiled using Spark 2.3.0.*

## run

```bash
$ spark-submit \
  --deploy-mode client \
  --driver-java-options=" \
    -Dinput-file=path/to/input.csv \
    -Doutput-path=path/to/takehome_output/" \ # on Spark master
  path/to/take-home-assembly-1.0-SNAPSHOT.jar
```

The default parameters for the job can be found in
[this file](./src/main/resources/reference.conf).
Any parameter can be overridden by specifying it inside `--driver-java-options`.
For instance, to override the desired number of most expensive houses,
add `-Dqueries.nb-most-expensive-houses=20`.

Parameters can also be overridden in a new file
(e.g. `application.conf`). Then the file has to be specified
inside `--driver-java-options`
 when running the job: `-Dconfig.file=path/to/application.conf`.

Logging properties of the Spark cluster will be used.

## Comments

Input, queries and output are separated in the code,
so that each one of these can be updated independently of the others.
The interface between them is like this :
```
input -> DataFrame -> queries -> DataFrame -> output
```

For this take home the output is written on disk on the Spark master,
but the code would be very easy to adapt to write the results somewhere
else, using `DataFrame` API for instance. Same for the reading.

I wish I had more time to add unit tests for each querying method.

Also, this code provides very basic error handling (just checks that the
CSV file can be read and that the associated `DataFrame` contains all the
columns used by the queries). Error handling should be greatly improved
for production. Each querying method should check
that the input `DataFrame` contains all the column it needs,
with the correct type, and that the queries succeed.
Specifying programmatically the schema when reading,
to get a `Dataset[House]` instead of a `Dataset[Row]`,
would help a lot for this, and would make most of the code way cleaner.
It would also make unit testing easier.
