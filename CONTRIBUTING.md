# Contributing

This project is work of [many contributors](https://github.com/rcongiu/Hive-JSON-Serde/graphs/contributors).

You're encouraged to submit [pull requests](https://github.com/rcongiu/Hive-JSON-Serde/pulls), 
[propose features and discuss issues](https://github.com/rcongiu/Hive-JSON-Serde/issues).

In the examples below, substitute your Github username for `contributor` in URLs.

## Fork the Project

Fork the [project on Github](https://github.com/rcongiu/Hive-JSON-Serde) and check out your copy.

```
git clone https://github.com/contributor/Hive-JSON-Serde.git
cd Hive-JSON-Serde
git remote add upstream https://github.com/rcongiu/Hive-JSON-Serde.git
```

## Build

Ensure that you can build the project and run tests.

```
git checkout develop
mvn test
```

### Architecture

JSON encoding and decoding is using a somewhat modified version of 
[Douglas Crockfords JSON library](https://github.com/douglascrockford/JSON-java), which is included in the distribution.

The SerDe builds a series of wrappers around `JSONObject`. Since serialization and deserialization are executed 
for every (and possibly billions) record we want to minimize object creation, so instead of serializing/deserializing
to an `ArrayList`, the `JSONObject` is kept and a cached
`ObjectInspector` is built around it. When deserializing, Hive gets a `JSONObject`, and a `JSONStructObjectInspector` 
to read from. Hive has `Structs`, `Maps`, `Arrays` and primitives while `JSON` has `Objects`, `Arrays` and primitives. 
Hive `Maps` and `Structs` are both implemented as `Object`, which are less restrictive than hive maps. 
A JSON `Object` could be a mix of keys and values of different types, while Hive expects you to declare the
type of map (eg. `map<string,string>`). The user is responsible for having the JSON data structure match hive 
table declaration.

See [www.congiu.com](http://www.congiu.com/?s=serde) for details.

### Compiling for Specific Targets

Use maven to compile the SerDe. This project uses maven profiles to support multiple version of Hive/CDH.

#### CDH4

```
mvn -Pcdh4 clean package
```

#### CDH5

```
mvn -Pcdh5 clean package
```

#### HDP 2.3

```
mvn -Phdp23 clean package
```

### Other versions of hadoop

Somebody asked for other versions of hadoop that were not supported by
Cloudera (1.x). It is possible to build the serde for those versions:

```
 mvn -Dcdh5.hadoop.version=1.2.1 -Dhadoop.dependency=hadoop-core clean package
```


### Generate a JAR

All output is generated into `json-serde/target/json-serde-VERSION-jar-with-dependencies.jar`.

```
$ mvn package
```

#### Specific Versions of Hive

If you want to compile the SerDe against a different version of the cloudera libs, use `-D`.

```
$ mvn -Dcdh.version=0.9.0-cdh3u4c-SNAPSHOT package
```

For Hive 0.14.0 and Cloudera 1.0.0.

```
mvn -Pcdh5 -Dcdh5.hive.version=1.0.0 clean package
```

## Write Tests

Try to write a test that reproduces the problem you're trying to fix or describes a feature that you want to build.

We definitely appreciate pull requests that highlight or reproduce a problem, even without a fix.

## Write Code

Implement your feature or bug fix.

## Write Documentation

Document any external behavior in the [README](README.md).

## Update Changelog

Add a line to [CHANGELOG](CHANGELOG.md) under *Next* release.
Make it look like every other line, including your name and link to your Github account.

## Commit Changes

Make sure git knows your name and email address:

```
git config --global user.name "Your Name"
git config --global user.email "contributor@example.com"
```

Writing good commit logs is important. A commit log should describe what changed and why.

```
git add ...
git commit
```

## Push

```
git push origin my-feature-branch
```

## Make a Pull Request

Go to https://github.com/contributor/Hive-JSON-Serde and select your feature branch.
Click the 'Pull Request' button and fill out the form. Pull requests are usually reviewed within a few days.

## Rebase

If you've been working on a change for a while, rebase with upstream/master.

```
git fetch upstream
git rebase upstream/master
git push origin my-feature-branch -f
```

## Update CHANGELOG Again

Update the [CHANGELOG](CHANGELOG.md) with the pull request number. A typical entry looks as follows.

It can be generated automatically with `mvn generate-sources`.

```
* [#123](https://github.com/rcongiu/Hive-JSON-Serde/pull/123): Reticulated splines - [@contributor](https://github.com/contributor).
```

Amend your previous commit and force push the changes.

```
git commit --amend
git push origin my-feature-branch -f
```

## Check on Your Pull Request

Go back to your pull request after a few minutes and see whether it passed muster with Travis-CI. Everything should 
look green, otherwise fix issues and amend your commit as described above.

## Be Patient

It's likely that your change will not be merged and that the nitpicky maintainers will ask you to do more, or fix 
seemingly benign problems. Hang on there!

Keep in mind that this SerDe is used by many, many people, so we don't want to make any non-backward compatible
change unless it's really, really necessary.
Also, we don't want to introduce any surprise behavior. 
For example, we do want the query to fail on incorrect/broken
data *unless we have a way for the user to force it*. The default behaviour on malformed data is to fail, and
not to quietly ingest it returning a value.


## Thank You

Please do know that we really appreciate and value your time and work. We love you, really.


