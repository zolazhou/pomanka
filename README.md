# pomanka

Pomanka is a very simple Kafka replacement for early stage of a project or developing environment.

### Usage

Before using this library, you need to compile a namespace to class:

```
> mkdir classes
> clj -M -e "(compile 'pomanka.dumper.offset)"
```

Add `classes` dir to `:paths` section in deps.edn:

```clojure
{:paths ["src" "classes"]
 :deps  {}}
```
