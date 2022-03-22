# Example App `markdownRenderer`

## Description

This example application lets you input Markdown and render it to HTML. It
demonstrates using the following platform-lib modules:

- [rscache](../../../pkg/rscache/README.md) to avoid duplicate work by caching
  HTML rendered from markdown.
- [rslog](../../../pkg/rslog/README.md) for debug logging.
- [rsnotify](../../../pkg/rsnotify/README.md) to provide a notification
  mechanism for `rscache`, `rsqueue`, and `rsstorage`.
- [rsqueue](../../../pkg/rsqueue/README.md) for queuing the jobs that render
  markdown to HTML.
- [rsstorage](../../../pkg/rsstorage/README.md) for underlying file storage
  support for caching rendered HTML.

## Usage

To use this test service, simply build and start the application with:

```bash
just build
./out/markdownRenderer
```

To use an alternate address/port, you can use the `--address` flag:

```bash
./out/markdownRenderer --address www.mysite.local:8082
./out/markdownRenderer --address :9000
```

Then, visit [http://localhost:8082](http://localhost:8082).

## Application Data

As you create documents, they will be saved in the `data` directory. The
database used by the queue is also stored in the `data` directory. You can
clear this directory any time when `markdownRenderer` is not running.

## Example Markdown

Here is some example Markdown you can paste into the app for testing.

```
# This is a header

## This is a subheader

Here is a list

* Day one
* Day two
* Day three

Here is some code:

    func PrintName(name string) {
      fmt.Printf("My name is %s", name)
    }

And here is a table:

Name    | Age
--------|------
Bob     | 27
Alice   | 23
========|======
Total   | 50
```
