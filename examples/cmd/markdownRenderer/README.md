# Example App `markdownRenderer`

## Usage

To use this test service, simply build and start the application with:

```
just build
./out/markdownRenderer
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

    func Jonathan(name string) {
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
