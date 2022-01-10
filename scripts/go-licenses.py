#!/usr/bin/env python3

# Reads one-package-name-per-line from stdin and finds the license text for
# each. Generate a markdown file to stdout. Err if we cannot identify a
# license for any package.

# Driven by go-licenses.sh in this same directory.

import os.path
import sys

# Identify all the vendor directories immediately beneath "src/*".
VENDORS = []
if os.path.exists("./vendor"):
    VENDORS.append("./vendor")

LICENSE_NAMES = ["License", "LICENSE", "LICENSE.md", "LICENSE.txt"]


def find_license(package):
    """
    Find a license for an imported package across all vendor directories.
    Assumes license consistency if the package is vendored in multiple
    hierarchies.
    """
    while True:
        for vendor in VENDORS:
            for name in LICENSE_NAMES:
                license = os.path.join(vendor, package, name)
                if os.path.exists(license):
                    return (package, license)
        package = os.path.dirname(package)
        if package == "":
            break
    return None


def emit_license(package, license_text):
    print("###", package)
    print()
    print("```")
    print(license_text.strip())
    print("```")
    print()


# This is a set of imported packages that do not have easily identified
# licenses. If this grows beyond the two we have today, we should consider
# something more easily maintained (a directory containing license
# exceptions...).
exceptions = set(
    [
    ]
)

# Seeded with licenses from our exceptions.
licenses = {
}

for line in sys.stdin:
    package = line.rstrip()

    if package in exceptions:
        continue

    licensing = find_license(package)
    if licensing:
        licensed_package, license = licensing
        with open(license, "r") as fh:
            licenses[licensed_package] = fh.read()
    else:
        raise Exception("no license for package %s" % package)

for package in sorted(licenses):
    emit_license(package, licenses[package])
