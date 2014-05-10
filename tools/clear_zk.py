#!/usr/bin/env python

import contextlib
import os
import re
import sys

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow.utils import kazoo_utils


@contextlib.contextmanager
def finalize_client(client):
    try:
        yield client
    finally:
        kazoo_utils.finalize_client(client)


def iter_children(client, path):
    if client.exists(path):
        for child_path in client.get_children(path):
            if path == "/":
                child_path = "/%s" % (child_path)
            else:
                child_path = "%s/%s" % (path, child_path)
            yield child_path
            for child_child_path in iter_children(client, child_path):
                yield child_child_path


def main():
    conf = {}
    if len(sys.argv) > 1:
        conf['hosts'] = sys.argv[1:]
    with finalize_client(kazoo_utils.make_client(conf)) as client:
        client.start(timeout=1.0)
        children = list(iter_children(client, "/taskflow"))
        for child_path in reversed(children):
            if not re.match(r"^/taskflow/(.*?)-test/(.*)$", child_path):
                continue
            print("Deleting %s" % child_path)
            client.delete(child_path)


if __name__ == "__main__":
    main()
