#!/bin/bash
set -eux
if [ -z "$(which etcd)" ]; then
    ETCD_VERSION=3.3.27
    case `uname -s` in
        Darwin)
            OS=darwin
            SUFFIX=zip
            ;;
        Linux)
            OS=linux
            SUFFIX=tar.gz
            ;;
        *)
            echo "Unsupported OS"
            exit 1
    esac
    case `uname -m` in
         x86_64)
             MACHINE=amd64
             ;;
         *)
            echo "Unsupported machine"
            exit 1
    esac
    TARBALL_NAME=etcd-v${ETCD_VERSION}-$OS-$MACHINE
    test ! -d "$TARBALL_NAME" && curl -L https://github.com/coreos/etcd/releases/download/v${ETCD_VERSION}/${TARBALL_NAME}.${SUFFIX} | tar xz
    export PATH=$PATH:$TARBALL_NAME
fi

$*
