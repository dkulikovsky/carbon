# Carbon

[![Build Status](https://secure.travis-ci.org/graphite-project/carbon.png?branch=master)](http://travis-ci.org/graphite-project/carbon)

Carbon is the storage component of [Graphite][] and is responsible for
receiving metrics over the network and persisting them to disk. Graphite
supports writing data files in either the [Ceres][] or [Whisper][] (legacy)
file formats.

[Graphite]: https://github.com/graphite-project
[Graphite Web]: https://github.com/graphite-project/graphite-web
[Whisper]: https://github.com/graphite-project/whisper
[Ceres]: https://github.com/graphite-project/ceres

## Development Status
The master branch of Carbon includes a significant refactor of Carbon which
introduces breaking changes from the [0.9.x][] series in the way Carbon is
configured and managed. Until noted otherwise, this branch should be considered
'alpha' quality and subject to minor incompatible changes.

[0.9.x]: https://github.com/graphite-project/carbon/branches/0.9.x

## Overview

Client applications can connect to the running carbon-cache.py daemon on port
2003 (default) and send it lines of text of the following format:

    my.metric.name value unix_timestamp

For example:

    performance.servers.www01.cpuUsage 42.5 1208815315

- The metric name groups the metric in heirarchical fashion using the 'dot' (*.*)
character as a path separator
- The value is a scalar integer or floating point value
- The unix\_timestamp is unix epoch time as an integer.

Each line like this corresponds to one data point for one metric.

Clients may also use Carbon's native metric format, the '[pickle protocol][]'

Once you've got some clients sending data to carbon-cache, you can view
graphs of that data through the frontend [Graphite Web][] application.

[pickle protocol]: http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-pickle-protocol

## Running carbon-daemon.py

Alternatively, you may run `carbon-daemon` as a
[Twistd plugin][], for example:

    Usage: twistd [options] carbon-cache [options]
    Options:
          --debug       Run in debug mode.
      -c, --config=     Use the given config file.
          --instance=   Manage a specific carbon instance. [default: a]
          --logdir=     Write logs to the given directory.
          --version     Display Twisted version and exit.
          --help        Display this help and exit.

Common options to `twistd(1)`, like `--pidfile`, `--logfile`, `--uid`, `--gid`,
`--syslog` and `--prefix` are fully supported and have precedence over
`carbon-daemon`'s own options. Please refer to `twistd --help` for the full list of
supported `twistd` options.

[Twistd plugin]: http://twistedmatrix.com/documents/current/core/howto/plugin.html

## Writing a client

First you obviously need to decide what data it is you want to graph with
graphite. The script [examples/example-client.py] demonstrates a simple client
that sends `loadavg` data for your local machine to carbon on a minutely basis.

The default storage schema stores data in one-minute intervals for 2 hours.
This is probably not what you want so you should create a custom storage schema
according to the docs on the [Graphite wiki][].

[Graphite wiki]: http://graphite.wikidot.com
[examples/example-client.py]: https://github.com/graphite-project/carbon/blob/master/examples/example-client.py

## Some notes on different rollup scripts for Ceres

There are several rollup scripts available (you need to run rollup frequently for Ceres, otherwise it won't lowern retentions of existing data):

- ceres-maintenance's rollup plugin - main and the most tested plugin
- simple-rollup.py - simplier version of rollup (based on the plugin), standalone, easier to read. Should behave more or less same as plugin.
- simple-rollup-ng.py - rewritten from scratch. It should be faster than simple-rollup. It's a lot more easier to read than simple-rollup (due to
 beeing more or less pep8 complaint, and also it have more comments in the code), but it's less tested. It also require user to run merge
 afterwards, because it tends to produce some ammount of small files. One of the problems that it should fix - was small rare data corruptions that
 occured with simple-rollup and also it uses same "baseline" time for all rollup-related work. Use at your own risk, if you want stability, stick
 with simple-rollup or ceres-maintenance plugin.
