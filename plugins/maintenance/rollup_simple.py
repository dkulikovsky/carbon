"""
Carbon plugin that performs rollups
"""
import logging
import logging.handlers
import time
import json
from carbon.conf import settings, read_writer_configs
import ceres
import traceback
from operator import itemgetter

from pprint import pprint
# Python 3.4 compatibility
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

#######################################################
# Put your custom aggregation logic in this function! #
#######################################################

read_writer_configs()
try:
    ceres.MAX_SLICE_GAP = int(settings['ceres']['MAX_SLICE_GAP'])
except KeyError:
    pass

FORMAT = '%(asctime)-15s %(message)s'
logger = None


def aggregate_avg(values):
    """
    Compute AVG for list of points.
    :param values: list of values
    :return:
    """
    l = len(values)
    if l is 0:
        return float('nan')
    return float(sum(values)) / len(values)


def aggregate_median(values):
    """
    Determine median (by value) points for list of values.
    :param values: list of values
    :return:
    """
    values.sort()
    return values[len(values) / 2]


def aggregate(method, values, new_step):
    """
    Aggregate list((ts, value)) using one of functions available.

    :exception ValueError: when unknown function specified
    :param method: Aggregation method. Available: avg, sum, min, max, median
    :param values: list of values
    :param new_step: step for aggregated data
    """
    if method in ('avg', 'average'):
        # values is guaranteed to be nonempty
        func = aggregate_avg
    elif method == 'sum':
        func = sum
    elif method == 'min':
        func = min
    elif method == 'max':
        func = max
    elif method == 'median':
        func = aggregate_median
    else:
        raise ValueError("Unsupported aggregation method: %s" % method)

    begin = values[-1][0]
    end = values[0][0]
    if int((end-begin)/new_step) < 0:
        raise ValueError("End < Begin in aggregate")
    result = list()
    i = -1
    logger.debug("begin=%i, end=%i, new_step=%i, new_points=%i", begin, end, new_step, int((end-begin)/new_step))
    for ts in range(begin, end, new_step):
        new_values = list()

        while values[i][0] <= ts:
            new_values.append(values[i][1])
            i -= 1

        result.append((ts, func(new_values)))
    return result


def node_found(node_fs_path, root, rollup_time = None):
    """
    :param node_fs_path: node path
    :param root: Ceres Tree root
    """
    global logger
    if logger is None:
        import random
        tid = random.randint(1, 10000)
        logging.basicConfig(level=logging.DEBUG,
                            format=FORMAT)
        logger = logging.getLogger("rollup-%i" % tid)
        logger.debug("initialized logfile '/srv/log/ceres_rollup_test/rollup-%i.log'" % tid)
    # Used for stats
    start_time = time.time()

    if not rollup_time:
        rollup_time = int(start_time)
    archives = list()
    t = rollup_time
    tree_tmp = ceres.getTree(root)
    node_path = tree_tmp.getNodePath(node_fs_path)
    node = ceres.CeresNode(tree_tmp, node_path, node_fs_path)

    try:
        metadata = node.readMetadata()
    except (OSError, ValueError) as e:
        traceback.print_exc()
        logger.error("%s failed to read metadata: %s", str(node), e)
        return
    logger.debug("==========%s==========", node_fs_path)
    for (precision, retention) in metadata['retentions']:
        archive_end = t - (t % precision)
        archive_start = archive_end - (precision * retention)
        t = archive_start
        archives.append({
            'precision': precision,
            'retention': retention,
            'start_time': archive_start,
            'end_time': archive_end,
            'slices': [s for s in node.slices if s.timeStep == precision]
        })

    s = StringIO()
    pprint(archives, s)
    logger.debug("Archives: %s", s.getvalue())

    try:
        do_rollup(logger, node, archives, float(metadata.get('xFilesFactor')),
                  metadata.get('aggregationMethod', 'avg'))
    except Exception as e:
        traceback.print_exc()
        logger.error("Got unhandled exception: %s", e)
        raise
    logger.info("%s rollup time: %.3f seconds",
                str(node), (time.time() - start_time))


def do_rollup(log, node, archives, xff, method):
    """
    :param log: class that will be used for logging.
    :param node: graphite's url path. Used to determine where we should store rolled up data
    :param archives: contains ceres slices for one precision and begin ts + end ts that should remain
    :param xff: xFilesFactor - ratio of precision archive must contain to be rolled up accurately

    This means, that if xff is 0.5 and you have more than 50% of NaNs, it won't be rolled up (otherwise there
    will be a good chance to have NaN filled archive).
    For more info about xff see http://obfuscurity.com/2012/04/Unhelpful-Graphite-Tip-9
    :param method: aggregation method
    """
    # empty node?
    begin = time.time()
    if not archives:
        return

    # Huge dict, used for statistical purpose
    # aggregate - how many times aggregation was performed
    # write - how many times data were written
    # slice_create - how many slices were create
    # slice_delete - how many slices were deleted
    # slice_delete_points - how many points were dropped
    # slice_read - how many slices were read
    # slice_read_points - how many points were read
    # slice_write - how many slices were written
    # slice_write_points - how many points were written
    # time - how much time processing of slice took (reading, rolling up, writing, deleting)
    rollup_stat = dict()
    for archive in archives:
        rollup_stat[archive['precision']] = {
            'aggregate': 0,
            'write': 0,
            'slice_create': 0,
            'slice_delete': 0,
            'slice_delete_points': 0,
            'slice_read': 0,
            'slice_read_points': 0,
            'slice_write': 0,
            'slice_write_points': 0,
            'time': 0.0,
        }

    archives_len = len(archives)
    # Before that TS no data can exist (it's start time of the oldest archive)
    window_start = archives[-1]['start_time']

    # list of (retention, list(points))
    # Rollup process for each data archive (Archive == slices with one retention):
    #  1) Read all points that shouldn't be here to memory
    #  2) For each founded list of points, find appropriate archive
    #  3) Aggregate them
    #  4) Write to the Archive
    for i in range(archives_len - 1):
        archive = archives[i]
        archive_op_start = time.time()
        archive_stat = rollup_stat[archive['precision']]
        current_archive = archives[i]
        current_start_time = current_archive['start_time']
        retention_points = list()
        log.debug("________Rolling up %i________", archive['precision'])
        # Checking if we need to rollup anything.
        for ceres_slice in current_archive['slices']:
            if (window_start > ceres_slice.endTime) or (ceres_slice.startTime > current_start_time):
                continue
            try:
                log.debug('Reading points from slice="%s", start=%i, end=%i',
                          ceres_slice,
                          max(window_start, ceres_slice.startTime),
                          current_start_time)
                points = ceres_slice.read(max(window_start, ceres_slice.startTime), current_start_time)
                archive_stat['slice_read'] += 1
                archive_stat['slice_read_points'] += len(points)
                # Getting rid of None points, because we aggregation functions relay on that
                new_points = [x for x in points if x[1] is not None]
                if len(new_points) > 0:
                    log.debug("Found %s points that need some retention, start_time=%s, end_time=%s",
                              len(new_points), new_points[-1][0], new_points[0][0])
                    # Ceres reads points sorted asc, and we relay on them to be sorted dsc
                    # TODO: modify code to relay on points to be sorted ASC
                    new_points.reverse()
                    retention_points.append(new_points)
            except ceres.NoData:
                pass

        if not retention_points:
            continue

        # For each points, that need retention, perform it. Detailed description:
        #  1) Find time range and archives which are suitable (time range can be splitted between multiple archives)
        #  2) Find what slice to use (if none - create one)
        #  3) Aggregate points, using aggregation function (with xFilesFactor in mind)
        #  4) Write points to slices.
        #
        #  Code can be suboptimal, and will need some work in future
        archive_range = range(i + 1, archives_len)
        for points in retention_points:
            log.debug("=======NewPoints=======")
            log.debug("New points %i: end_time=%i, start_time=%i", len(points), points[0][0], points[-1][0])
            # Find appropriate archives for points
            for j in archive_range:
                log.debug("------NewArchive------")
                log.debug("archive: %i, archive's start_time: %i, archive's end_time: %i",
                          j, archives[j]['start_time'], archives[j]['end_time'])
                # If point's end time older than archive's start time - continuing.
                if len(points) == 0:
                    break
                if points[0][0] <= archives[j]['start_time']:
                    continue
                # Do nothing if points starts after the end of archive
                if points[-1][0] > archives[j]['end_time']:
                    log.debug("Points start_time=%s > archive's end_time=%s", points[-1][0], archives[j]['end_time'])
                    log.debug("Dropping %s points", len(points))
                    break

                # Trying to find correct start time. Going backward through list
                k = -1
                while points[k][0] < archives[j]['start_time']:
                    k -= 1
                log.debug("points len: %i, k: %i, time[k]: %i", len(points), k, points[k][0])
                points_to_write = points[0:k]

                # Trying to find appropriate slices for points we've got.
                for ceres_slice in archives[j]['slices']:
                    if len(points_to_write) == 0:
                        break
                    log.debug("slice_endTime: %i, point's endTime: %i", ceres_slice.startTime, points_to_write[0][0])
                    if ceres_slice.endTime >= points[0][0]:
                        log.debug("Trying to %s fit points into slice %s, slice's startTime: %i, point's startTime: %i",
                                  len(points_to_write), ceres_slice, ceres_slice.startTime, points[-1][0])
                        q = fit_points(log, node, method, xff, archives, i, j, rollup_stat, ceres_slice,
                                       points_to_write)
                        points_to_write = points_to_write[0:q]

                if len(points_to_write) > 1:
                    log.debug("%i points left, writing them...", len(points))
                    fit_points(log, node, method, xff, archives, i, j, rollup_stat, None, points_to_write)
                else:
                    break
                points = points[0:k]

                log.debug("New end_time=%i, start_time=%i", points[0][0], points[-1][0])
        archive_stat['time'] += time.time() - archive_op_start

    # Delete all obsolete points from all archives.
    for archive in archives:
        archive_stat = rollup_stat[archive['precision']]
        archive_op_start = time.time()
        for ceres_slice in archive['slices']:
            if (window_start > ceres_slice.endTime) or (ceres_slice.startTime < archive['start_time']):
                archive_stat['slice_delete'] += 1
                archive_stat['slice_delete_points'] += (min(ceres_slice.endTime, archive['start_time']) -
                                                        ceres_slice.startTime) / ceres_slice.timeStep
                try:
                    ceres_slice.deleteBefore(archive['start_time'])
                except ceres.SliceDeleted:
                    pass
        archive_stat['time'] += time.time() - archive_op_start
    log.info("%s rollup stat: %s", str(node), json.dumps(rollup_stat))
    s = StringIO()
    pprint(rollup_stat, s)
    log.debug("%s rollup stat: %s", node, s.getvalue())
    log.info("%s rollup took: %f", str(node), time.time() - begin)


def fit_points(log, node, method, xff, archives, i, j, rollup_stat, ceres_slice, points):
    """
    Fit points to slice or archive (if slice is None).

    All params are same as in main body.
    """
    step = archives[j]['precision']
    archive_stat = rollup_stat[archives[i]['precision']]
    stat = rollup_stat[step]

    start_time = archives[j]['start_time']
    q = -1
    if ceres_slice is not None:
        start_time = ceres_slice.startTime
    log.debug("Trying to fit %s points to start_time=%s, points_start_time=%s, points_end_time=%s",
              len(points),
              start_time,
              points[q][0],
              points[0][0])
    while points[q][0] < start_time and points[q][0] != points[0][0]:
        q -= 1

    log.debug("Fitted with point's startTime: %i", points[q][0])
    if points[q][0] != points[0][0]:
        if len(points[0:q])*archives[i]['precision']/archives[j]['precision'] < xff:
            points_to_write = points[0:q]
        else:
            archive_stat['aggregate'] += 1
            points_to_write = aggregate(method, points[0:q], step)

        log.debug("Writing %i points to slice=%s", len(points_to_write), ceres_slice)
        write_points(log, node, archives[j], points_to_write, ceres_slice, stat)
        return q
    return -1


def write_points(log, node, archive, points, slice_to_write, stat):
    """
    :param log: class that will be used for logging.
    :param node: CeresNode
    :param archive: Ceres archive
    :param points: List of points
    :param slice_to_write: Last slice available
    :param stat: statistics
    """
    if not points:
        return

    log.debug("Writing points to slice %s...", slice_to_write)
    written = False
    # Writing to last_seen_slice with gap
    if slice_to_write:
        try:
            slice_to_write.write(points)
            written = True

            stat['slice_write'] += 1
            stat['slice_write_points'] += len(points)
        except (ceres.SliceDeleted, ceres.SliceGapTooLarge):
            pass

    # gap in last slice too large or no suitable slice found -- creating new slice
    if not written:
        log.debug("No slice found, creating new one: start_time=%i, precision=%i", points[0][0], archive['precision'])
        new_slice = ceres.CeresSlice.create(node, points[0][0], archive['precision'])
        new_slice.write(points)
        log.debug("New slice created: %s", new_slice)
        archive['slices'].append(new_slice)
        # Keep slices sorted (dsc) by startTime, cause we rely on that in 'fit_points'
        archive['slices'] = sorted(archive['slices'], key=lambda x: x.startTime, reverse=True)

        stat['slice_create'] += 1
        stat['slice_write'] += 1
        stat['slice_write_points'] += len(points)
    return
