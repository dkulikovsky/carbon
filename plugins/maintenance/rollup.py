import logging
import glob
import logging.handlers
from os import path
import time
import json
from carbon.conf import settings, read_writer_configs
import ceres
#######################################################
# Put your custom aggregation logic in this function! #
#######################################################

read_writer_configs()
try:
  ceres.MAX_SLICE_GAP = int(settings['ceres']['MAX_SLICE_GAP'])
except KeyError:
  pass

date = time.strftime("%Y-%m-%d %H:%M:%S")
LOG = "/var/log/ceres_rollup/ceres_rollup.err"
logger = logging.getLogger("rollup")
logger.setLevel(logging.INFO)

needRoll = False
if path.isfile(LOG):
  needRoll = True

fh = logging.FileHandler(LOG)
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s  %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

if needRoll:
  rotate = logging.handlers.RotatingFileHandler(LOG, backupCount=2)
  logger.addHandler(rotate)
  logger.handlers[1].doRollover()


def aggregate(method, values):
  if method in ('avg', 'average'):
    return float(sum(values)) / len(values) # values is guaranteed to be nonempty

  elif method == 'sum':
    return sum(values)

  elif method == 'min':
    return min(values)

  elif method == 'max':
    return max(values)

  elif method == 'median':
    values.sort()
    return values[ len(values) / 2 ]

def node_found(node):
  startTime = time.time()
  archives = []
  t = int( time.time() )
  try:
    metadata = node.readMetadata()
  except:
    logger.error("%s failed to read metadata" % str(node))
    return
  for (precision, retention) in metadata['retentions']:
    archiveEnd =  t - (t % precision)
    archiveStart = archiveEnd - (precision * retention)
    t = archiveStart
    archives.append({
      'precision' : precision,
      'retention' : retention,
      'startTime' : archiveStart,
      'endTime'   : archiveEnd,
      'slices'    : [s for s in node.slices if s.timeStep == precision]
    })

  do_rollup(node, archives, float(metadata.get('xFilesFactor')), metadata.get('aggregationMethod', 'avg'))
  logger.info("%s rollup time: %.3f seconds" % (str(node), (time.time() - startTime)))

def do_rollup(node, archives, xff, method):
  # empty node?
  if not archives:
    return

  rollupStat = {}
  for archive in archives:
    rollupStat[archive['precision']] = {
      'aggregate': 0,
      'drop': 0,
      'memory': 0,
      'write': 0,
      'slice_create': 0,
      'slice_delete': 0,
      'slice_delete_points': 0,
      'slice_read': 0,
      'slice_read_points': 0,
      'slice_write': 0,
      'slice_write_points': 0,
      'slice_update': 0,
      'slice_update_points': 0,
    }

  # list of (slice,deletePrioTo) -- will be dropped after aggregation
  overflowSlices = []

  # dict of in-memory aggregated points (one or more retentions skipped)
  coarsePoints = {}

  # start time of node ( = start time of lowest precision archive)
  windowStart = archives[-1]['startTime']

  # dropping data from lowest precision archive
  fineStep = archives[-1]['precision']
  for slice in archives[-1]['slices']:
    if slice.startTime < windowStart:
      overflowSlices.append((slice, windowStart))

  for i in xrange(len(archives) - 1):
    statTime = time.time()

    # source archive for aggregation
    fineArchive = archives[i]
    fineStep    = fineArchive['precision']
    fineStat    = rollupStat[fineStep]

    # lower precision archive
    coarseArchive = archives[i + 1]
    coarseStep    = coarseArchive['precision']
    coarseStart   = coarseArchive['startTime']
    coarseStat    = rollupStat[coarseStep]

    # end time for lower presicion archive ( = start time of source archive)
    windowEnd = coarseArchive['endTime']

    # reading points from source archive
    finePoints = []
    for slice in fineArchive['slices']:
      # dropping data prior to start time of this archive
      if windowStart > slice.endTime:
        overflowSlices.append((slice, slice.endTime))
        continue
      # slice starts after lower precision archive ends -- no aggregation needed
      if windowEnd <= slice.startTime:
        continue
      try:
        slicePoints = slice.read(max(windowStart, slice.startTime), windowEnd)
        finePoints += [p for p in slicePoints if p[1] is not None]

        fineStat['slice_read']        += 1
        fineStat['slice_read_points'] += len(slicePoints)
      # no data in slice, just removing slice
      except ceres.NoData:
        pass

      # dropping data, which aggregating right now
      overflowSlices.append((slice, windowEnd))

    finePoints = dict(finePoints)
    # adding in-memory aggregated data
    finePoints.update(coarsePoints)
    # sort by timestamp in ascending order
    finePoints = sorted(finePoints.items())

    coarsePoints = {}
    # no points to aggregate :(
    if not finePoints:
      continue

    # start time of aggregation (skipping already aggregated points)
    startTime  = finePoints[0][0]
    startTime -= startTime % coarseStep

    # ... and last
    endTime  = finePoints[-1][0]
    endTime -= endTime % coarseStep
    endTime += coarseStep

    # since we are trying to write points in bulk and already existing slices
    # we need a list of slice start/end times
    # sliceEvents: list of (time, isEnd, slice-number)
    sliceEvents   = []
    
    # writeSlices: list of slices, where writePoints already exists
    writeSlices   = []
    # lastSeenSlice: slice with maximum endTime
    # data will be written there with gap if no writeSlices found 
    lastSeenSlice = None
    for j in xrange(len(coarseArchive['slices'])):
      slice = coarseArchive['slices'][j]
      # slice starts after end of aggregation
      if slice.startTime >= endTime:
        continue

      # slice ended before start of aggregation -- it can be lastSeenSlice
      if slice.endTime <= startTime:
        if lastSeenSlice is None or lastSeenSlice.endTime < slice.endTime:
          lastSeenSlice = slice
        continue

      # starting point is not covered by slice -- adding start slice event
      if slice.startTime > startTime:
        sliceEvents.append((slice.startTime, False, j))
      # starting point covered by slice
      else:
        writeSlices.append(j)
      # adding end slice event
      sliceEvents.append((slice.endTime, True, j))
    # sort slice events by time
    sliceEvents.sort()

    sliceEventsIterator = iter(sliceEvents)
    finePointsIterator = iter(finePoints)

    # list of points with no gap between and no slice start/end events
    # all these points will be written to one list of slices
    writePoints = []
    try:
      sliceEvent = next(sliceEventsIterator)
    except StopIteration:
      sliceEvent = None

    finePoint = next(finePointsIterator)
    for ts in xrange(startTime, endTime, coarseStep):
      tsEndTime = ts + coarseStep

      # no data for current timestamp -- next existing point is newer
      if tsEndTime <= finePoint[0]:
        # writing previously found points if needed
        lastSeenSlice = write_points(node, coarseArchive, writePoints, writeSlices, lastSeenSlice, coarseStat)
        writePoints = []
        continue

      values = []
      try:
        # finding all values for current coarse point
        while finePoint[0] < tsEndTime:
          values.append(finePoint[1])
          finePoint = next(finePointsIterator)
      except StopIteration:
        pass

      fineStat['aggregate'] += 1

      # checking xff
      if float(len(values)) * fineStep / coarseStep < xff:
        # writing previously found points if needed
        lastSeenSlice = write_points(node, coarseArchive, writePoints, writeSlices, lastSeenSlice, coarseStat)
        writePoints = []

        fineStat['drop'] += 1
        continue

      newValue = aggregate(method, values)
      # in-memory aggregated point (writePoints is empty since timestamps processed in ascending order)
      if ts < coarseStart:
        coarsePoints[ts] = newValue

        fineStat['memory'] += 1
        continue

      # slice event found before current timestamp
      if sliceEvent and sliceEvent[0] <= ts:
        # since writeSlices changed -- writting all points
        lastSeenSlice = write_points(node, coarseArchive, writePoints, writeSlices, lastSeenSlice, coarseStat)
        writePoints = [(ts, newValue)]
        # updating writeSlices add lastSeenSlice
        try:
          while sliceEvent[0] <= ts:
            if sliceEvent[1]:
              writeSlices.remove(sliceEvent[2])
              lastSeenSlice = coarseArchive['slices'][sliceEvent[2]]
            else:
              writeSlices.append(sliceEvent[2])
            sliceEvent = next(sliceEventsIterator)
        except StopIteration:
          sliceEvent = None
      # no gaps, no events, just adding to list
      else:
        writePoints.append((ts, newValue))

      fineStat['write'] += 1

    # writing last portion of points
    write_points(node, coarseArchive, writePoints, writeSlices, lastSeenSlice, coarseStat)

    fineStat['time'] = time.time() - statTime

  # after all -- drop aggregated data from source archives
  for slice, deletePriorTo in overflowSlices:
    try:
      rollupStat[slice.timeStep]['slice_delete']        += 1
      rollupStat[slice.timeStep]['slice_delete_points'] += (min(slice.endTime, deletePriorTo) - slice.startTime) / slice.timeStep
      slice.deleteBefore(deletePriorTo)
    except ceres.SliceDeleted:
      pass

  logger.info("%s rollup stat: %s" % (str(node), json.dumps(rollupStat)))

def write_points(node, archive, points, slices, lastSlice, stat):
  if not points:
    return lastSlice

  written = False
  # trying to update all existing slices
  for i in slices:
    try:
      archive['slices'][i].write(points)
      written = True

      stat['slice_update']        += 1
      stat['slice_update_points'] += len(points)
    except ceres.SliceDeleted:
      pass
  # if not -- writing to lastSeenSlice with gap
  if not written and lastSlice:
    try:
      lastSlice.write(points)
      written = True

      stat['slice_write']        += 1
      stat['slice_write_points'] += len(points)
    except (ceres.SliceDeleted,ceres.SliceGapTooLarge):
      pass
  # gap in last slice too large -- creating new slice
  if not written:
    newSlice = ceres.CeresSlice.create(node, points[0][0], archive['precision'])
    newSlice.write(points)
    archive['slices'].append(newSlice)
    lastSlice = newSlice

    stat['slice_create']       += 1
    stat['slice_write']        += 1
    stat['slice_write_points'] += len(points)
  return lastSlice
