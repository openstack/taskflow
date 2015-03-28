# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import logging
import math
import os
import sys

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from six.moves import range as compat_range

from taskflow import engines
from taskflow.engines.worker_based import worker
from taskflow.patterns import unordered_flow as uf
from taskflow import task
from taskflow.utils import threading_utils

# INTRO: This example walks through a workflow that will in parallel compute
# a mandelbrot result set (using X 'remote' workers) and then combine their
# results together to form a final mandelbrot fractal image. It shows a usage
# of taskflow to perform a well-known embarrassingly parallel problem that has
# the added benefit of also being an elegant visualization.
#
# NOTE(harlowja): this example simulates the expected larger number of workers
# by using a set of threads (which in this example simulate the remote workers
# that would typically be running on other external machines).
#
# NOTE(harlowja): to have it produce an image run (after installing pillow):
#
# $ python taskflow/examples/wbe_mandelbrot.py output.png

BASE_SHARED_CONF = {
    'exchange': 'taskflow',
}
WORKERS = 2
WORKER_CONF = {
    # These are the tasks the worker can execute, they *must* be importable,
    # typically this list is used to restrict what workers may execute to
    # a smaller set of *allowed* tasks that are known to be safe (one would
    # not want to allow all python code to be executed).
    'tasks': [
        '%s:MandelCalculator' % (__name__),
    ],
}
ENGINE_CONF = {
    'engine': 'worker-based',
}

# Mandelbrot & image settings...
IMAGE_SIZE = (512, 512)
CHUNK_COUNT = 8
MAX_ITERATIONS = 25


class MandelCalculator(task.Task):
    def execute(self, image_config, mandelbrot_config, chunk):
        """Returns the number of iterations before the computation "escapes".

        Given the real and imaginary parts of a complex number, determine if it
        is a candidate for membership in the mandelbrot set given a fixed
        number of iterations.
        """

        # Parts borrowed from (credit to mark harris and benoît mandelbrot).
        #
        # http://nbviewer.ipython.org/gist/harrism/f5707335f40af9463c43
        def mandelbrot(x, y, max_iters):
            c = complex(x, y)
            z = 0.0j
            for i in compat_range(max_iters):
                z = z * z + c
                if (z.real * z.real + z.imag * z.imag) >= 4:
                    return i
            return max_iters

        min_x, max_x, min_y, max_y, max_iters = mandelbrot_config
        height, width = image_config['size']
        pixel_size_x = (max_x - min_x) / width
        pixel_size_y = (max_y - min_y) / height
        block = []
        for y in compat_range(chunk[0], chunk[1]):
            row = []
            imag = min_y + y * pixel_size_y
            for x in compat_range(0, width):
                real = min_x + x * pixel_size_x
                row.append(mandelbrot(real, imag, max_iters))
            block.append(row)
        return block


def calculate(engine_conf):
    # Subdivide the work into X pieces, then request each worker to calculate
    # one of those chunks and then later we will write these chunks out to
    # an image bitmap file.

    # And unordered flow is used here since the mandelbrot calculation is an
    # example of an embarrassingly parallel computation that we can scatter
    # across as many workers as possible.
    flow = uf.Flow("mandelbrot")

    # These symbols will be automatically given to tasks as input to their
    # execute method, in this case these are constants used in the mandelbrot
    # calculation.
    store = {
        'mandelbrot_config': [-2.0, 1.0, -1.0, 1.0, MAX_ITERATIONS],
        'image_config': {
            'size': IMAGE_SIZE,
        }
    }

    # We need the task names to be in the right order so that we can extract
    # the final results in the right order (we don't care about the order when
    # executing).
    task_names = []

    # Compose our workflow.
    height, _width = IMAGE_SIZE
    chunk_size = int(math.ceil(height / float(CHUNK_COUNT)))
    for i in compat_range(0, CHUNK_COUNT):
        chunk_name = 'chunk_%s' % i
        task_name = "calculation_%s" % i
        # Break the calculation up into chunk size pieces.
        rows = [i * chunk_size, i * chunk_size + chunk_size]
        flow.add(
            MandelCalculator(task_name,
                             # This ensures the storage symbol with name
                             # 'chunk_name' is sent into the tasks local
                             # symbol 'chunk'. This is how we give each
                             # calculator its own correct sequence of rows
                             # to work on.
                             rebind={'chunk': chunk_name}))
        store[chunk_name] = rows
        task_names.append(task_name)

    # Now execute it.
    eng = engines.load(flow, store=store, engine_conf=engine_conf)
    eng.run()

    # Gather all the results and order them for further processing.
    gather = []
    for name in task_names:
        gather.extend(eng.storage.get(name))
    points = []
    for y, row in enumerate(gather):
        for x, color in enumerate(row):
            points.append(((x, y), color))
    return points


def write_image(results, output_filename=None):
    print("Gathered %s results that represents a mandelbrot"
          " image (using %s chunks that are computed jointly"
          " by %s workers)." % (len(results), CHUNK_COUNT, WORKERS))
    if not output_filename:
        return

    # Pillow (the PIL fork) saves us from writing our own image writer...
    try:
        from PIL import Image
    except ImportError as e:
        # To currently get this (may change in the future),
        # $ pip install Pillow
        raise RuntimeError("Pillow is required to write image files: %s" % e)

    # Limit to 255, find the max and normalize to that...
    color_max = 0
    for _point, color in results:
        color_max = max(color, color_max)

    # Use gray scale since we don't really have other colors.
    img = Image.new('L', IMAGE_SIZE, "black")
    pixels = img.load()
    for (x, y), color in results:
        if color_max == 0:
            color = 0
        else:
            color = int((float(color) / color_max) * 255.0)
        pixels[x, y] = color
    img.save(output_filename)


def create_fractal():
    logging.basicConfig(level=logging.ERROR)

    # Setup our transport configuration and merge it into the worker and
    # engine configuration so that both of those use it correctly.
    shared_conf = dict(BASE_SHARED_CONF)
    shared_conf.update({
        'transport': 'memory',
        'transport_options': {
            'polling_interval': 0.1,
        },
    })

    if len(sys.argv) >= 2:
        output_filename = sys.argv[1]
    else:
        output_filename = None

    worker_conf = dict(WORKER_CONF)
    worker_conf.update(shared_conf)
    engine_conf = dict(ENGINE_CONF)
    engine_conf.update(shared_conf)
    workers = []
    worker_topics = []

    print('Calculating your mandelbrot fractal of size %sx%s.' % IMAGE_SIZE)
    try:
        # Create a set of workers to simulate actual remote workers.
        print('Running %s workers.' % (WORKERS))
        for i in compat_range(0, WORKERS):
            worker_conf['topic'] = 'calculator_%s' % (i + 1)
            worker_topics.append(worker_conf['topic'])
            w = worker.Worker(**worker_conf)
            runner = threading_utils.daemon_thread(w.run)
            runner.start()
            w.wait()
            workers.append((runner, w.stop))

        # Now use those workers to do something.
        engine_conf['topics'] = worker_topics
        results = calculate(engine_conf)
        print('Execution finished.')
    finally:
        # And cleanup.
        print('Stopping workers.')
        while workers:
            r, stopper = workers.pop()
            stopper()
            r.join()
    print("Writing image...")
    write_image(results, output_filename=output_filename)


if __name__ == "__main__":
    create_fractal()
