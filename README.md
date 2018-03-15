# jobChomper

[![Build 
Status](https://travis-ci.org/bhautikj/jobChomper.svg?branch=master)](https://travis-ci.org/bhautikj/jobChomper)

## Preamble

`jobChomper` is a colection of frequently-used routines I use for running a graph of commands in parallel. Typically this is used for, say, processing a large number of images. `jobChomper` is robust, and can pick up and finish executing partially run graphs. It's a bit of a dumping ground for stuff I do all the time, so at the moment I'll suggest that it's still a work-in-progress.

## Example

The `Node` class is the basic object that does work in `jobChomper`. It gets passed a shared dictionary that serves as both a source for data to run on as well as somewhere to put results. For example:

```
	class cA(jobChomper.node.Node):
	  def __init__(self):
	    super().__init__()

	  def work(self, params):
	    time.sleep(0.5)
	    if "bin" not in params.keys():
	      params["bin"] = []
	    params["bin"].append("cA")
	    return True

	class cB(jobChomper.node.Node):
	  def __init__(self):
	    super().__init__()

	  def work(self, params):
	    time.sleep(0.5)
	    if "bin" not in params.keys():
	      params["bin"] = []
	    params["bin"].append("cB")
	    return True

	class cC(jobChomper.node.Node):
	  def __init__(self):
	    super().__init__()

	  def work(self, params):
	    time.sleep(0.5)
	    if "bin" not in params.keys():
	      params["bin"] = []
	    params["bin"].append("cC")
	    return True
```

Assuming that we have a directory that we can store intermediate results in - `testBase`:
```
	chomper = jobChomper.chomper.Chomper(testBase, numWorkers=5)

	for i in range(10):
	  jobID = chomper.createJob()
	  graphfile = os.path.join(testdir, 'cGraph.graph')
	  chomper.runGraph(jobID, graphfile)
	  # print("QUEUED JOB:", i, "\n")

	timeStart = time.time()
	results = chomper.waitForComplete()
	interval = time.time()-timeStart
```

The results get stored in the dict `results`; in this case:
```
results[0]['bin']='cA'
results[1]['bin']='cA'
results[2]['bin']='cA'
```

## Running tests

`python -m unittest`

Travis CI hookup: https://www.smartfile.com/blog/testing-python-with-travis-ci/
