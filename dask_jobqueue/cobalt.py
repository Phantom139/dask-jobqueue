from __future__ import absolute_import, division, print_function

import logging
import shlex
import subprocess
import sys

import six

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)

class CobaltCluster(JobQueueCluster):
	submit_command = "qsub --mode script"
	cancel_command = "qdel"

	def __init__(self, queue=None, project=None, walltime=None, ncpus=None, job_extra=None, config_name="cobalt", **kwargs):
		if queue is None:
			queue = dask.config.get("jobqueue.%s.queue" % config_name)
		if project is None:
			project = dask.config.get("jobqueue.%s.project" % config_name)			
		if ncpus is None:
			ncpus = dask.config.get("jobqueue.%s.ncpus" % config_name)
		if walltime is None:
			walltime = dask.config.get("jobqueue.%s.walltime" % config_name)
		if job_extra is None:
			job_extra = dask.config.get("jobqueue.%s.job-extra" % config_name)

		# Instantiate args and parameters from parent abstract class
		super(CobaltCluster, self).__init__(config_name=config_name, **kwargs)

		header_lines = []
		# Cobalt header build
		if queue is not None:
			header_lines.append("#COBALT -q %s" % queue)
			self.submit_command += " -q %s" % queue
		if project is not None:
			header_lines.append("#COBALT -A %s" % project)
			self.submit_command += " -A %s" % project
		if walltime is not None:
			header_lines.append("#COBALT -t %s" % walltime)
			self.submit_command += " -t %s" % walltime
		if ncpus is None:
			# Compute default cores specifications
			ncpus = self.worker_cores
			logger.info("ncpus specification for COBALT not set, initializing it to %s" % ncpus)
		if ncpus is not None:
			header_lines.append("#COBALT -n %s" % ncpus)
			self.submit_command += " -n %s" % ncpus
		if self.log_directory is not None:
			header_lines.append("#COBALT -o %s/" % self.log_directory)
		header_lines.extend(["#COBALT %s" % arg for arg in job_extra])

		# Declare class attribute that shall be overridden
		self.job_header = "\n".join(header_lines)

		logger.debug("Job script: \n %s" % self.job_script())
		
    def _call(self, cmd, **kwargs):
        cmd_str = " ".join(cmd)
        logger.debug(
            "Executing the following command to command line\n{}".format(cmd_str)
        )

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
        )

        out, err = proc.communicate()
        if six.PY3:
            out, err = out.decode(), err.decode()
        if proc.returncode != 0:
			if proc.returncode != 1:
				raise RuntimeError(
					"Command exited with non-zero exit code.\n"
					"Exit code: {}\n"
					"Command:\n{}\n"
					"stdout:\n{}\n"
					"stderr:\n{}\n".format(proc.returncode, cmd_str, out, err)
				)
			else:
				if(err.find("error") != -1):
					raise RuntimeError(
						"Command exited with non-zero exit code (error).\n"
						"Exit code: {}\n"
						"Command:\n{}\n"
						"stdout:\n{}\n"
						"stderr:\n{}\n".format(proc.returncode, cmd_str, out, err)
					)					
        return out		
