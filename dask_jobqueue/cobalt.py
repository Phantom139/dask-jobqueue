from __future__ import absolute_import, division, print_function

import logging
import math
import shlex
import subprocess
import sys
import re

import six

import dask

from .core import JobQueueCluster, docstrings

#RF Debug
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class CobaltCluster(JobQueueCluster):
	submit_command = "qsub --mode script"
	cancel_command = "qdel"
	
	_script_template = """
%(shebang)s
%(job_header)s

%(env_header)s

%(worker_command)s
""".lstrip()	

	def __init__(self, 
				 name=None, 
				 queue=None, 
				 project=None, 
				 walltime=None, 
				 ncpus=None, 
				 processes=None, 
				 job_extra=None, 
				 host=None, 
				 log_directory=None, 
				 config_name="cobalt", 
				 death_timeout=None,
				 local_directory=None,				 
				 python=sys.executable,
				 extra=None,
				 **kwargs
	):
		if name is None:
			name = dask.config.get("jobqueue.%s.name" % config_name)
		if queue is None:
			queue = dask.config.get("jobqueue.%s.queue" % config_name)
		if project is None:
			project = dask.config.get("jobqueue.%s.project" % config_name)
		if processes is None:
			processes = dask.config.get("jobqueue.%s.processes" % config_name)			
		if ncpus is None:
			ncpus = dask.config.get("jobqueue.%s.ncpus" % config_name)
		if walltime is None:
			walltime = dask.config.get("jobqueue.%s.walltime" % config_name)
		if job_extra is None:
			job_extra = dask.config.get("jobqueue.%s.job-extra" % config_name)
		if death_timeout is None:
			death_timeout = dask.config.get("jobqueue.%s.death-timeout" % config_name)
		if local_directory is None:
			local_directory = dask.config.get(
				"jobqueue.%s.local-directory" % config_name
			)			

		# Instantiate args and parameters from parent abstract class
		super(CobaltCluster, self).__init__(config_name=config_name, **kwargs)

		header_lines = []
		# Cobalt header build
		if queue is not None:
			header_lines.append("#COBALT -q %s" % queue)
			#self.submit_command += " -q %s" % queue
		if project is not None:
			header_lines.append("#COBALT -A %s" % project)
			#self.submit_command += " -A %s" % project
		if walltime is not None:
			header_lines.append("#COBALT -t %s" % walltime)
			#self.submit_command += " -t %s" % walltime
		if ncpus is None:
			# Compute default cores specifications
			ncpus = self.worker_cores
			logger.info("ncpus specification for COBALT not set, initializing it to %s" % ncpus)
		if ncpus is not None:
			header_lines.append("#COBALT -n %s" % ncpus)
			#self.submit_command += " -n %s" % ncpus
		if log_directory is not None:
			header_lines.append("#COBALT -o %s/" % log_directory)
			#self.submit_command += " -o %s/" % log_directory
			header_lines.append("#COBALT -e %s/" % log_directory)
			#self.submit_command += " -e %s/" % log_directory
		header_lines.extend(["#COBALT %s" % arg for arg in job_extra])

		header_lines.append("JOB_ID=${COBALT_JOBID}")
		
		# Declare class attribute that shall be overridden
		self.job_header = "\n".join(header_lines)
		
		#
		# dask-worker command line build
		dask_worker_command = "%(python)s -m distributed.cli.dask_worker" % dict(python=python)
		if host is not None:
			command_args = [dask_worker_command, host]
		else:
			command_args = [dask_worker_command, self.scheduler.address]
		command_args += ["--nthreads", self.worker_process_threads]
		if processes is not None and processes > 1:
			command_args += ["--nprocs", processes]

		command_args += ["--memory-limit", self.worker_process_memory]
		command_args += ["--name", "%s--${JOB_ID}--" % name]

		if death_timeout is not None:
			command_args += ["--death-timeout", death_timeout]
		if local_directory is not None:
			command_args += ["--local-directory", local_directory]
		if extra is not None:
			command_args += extra

		self._command_template = " ".join(map(str, command_args))		

		logger.debug("Job script: \n %s" % self.job_script())
		
	# RF: For some silly reason, Argonne's COBALT system passes job submission through stderr which causes a return code of 1 on job submit success.
	#  This override should allow for that to proceed and only raise RuntimeError if "error" is found in stderr
	def _call(self, cmd, **kwargs):
		cmd_str = " ".join(cmd)
		logger.debug(
			"Executing the following command to command line\n{}".format(cmd_str)
		)

		proc = subprocess.Popen(
			cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
		)

		out, err = proc.communicate()
		logger.debug("_call(): Returns: {}\n"
		"stdout:\n{}\n"
		"stderr:\n{}\n".format(proc.returncode, out, err))
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

	def start_workers(self, n=1):
		""" Start workers and point them to our local scheduler """
		logger.debug("starting %s workers", n)
		num_jobs = int(math.ceil(n / self.worker_processes))
		for _ in range(num_jobs):
			with self.job_file() as fn:
				#RF: Testing on Theta shows the script file must be executable.
				self._call(shlex.split("chmod +x") + [fn])
				out = self._submit_job(fn)
				job = self._job_id_from_submit_output(out)
				if not job:
					raise ValueError("Unable to parse jobid from output of %s" % out)
				logger.debug("started job: %s", job)
				self.pending_jobs[job] = {}						