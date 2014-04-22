from collections import Iterable
from inspect import getsource
from itertools import count
from logging import getLogger
from os import remove, chmod, stat, mkdir, getuid, getpid
from os.path import join, exists,  basename
from pickle import load, dump
from stat import S_IEXEC
from subprocess import Popen as Popen_local
from subprocess import PIPE
from time import sleep, time


lg = getLogger('lsf')

# TODO: allow stacking
# TODO: implement keyboard interrupt
# TODO: change lsf_dir

lsf_dir = '/PHShome/gp902/projects/lsf'
lg.info('Using directory ' + lsf_dir)

virtual_env = '/PHShome/gp902/toolbox/python/bin/activate'
batch_generator = count()


def _generate_jobid(funct):
    """Generate a (almost) unique job id.

    Parameters
    ----------
    funct : function
        function to execute (we only need the name)

    Returns
    -------
    generator
        str for each job

    Notes
    -----
    Generators are much nicer than lists, however it can only be run once.

    We need to use global, to keep the index across multiple calls of the same
    function.

    It's important to generate unique IDs, in the long term it might be
    necessary to include the hostname.

    This is an infinite generator, not sure if the syntax is very clear, but it
    works. It'll keep spitting out unique ID.

    """
    global batch_generator
    batch_index = next(batch_generator)

    job_index = count()
    while True:
        yield '{0}_p{1}_b{2:06}_{3}_j{4:06}'.format(getuid(), getpid(),
                                                    batch_index,
                                                    funct.__name__,
                                                    next(job_index))


def _parse_stdout(stdout):
    str_beg = 'The output (if any) follows:\n\n'
    str_end = '\n\nPS:\n'

    return stdout[stdout.find(str_beg) + len(str_beg):stdout.find(str_end)]


def _parse_resource_usage(stdout):
    str_beg = 'Resource usage summary:\n\n'
    str_end = '\nThe output (if any) follows:\n'

    return stdout[stdout.find(str_beg) + len(str_beg):stdout.find(str_end)]


def _parse_seconds(stdout):
    str_beg = 'CPU time   :'
    str_end = 'sec.\n'

    return stdout[stdout.find(str_beg) + len(str_beg):stdout.find(str_end)]


class Popen():
    """Create a subprocess in the LSF cluster.

    Parameters
    ----------
    args : str
        script to run

    stdin=None
    stdout=None
    stderr=None,

    shell=False,
    cwd=None,
    env=None

    start_new_session=False

    # any other parameters will probably mess up Popen
    memlim : memory limit in MB
    queue : vshort

    Methods
    -------
    'args'
    'communicate' : OK
     'kill' : OK
     'pid' : OK
     'poll' : OK (I think)
     'returncode',
     'send_signal',
     'stderr',
     'stdin',
     'stdout',
     'terminate',
     'universal_newlines',
     'wait' : OK

     """
    def __init__(self, args, log, queue='vshort', **kwargs):
        self.pid = None

        # hope that log is unique, we need a better way to define the name
        self.jobname = basename(args)

        self._stdout = log + '.o'
        self._stderr = log + '.e'

        try:
            remove(self._stdout)
        except FileNotFoundError:
            pass
        try:
            remove(self._stderr)
        except FileNotFoundError:
            pass

        cmd = []
        cmd.append('bsub')
        cmd.append('-q ' + queue)
        cmd.append('-J ' + self.jobname)
        cmd.append('-o ' + self._stdout)
        cmd.append('-e ' + self._stderr)
        self._cmd = 'echo \"' + args + '\" | ' + ' '.join(cmd)

        # use standard input
        kwargs.update({'stdout': PIPE, 'stderr': PIPE})
        self._kwargs = kwargs

        self._submit()

    def _submit(self):
        pid_submit = Popen_local(self._cmd, shell=True, **self._kwargs)

        stdout, stderr = pid_submit.communicate()
        # if stderr, raise exception
        stdout = stdout.decode('utf-8')
        self.pid = stdout[stdout.find('<')+1:stdout.find('>')]

    def communicate(self):
        self.wait()

        try:
            with open(self._stdout, 'r') as f:
                stdout = f.read()
            with open(self._stderr, 'r') as f:
                stderr = f.read()

            remove(self._stdout)
            remove(self._stderr)
        except FileNotFoundError:
            stdout = stderr = None

        return stdout, stderr

    def poll(self):
        poll = Popen_local('bjobs ' + self.pid, shell=True, stdout=PIPE)
        status = poll.communicate()[0].decode('utf-8')
        status = status[status.find('\n')+1:]
        return status.split()[2]

    def wait(self):
        while True:
            try:
                sleep(0.1)
                if self.poll() in ('DONE', 'EXIT'):
                    break
            except KeyboardInterrupt:
                self.kill()
                break

    def kill(self):
        Popen_local('bkill ' + self.pid, shell=True)
        return self.communicate()


def _prepare_function(func, preamble, input, output, script_file):
    code = ('#!/usr/bin/env python3\n\n' +
            'from os import remove\n' +
            'from pickle import load, dump\n' +
            preamble + '\n' +
            getsource(func) + '\n' +
            'with open(\'' + input + '\', \'rb\') as f:\n'
            '    values = load(f)\n\n'
            'output = ' + func.__name__ + '(*values)\n\n' +
            'with open(\'' + output + '\', \'wb\') as f:\n'
            '    dump(output, f)\n' +
            'remove(\'' + input + '\')\n' +
            'remove(\'' + script_file + '\')\n'
            )
    return code


def map_lsf(funct, iterable, imports=None, variables=None, queue='vshort'):
    """Run function on iterables, on LSF.

    Parameters
    ----------
    funct : function
        function to execute multiple times
    iterable : iterable
        parameters to pass to the function
    imports : dict
        functions to import in the preamble, where key is the module and values
        are the function(s) to import (as string or tuple), such as:
        {'os': ('remove', 'chmod'), 'os.path': 'join'}

    Returns
    -------
    list
        list with the results for each iteratation


    Notes
    -----
    You always need to pass one iterable at least. It doesn't make sense not to
    use it. The only case I can imagine is when running a randomization, but
    we could use the number as seed anyway.

    """
    t0 = time()

    # create paths
    input_dir = join(lsf_dir, 'input')
    output_dir = join(lsf_dir, 'output')
    funct_dir = join(lsf_dir, 'funct')
    log_dir = join(lsf_dir, 'log')

    if not exists(input_dir):
        mkdir(input_dir)
    if not exists(output_dir):
        mkdir(output_dir)
    if not exists(funct_dir):
        mkdir(funct_dir)
    if not exists(log_dir):
        mkdir(log_dir)

    # generate a unique jobid
    jobid_generator = _generate_jobid(funct)

    # create preamble common to all the functions
    preamble = []
    if imports is not None:
        for module, subfunc in imports.items():
            if not isinstance(subfunc, str):
                subfunc = ', '.join(subfunc)
            preamble.append('from ' + module + ' import ' + subfunc)

    # store the variables that are common to all the functions
    # note that it's a dict, there is no guarantee about the order in which
    # they are stored
    if variables is not None:
        variable_file = join(input_dir,
                             '{0}_p{1}_common_variables.pkl'.format(getuid(),
                                                                    getpid()))
        preamble.append('\nwith open(\'' + variable_file + '\', \'rb\') as f:')

        with open(variable_file, 'wb') as f:
            for var_name, var_value in variables.items():
                dump(var_value, f)
                preamble.append('    ' + var_name + ' = load(f)')

        preamble.append('')  # extra space

    # submit jobs
    all_ps = []  # processes
    out_all = []

    for i, val in enumerate(iterable):
        jobid = next(jobid_generator)
        input_file = join(lsf_dir, 'input', 'input_' + jobid)
        output_file = join(lsf_dir, 'output', 'output_' + jobid)
        script_file = join(lsf_dir, 'funct', 'funct_' + jobid)
        log_file = join(lsf_dir, 'log', 'log_' + jobid)

        if isinstance(val, str) or not isinstance(val, Iterable):
            val = (val, )

        with open(input_file, 'wb') as f:
            dump(val, f)
        out_all.append(output_file)

        with open(script_file, 'w') as f:
            f.write(_prepare_function(funct, '\n'.join(preamble),
                                      input_file, output_file, script_file))

        st = stat(script_file)
        chmod(script_file, st.st_mode | S_IEXEC)

        # source does not seem necessary
        # cmd = 'source ' + virtual_env + '; '  + script_file
        cmd = script_file
        all_ps.append(Popen(cmd, log=log_file, queue=queue))
        lg.debug('Submitting script: ' + script_file)

    # wait for jobs to finish
    cpu_time = 0.
    while all_ps:
        sleep(0.2)
        for ps in all_ps:
            if ps.poll() in ('DONE', 'EXIT'):
                stdout, stderr = ps.communicate()
                if stdout:
                    usage = _parse_resource_usage(stdout)
                    stdout = _parse_stdout(stdout)
                    cpu_time += float(_parse_seconds(usage).strip())
                else:
                    usage = 'could not read usage (CPU time not available)'
                    cpu_time += 0
                    stdout = ''

                if stderr:
                    lg.error(ps.jobname + ' has finished with error:\n' +
                             stderr)
                    lg.debug('Resource usage\n' + usage)
                else:
                    lg.info(ps.jobname + ' has finished')
                    lg.debug('Output\n' + stdout)
                    lg.debug('Resource usage\n' + usage)

                all_ps.remove(ps)

    # collect output
    output = []
    for output_file in out_all:
        try:
            with open(output_file, 'rb') as f:
                output.append(load(f))
        except FileNotFoundError:
            output.append(None)
        else:
            remove(output_file)

    if variables is not None:
        remove(variable_file)

    wall_time = time() - t0
    lg.warning('Wall Time: {0:0.2f}s, CPU time: {1:0.2f}s, speed-up: {2:0.2f}X'
               ''.format(wall_time, cpu_time, cpu_time / wall_time))
    return output
