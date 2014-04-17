from inspect import getsource
from logging import getLogger
from os import remove, chmod, stat, mkdir, getlogin, getpid
from os.path import join, exists,  basename
from pickle import load, dump
from stat import S_IEXEC
from subprocess import Popen as Popen_local
from subprocess import PIPE
from time import sleep

lg = getLogger('lsf')

# TODO: multiple inputs
# TODO: allow stacking
# TODO: implement keyboard interrupt
# TODO: change lsf_dir

lsf_dir = '/PHShome/gp902/projects/lsf/log'
lg.info('Using directory ' + lsf_dir)

virtual_env = '/PHShome/gp902/toolbox/python/bin/activate'
batch_index = -1


def _generate_jobid(funct, n_jobs):
    """Generate a (almost) unique job id.
    
    Parameters
    ----------
    funct : function
        function to execute (we only need the name)
    n_jobs : int
        number of jobs to generate
        
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
    
    """
    global batch_index
    batch_index += 1
    
    for job_index in range(n_jobs):
        yield '{0}_p{1}_b{2:06}_{3}_j{4:06}'.format(getlogin(), getpid(), 
                                                    batch_index, funct.__name__,
                                                    job_index)


def _parse_stdout(stdout):
    str_beg = 'The output (if any) follows:\n\n'
    str_end = '\n\nPS:\n'
    
    return stdout[stdout.find(str_beg) + len(str_beg):stdout.find(str_end)]


def _parse_resource_usage(stdout):
    str_beg = 'Resource usage summary:\n\n'
    str_end = '\nThe output (if any) follows:\n'
    
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
        
        # remove bad characters from jobname
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
        self._cmd = ' '.join(cmd) + ' ' + args
        
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
                stdout = f.read()  # TODO: clean it up
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


def _prepare_function(func, input, output, preamble):
    code = ('#!/usr/bin/env python3\n\n' +
            'from pickle import load, dump\n' + 
            preamble + '\n' +
            getsource(func) + '\n' +
            'with open(\'' + input + '\', \'rb\') as f:\n'
            '    values = load(f)\n\n'
            'output = ' + func.__name__ + '(values)\n\n' +
            'with open(\'' + output + '\', \'wb\') as f:\n'
            '    dump(output, f)'
            )
    return code
    

def map_lsf(funct, iterable, imports=None, variables=None, queue=None):
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
    """
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
    jobid_generator = _generate_jobid(funct, len(iterable))
    
    # create preamble common to all the functions
    preamble = []
    preamble.append('source ' + virtual_env)
    if imports is not None:
        for module, subfunc in imports.items():
            if not isinstance(subfunc, str):
                subfunc = ', '.join(subfunc)
            preamble.append('from ' + module + ' import ' + subfunc)

    # store the variables that are common to all the functions
    # note that it's a dict, there is no guarantee about the order in which they
    # are stored
    if variables is not None:
        variable_file = join(input_dir, 'common_variables.pkl')
        preamble.append('\nwith open(\'' + variable_file + '\', \'rb\') as f:')

        with open(variable_file, 'wb') as f:
            for var_name, var_value in variables.items():
                dump(var_value, f)
                preamble.append('    ' + var_name + ' = load(f)')
                
        preamble.append('') # extra space
    
    # submit jobs    
    all_ps = []  # processes
    out_all = []  
    
    for i, val in enumerate(iterable):
        jobid = next(jobid_generator)
        input_file = join(lsf_dir, 'input', 'input_' + jobid)
        output_file = join(lsf_dir, 'output', 'output_' + jobid)
        script_file = join(lsf_dir, 'funct', 'funct_' + jobid)
        log_file = join(lsf_dir, 'log', 'log_' + jobid)

        with open(input_file, 'wb') as f:
            dump(val, f)
        out_all.append(output_file)
        
        with open(script_file, 'w') as f:
            f.write(_prepare_function(funct, input_file, output_file, 
                                      '\n'.join(preamble)))

        st = stat(script_file)
        chmod(script_file, st.st_mode | S_IEXEC)
        all_ps.append(Popen(script_file, log=log_file, queue=queue))
        lg.debug('Submitting script: ' + script_file)

    # wait for jobs to finish
    while all_ps:
        sleep(0.2)
        for ps in all_ps:
            if ps.poll() in ('DONE', 'EXIT'):
                stdout, stderr = ps.communicate()
                usage = _parse_resource_usage(stdout)
                if stderr:
                    lg.error(ps.jobname + ' has finished with error:\n' + 
                             stderr)
                    lg.debug('Resource usage\n' + usage)
                else:
                    lg.info(ps.jobname + ' has finished')
                    lg.debug('Output\n' + _parse_stdout(stdout))
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

    # clean up
    return output
