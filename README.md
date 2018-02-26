## A worker queue implements with Etcd and Python ##

### Approach ###

 * Client submit job by creating a file in queue folder with append=True
 * On start-up, workers scan for unstarted tasks
 * Workers watch for new files come into the folder
   * Each worker will try to acquire for the job by creating a lock file
     since this is an atomic action, only one will get the job
   * Worker executes the job and update status to success when it is done.
   * During the execution, the worker must periodically refresh the ttl

### Questions ###

 * How to handle failures?
 * How to handle "round-robin" behavior?

### References ###

 * https://github.com/OpsLabJPL/etcdq
 * https://stackoverflow.com/questions/34629860/how-would-you-implement-a-working-queue-in-etcd
