## A worker queue implements with Etcd and Python ##

This is a POC of a distributed task queue framework built on top of etcd. The goal of this is to
provide a reliable and scalalbe framework to build distributed task execution.

### Approach ###

Task submissions:
 * Task submission is done by by creating a folder in `tasks/data`.
 * A file is put into `queues` folder to enqueue the task

Task processing:
 * Worker scans for pending files in `queues` folder
 * If there is no pending task, then it starts to watch for change
   * When there is new task, or there is change in states. Worker will try to acquire for the job by creating a lock filem, since this is an atomic action, only one will get the job
   * Worker will create a `running` file to indicate the task is running.
   * Worker executes the task and update status to success when it is done.
   * During the execution, the worker must periodically refresh the ttl
   * On failure, the worker should remove it locks to the file so other workers can acquire the task.

### Questions ###

 * How to handle failures?
 * How to handle "round-robin" behavior?
 * How to cancel a task?: use process interupt
 * Capacity-based worker
 * Task routing
 * Task dependencies
 * Task priorities
 * Task retries
 * Error handling
 * Task communication


### References ###

 * https://github.com/OpsLabJPL/etcdq
 * https://stackoverflow.com/questions/34629860/how-would-you-implement-a-working-queue-in-etcd
